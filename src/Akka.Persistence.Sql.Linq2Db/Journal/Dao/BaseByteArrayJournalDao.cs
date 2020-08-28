using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using LinqToDB;
using LinqToDB.Data;

namespace Akka.Persistence.Sql.Linq2Db
{
    public abstract class BaseByteArrayJournalDao : BaseJournalDaoWithReadMessages, IJournalDaoWithUpdates
    {
        private readonly ISourceQueueWithComplete<(TaskCompletionSource<NotUsed>, List<JournalRow>)> writeQueue;
        private readonly JournalConfig _journalConfig;
        private readonly FlowPersistentReprSerializer<JournalRow> _serializer;

        protected readonly AkkaPersistenceDataConnectionFactory ConnectionFactory;

        protected BaseByteArrayJournalDao(IAdvancedScheduler sched, IMaterializer materializer, AkkaPersistenceDataConnectionFactory connectionFactory, JournalConfig config, ByteArrayJournalSerializer serializer)
            : base(sched, materializer)
        {
            ConnectionFactory = connectionFactory;
            _journalConfig = config;
            _serializer = serializer;

            writeQueue = Source
                .Queue<(TaskCompletionSource<NotUsed>, List<JournalRow>)>(_journalConfig.DaoConfig.BufferSize, OverflowStrategy.DropNew)
                .BatchWeighted(
                    _journalConfig.DaoConfig.BatchSize,
                    cf => cf.Item2.Count,
                    tup => (new[] {tup.Item1}, tup.Item2),
                    (oldRows, newRows) => (oldRows.Item1.Append(newRows.Item1).ToArray(), oldRows.Item2.Concat(newRows.Item2).ToList()))
                .SelectAsync(_journalConfig.DaoConfig.Parallelism, async promisesAndRows =>
                {
                    try
                    {
                        await WriteJournalRows(promisesAndRows.Item2);
                        foreach (var promise in promisesAndRows.Item1)
                        {
                            promise.TrySetResult(NotUsed.Instance);
                        }
                    }
                    catch (Exception e)
                    {
                        foreach (var promise in promisesAndRows.Item1)
                        {
                            promise.TrySetException(e);
                        }
                    }

                    return promisesAndRows;
                })
                .ToMaterialized(Sink.Ignore<(TaskCompletionSource<NotUsed>[], List<JournalRow>)>(), Keep.Left)
                .Run(mat);
        }

        /// <summary>
        /// This logging may block since we don't control how the user will configure logback
        /// We can't use a Akka logging neither because we don't have an ActorSystem in scope and
        /// we should not introduce another dependency here.
        /// Therefore, we make sure we only log a warning for logical deletes once
        /// </summary>
        private readonly Lazy<object> logWarnAboutLogicalDeletionDeprecation =
            new Lazy<object>(() => new object(), LazyThreadSafetyMode.None);

        private async Task QueueWriteJournalRows(IEnumerable<JournalRow> xs)
        {
            var promise = new TaskCompletionSource<NotUsed>();
            await writeQueue.OfferAsync((promise, xs.ToList()))
                .ContinueWith(task =>
                {
                    var result = task.Result;
                    switch (result)
                    {
                        case QueueOfferResult.Enqueued _:
                            break;
                        case QueueOfferResult.Failure f:
                            promise.SetException(new Exception("Failed to write journal row batch", f.Cause));
                            break;
                        case QueueOfferResult.Dropped _:
                            promise.SetException(new Exception($"Failed to enqueue journal row batch write, the queue buffer was full ({_journalConfig.DaoConfig.BufferSize} elements) please check the linq2db-journal.bufferSize setting"));
                            break;
                        case QueueOfferResult.QueueClosed _:
                            promise.SetException(new Exception("Failed to enqueue journal row batch write, the queue was closed"));
                            break;
                    }
                });
            await promise.Task;
        }

        private async Task WriteJournalRows(IReadOnlyCollection<JournalRow> xs)
        {
            await using var db = ConnectionFactory.GetConnection();
            if (xs.Count > 1)
            {
                db.GetTable<JournalRow>()
                    .TableName(_journalConfig.TableConfiguration.TableName)
                    .BulkCopy(new BulkCopyOptions {BulkCopyType = BulkCopyType.Default}, xs);
            }
            else if (xs.Count == 1)
            {
                await db.InsertAsync(xs.First());
            }

            // Write atomically without auto-commit
        }

        public Task<IEnumerable<Try<NotUsed>>> AsyncWriteMessages(IEnumerable<AtomicWrite> messages)
        {
            var serializedTries = _serializer.Serialize(messages);

            // If serialization fails for some AtomicWrites, the other AtomicWrites may still be written
            var taskList = serializedTries.Select(async rowsToWrite =>
            {
                if (rowsToWrite.IsSuccess == false)
                {
                    return new Try<NotUsed>(rowsToWrite.Failure.Value);
                }

                try
                {
                    await QueueWriteJournalRows(rowsToWrite.Get());
                    return new Try<NotUsed>(NotUsed.Instance);
                }
                catch (Exception e)
                {
                    return new Try<NotUsed>(e);
                }
            }).ToArray();

            return Task<IEnumerable<Try<NotUsed>>>.Factory.ContinueWhenAll(taskList, task =>
                task.Select(t => t.Result));
        }

        public async Task Delete(string persistenceId, long maxSequenceNr)
        {
            async Task MarkJournalMessagesAsDeleted(DataConnection dataConnection)
            {
                await dataConnection.GetTable<JournalRow>().TableName(_journalConfig.TableConfiguration.TableName)
                    .Where(r => r.PersistenceId == persistenceId && r.SequenceNumber <= maxSequenceNr && r.Deleted == false)
                    .Set(r => r.Deleted, true)
                    .UpdateAsync();
            }

            await using var db = ConnectionFactory.GetConnection();
            if (_journalConfig.DaoConfig.LogicalDelete)
            {
                // We only log a warning when user effectively deletes an event.
                // The rationale here is that this feature is not so broadly used and the default
                // is to have logical delete enabled.
                // We don't want to log warnings for users that are not using this,
                // so we make it happen only when effectively used.
                var obj = logWarnAboutLogicalDeletionDeprecation.Value; // TODO: log warning
                await MarkJournalMessagesAsDeleted(db);
            }
            else
            {
                await db.BeginTransactionAsync();

                await MarkJournalMessagesAsDeleted(db);
                var highestMarkedSequenceNr = await HighestMarkedSequenceNr(persistenceId);

                await db.GetTable<JournalRow>().TableName(_journalConfig.TableConfiguration.TableName)
                    .Where(r => r.PersistenceId == persistenceId && r.SequenceNumber <= highestMarkedSequenceNr - 1)
                    .DeleteAsync();

                await db.CommitTransactionAsync();
            }
        }

        public async Task<Done> Update(string persistenceId, long sequenceNr, object payload)
        {
            var write = new Persistent(payload, sequenceNr, persistenceId);
            var serialize = _serializer.Serialize(write);
            if (serialize.IsSuccess)
            {
                throw new ArgumentException($"Failed to serialize {write.GetType()} for update of {persistenceId}] @ {sequenceNr}", serialize.Failure.Value);
            }

            await using var db = ConnectionFactory.GetConnection();
            await db.GetTable<JournalRow>().TableName(_journalConfig.TableConfiguration.TableName)
                .Where(r => r.PersistenceId == persistenceId && r.SequenceNumber == sequenceNr)
                .Set(r => r.Message, serialize.Get().Message)
                .UpdateAsync();

            return Done.Instance;
        }

        public virtual async Task<long> HighestMarkedSequenceNr(string persistenceId)
        {
            await using var db = ConnectionFactory.GetConnection();
            return await db.GetTable<JournalRow>().TableName(_journalConfig.TableConfiguration.TableName)
                .Where(r => r.PersistenceId == persistenceId && r.Deleted)
                .Select(r => (long?)r.SequenceNumber)
                .MaxAsync() ?? 0L;
        }
        
        public virtual async Task<long> HighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            await using var db = ConnectionFactory.GetConnection();
            return await db.GetTable<JournalRow>().TableName(_journalConfig.TableConfiguration.TableName)
                .Where(r => r.PersistenceId == persistenceId)
                .Select(r => (long?)r.SequenceNumber)
                .MaxAsync() ?? 0L;
        }

        public override Source<Try<(IPersistentRepresentation, long)>, NotUsed> Messages(string persistenceId, long fromSequenceNr, long toSequenceNr, long max)
        {
            using var db = ConnectionFactory.GetConnection();

            IQueryable<JournalRow> messagesQuery = db.GetTable<JournalRow>().TableName(_journalConfig.TableConfiguration.TableName)
                .Where(r => r.PersistenceId == persistenceId && r.SequenceNumber >= fromSequenceNr && r.SequenceNumber <= toSequenceNr && r.Deleted == false)
                .OrderBy(r => r.SequenceNumber);

            if (max <= int.MaxValue)
            {
                messagesQuery = messagesQuery.Take((int) max);
            }

            return Source.From(messagesQuery.ToList())
                .Via(_serializer.DeserializeFlow())
                .Select(sertry => sertry.IsSuccess
                    ? new Try<(IPersistentRepresentation, long)>((sertry.Success.Value.Item1, sertry.Success.Value.Item3))
                    : new Try<(IPersistentRepresentation, long)>(sertry.Failure.Value));
        }
    }
}