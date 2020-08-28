using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Journal;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;
using Akka.Util.Internal;

namespace Akka.Persistence.Sql.Linq2Db
{
    public class Linq2DbAsyncWriteJournal : AsyncWriteJournal
    {
        private readonly ActorMaterializer _mat;
        private readonly JournalConfig _journalConfig;
        private readonly ByteArrayJournalDao _dao;

        /// <summary>
        /// ReadHighestSequence must be performed after pending write for a persistenceId
        /// when the persistent actor is restarted.
        /// </summary>
        private readonly Dictionary<string, Task> writeInProgress = new Dictionary<string, Task>();

        public Linq2DbAsyncWriteJournal(Config config)
        {
            _mat = ActorMaterializer.Create(Context);
            _journalConfig = new JournalConfig(config);
            try
            {
                _dao = new ByteArrayJournalDao(Context.System.Scheduler.Advanced, _mat,
                    new AkkaPersistenceDataConnectionFactory(_journalConfig),
                    _journalConfig, Context.System.Serialization);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            if (_journalConfig.TableConfiguration.AutoInitialize)
            {
                try
                {
                    _dao.InitializeTables();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

        protected override bool ReceivePluginInternal(object message)
        {
            switch (message)
            {
                case WriteFinished ({ } persistenceId, _):
                    writeInProgress.Remove(persistenceId);
                    break;
                case InPlaceUpdateEvent ({ } persistenceId, { } sequenceNr, { } write):
                    _dao.Update(persistenceId, sequenceNr, write).PipeTo(Sender);
                    break;
                default:
                    return false;
            }

            return true;
        }

        public override Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr, long toSequenceNr, long max, Action<IPersistentRepresentation> recoveryCallback)
        {
            return _dao.MessagesWithBatch(persistenceId, fromSequenceNr, toSequenceNr, _journalConfig.DaoConfig.ReplayBatchSize, Option<(TimeSpan, SchedulerBase)>.None)
                .Take(max)
                .SelectAsync(1, t => t.IsSuccess
                    ? Task.FromResult(t.Success.Value)
                    : Task.FromException<(IPersistentRepresentation, long)>(t.Failure.Value))
                .RunForeach(r => recoveryCallback(r.Item1), _mat);
        }

        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            if (!writeInProgress.ContainsKey(persistenceId))
                return await _dao.HighestSequenceNr(persistenceId, fromSequenceNr);

            // TODO: We must fetch the highest sequence number after the previous write has completed
            // TODO: If the previous write failed then we can ignore this
            await writeInProgress[persistenceId];
            return await _dao.HighestSequenceNr(persistenceId, fromSequenceNr);
        }

        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var atomicWrites = messages as AtomicWrite[] ?? messages.ToArray();

            // TODO: Currently AtomicWrite does not support withTimestamp
            //// Add timestamp to all payloads in all AtomicWrite messages
            // var timedMessages = atomicWrites.Select(atomWrt =>
            // {
            //     // since they are all persisted atomically,
            //     // all PersistentRepr on the same atomic batch gets the same timestamp
            //     var now = DateTime.UtcNow.Ticks;
            // });

            var future = _dao.AsyncWriteMessages(atomicWrites);
            var persistenceId = atomicWrites.Head().PersistenceId;
            writeInProgress.AddOrSet(persistenceId, future);
     
            var context = Context;
            return await future.ContinueWith(task =>
            {
                context.Self.Tell(new WriteFinished(persistenceId, future));
                return task.Result
                    .Select(r => r.IsSuccess ? null : TryUnwrapException(r.Failure.Value))
                    .ToImmutableList() as IImmutableList<Exception>;
            });
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr) => 
            _dao.Delete(persistenceId, toSequenceNr);
        
        private sealed class WriteFinished
        {
            public string PersistenceId { get; }
            public Task Future { get; }

            public WriteFinished(string persistenceId, Task future)
            {
                PersistenceId = persistenceId;
                Future = future;
            }

            public void Deconstruct(out string persistenceId, out Task future) => 
                (persistenceId, future) = (PersistenceId, Future);
        }

        /// <summary>
        /// Extra Plugin API: May be used to issue in-place updates for events.
        /// To be used only for data migrations such as "encrypt all events" and similar operations.
        /// 
        /// The write payload may be wrapped in a <see cref="Akka.Persistence.Journal.Tagged"/>,
        /// in which case the new tags will overwrite the existing tags of the event.
        /// </summary>
        public sealed class InPlaceUpdateEvent
        {
            public string PersistenceId { get; }
            public long SequenceNr { get; }
            public object Write { get; }
            
            public InPlaceUpdateEvent(string persistenceId, long sequenceNr, object write)
            {
                PersistenceId = persistenceId;
                SequenceNr = sequenceNr;
                Write = write;
            }
            
            public void Deconstruct(out string persistenceId, out long sequenceNr, out object write) => 
                (persistenceId, sequenceNr, write) = (PersistenceId, SequenceNr, Write);
        }
    }
}