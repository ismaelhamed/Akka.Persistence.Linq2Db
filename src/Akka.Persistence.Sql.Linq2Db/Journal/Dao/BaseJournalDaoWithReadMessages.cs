using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Pattern;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Persistence.Sql.Linq2Db
{
    public abstract class BaseJournalDaoWithReadMessages : IJournalDaoWithReadMessages
    {
        protected BaseJournalDaoWithReadMessages(IAdvancedScheduler ec, IMaterializer mat)
        {
            this.ec = ec;
            this.mat = mat;
        }

        protected IAdvancedScheduler ec;
        protected IMaterializer mat;

        /// <summary>
        /// Returns a Source of <see cref="IPersistentRepresentation"/> and ordering number for a certain persistenceId.
        /// It includes the events with sequenceNr between `fromSequenceNr` (inclusive) and `toSequenceNr` (inclusive).
        /// </summary>
        public abstract Source<Try<(IPersistentRepresentation, long)>, NotUsed> Messages(string persistenceId, long fromSequenceNr, long toSequenceNr, long max);

        /// <summary>
        /// Returns a Source of <see cref="IPersistentRepresentation"/> and ordering number for a certain persistenceId.
        /// It includes the events with sequenceNr between `fromSequenceNr` (inclusive) and `toSequenceNr` (inclusive).
        /// </summary>
        public Source<Try<(IPersistentRepresentation, long)>, NotUsed> MessagesWithBatch(string persistenceId, long fromSequenceNr, long toSequenceNr, int batchSize, Option<(TimeSpan, SchedulerBase)> refreshInterval)
        {
            var src = Source
                .UnfoldAsync<(long, FlowControl), IEnumerable<Try<(IPersistentRepresentation, long)>>>(
                    (Math.Max(1, fromSequenceNr), FlowControl.Continue.Instance), async opt =>
                    {
                        var (from, flowControl) = opt;

                        async Task<Option<((long, FlowControl), IEnumerable<Try<(IPersistentRepresentation, long)>>)>> RetrieveNextBatch()
                        {
                            var msg = await Messages(persistenceId, from, toSequenceNr, batchSize)
                                .RunWith(Sink.Seq<Try<(IPersistentRepresentation, long)>>(), mat);

                            var hasMoreEvents = msg.Count == batchSize;
                            // Events are ordered by sequence number, therefore the last one is the largest)
                            var lastOption = msg.LastOrDefault();

                            var lastSeqNrInBatch = Option<long>.None;
                            if (lastOption != null && lastOption.IsSuccess)
                            {
                                lastSeqNrInBatch = lastOption.Success.Select(r => r.Item1.SequenceNr);
                            }
                            else if (lastOption != null && lastOption.Failure.HasValue)
                            {
                                throw lastOption.Failure.Value;
                            }

                            FlowControl nextControl;
                            
                            var hasLastEvent = lastSeqNrInBatch.HasValue && lastSeqNrInBatch.Value >= toSequenceNr;
                            if (hasLastEvent || from > toSequenceNr)
                            {
                                nextControl = FlowControl.Stop.Instance;
                            }
                            else if (hasMoreEvents)
                            {
                                nextControl = FlowControl.Continue.Instance;
                            }
                            else if (refreshInterval.HasValue == false)
                            {
                                nextControl = FlowControl.Stop.Instance;
                            }
                            else
                            {
                                nextControl = FlowControl.ContinueDelayed.Instance;
                            }

                            // Continue querying from the last sequence number (the events are ordered)
                            var nextFrom = lastSeqNrInBatch.HasValue ? lastSeqNrInBatch.Value + 1 : from;
                            return new Option<((long, FlowControl), IEnumerable<Try<(IPersistentRepresentation, long)>>)>(((nextFrom, nextControl), msg));
                        }

                        return flowControl switch
                        {
                            FlowControl.Stop _ => Option<((long, FlowControl), IEnumerable<Try<(IPersistentRepresentation, long)>>)>.None,
                            FlowControl.Continue _ => await RetrieveNextBatch(),
                            FlowControl.ContinueDelayed _ when refreshInterval.HasValue => await FutureTimeoutSupport.After(refreshInterval.Value.Item1, refreshInterval.Value.Item2, RetrieveNextBatch),
                            _ => throw new ArgumentException( "Expected values are: Stop, Continue, and ContinueDelayed.", nameof(flowControl))
                        };
                    });

            return src.SelectMany(r => r);
        }
    }
}