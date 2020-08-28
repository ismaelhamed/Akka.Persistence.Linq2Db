using System;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Persistence.Sql.Linq2Db
{
    public interface IJournalDaoWithReadMessages
    {
        /// <summary>
        /// Returns a Source of <see cref="IPersistentRepresentation"/> and ordering number for a certain persistenceId.
        /// It includes the events with sequenceNr between `fromSequenceNr` (inclusive) and `toSequenceNr` (inclusive).
        /// </summary>
        Source<Try<(IPersistentRepresentation, long)>, NotUsed> Messages(string persistenceId, long fromSequenceNr, long toSequenceNr, long max);

        /// <summary>
        /// Returns a Source of <see cref="IPersistentRepresentation"/> and ordering number for a certain persistenceId.
        /// It includes the events with sequenceNr between `fromSequenceNr` (inclusive) and `toSequenceNr` (inclusive).
        /// </summary>
        Source<Try<(IPersistentRepresentation, long)>, NotUsed> MessagesWithBatch(string persistenceId, long fromSequenceNr, long toSequenceNr, int batchSize, Option<(TimeSpan, SchedulerBase)> refreshInterval);
    }
}