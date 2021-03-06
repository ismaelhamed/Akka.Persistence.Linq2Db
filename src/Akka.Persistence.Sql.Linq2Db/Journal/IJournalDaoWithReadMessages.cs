﻿using System;
using Akka.Actor;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Persistence.Sql.Linq2Db
{
    public interface IJournalDaoWithReadMessages
    {
        Source<Try<(IPersistentRepresentation, long)>,NotUsed> Messages(
            string persistenceId, long fromSequenceNr, long toSequenceNr,
            long max);
        Source<Try<(IPersistentRepresentation, long)>,NotUsed> MessagesWithBatch(
            string persistenceId, long fromSequenceNr, long toSequenceNr,
            int batchSize, Option<(TimeSpan,SchedulerBase)> refreshInterval);
    }
}