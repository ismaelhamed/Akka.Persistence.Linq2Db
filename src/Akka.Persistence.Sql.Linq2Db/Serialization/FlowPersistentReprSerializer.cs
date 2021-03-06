﻿using System.Collections.Immutable;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Persistence.Sql.Linq2Db
{
    public abstract class
        FlowPersistentReprSerializer<T> : PersistentReprSerializer<T>
    {
        public
            Flow<T, Try<(IPersistentRepresentation, IImmutableSet<string>, long)>
                , NotUsed> deserializeFlow()
        {
            return Flow.Create<T, NotUsed>().Select(t => deserialize(t));
            //.Select(t => );

        }
    }
}