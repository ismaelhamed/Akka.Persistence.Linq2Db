using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Persistence.Journal;
using Akka.Util;

namespace Akka.Persistence.Sql.Linq2Db
{
    public abstract class PersistentReprSerializer<T>
    {
        public IEnumerable<Try<IEnumerable<T>>> Serialize(IEnumerable<AtomicWrite> messages)
        {
            return messages.Select(aw =>
            {
                var serialized = (aw.Payload as IEnumerable<IPersistentRepresentation>)!.Select(Serialize);
                return TrySeq.Sequence(serialized);
            });
        }
        
        public Try<T> Serialize(IPersistentRepresentation persistentRepr)
        {
            return persistentRepr.Payload switch
            {
                Tagged t => Serialize(persistentRepr.WithPayload(t.Payload), t.Tags),
                _ => Serialize(persistentRepr, ImmutableHashSet<string>.Empty)
            };
        }

        protected abstract Try<T> Serialize(IPersistentRepresentation persistentRepr, IImmutableSet<string> tTags);

        protected abstract Try<(IPersistentRepresentation, IImmutableSet<string>, long)> Deserialize(T t);
    }
}