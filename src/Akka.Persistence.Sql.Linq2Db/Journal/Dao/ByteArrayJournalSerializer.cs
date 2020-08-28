using System;
using System.Collections.Immutable;
using System.Linq;
using Akka.Actor;
using Akka.Configuration;
using Akka.Serialization;
using Akka.Util;

namespace Akka.Persistence.Sql.Linq2Db
{
    public class ByteArrayJournalSerializer : FlowPersistentReprSerializer<JournalRow>
    {
        private readonly Akka.Serialization.Serialization _serializer;
        private readonly string _separator;
        private readonly JournalConfig _journalConfig;

        public ByteArrayJournalSerializer(JournalConfig journalConfig, Akka.Serialization.Serialization serializer, string separator)
        {
            _journalConfig = journalConfig;
            _serializer = serializer;
            _separator = separator;
        }
        protected override Try<JournalRow> Serialize(IPersistentRepresentation persistentRepr, IImmutableSet<string> tTags)
        {

            return Try<JournalRow>.From(() =>
            {
                var serializer = _serializer.FindSerializerForType(persistentRepr.Payload.GetType(),_journalConfig.DefaultSerializer);
                // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                string manifest = "";
                var binary = Akka.Serialization.Serialization.WithTransport(_serializer.System, () =>
                {
                
                    if (serializer is SerializerWithStringManifest stringManifest)
                    {
                        manifest =
                            stringManifest.Manifest(persistentRepr.Payload);
                    }
                    else
                    {
                        if (serializer.IncludeManifest)
                        {
                            manifest = persistentRepr.Payload.GetType().TypeQualifiedName();
                        }
                    }

                    return serializer.ToBinary(persistentRepr.Payload);
                });
                return new JournalRow()
                {
                    Manifest = manifest,
                    Message = binary,
                    PersistenceId = persistentRepr.PersistenceId,
                    Tags = tTags.Any()?  tTags.Aggregate((tl, tr) => tl + _separator + tr) : "",
                    Identifier = serializer.Identifier, SequenceNumber = persistentRepr.SequenceNr
                };
            });
        }

        protected override Try<(IPersistentRepresentation, IImmutableSet<string>, long)> Deserialize(JournalRow t)
        {
            return Try<(IPersistentRepresentation, IImmutableSet<string>, long)>.From(
                () =>
                {
                    object deserialized = null;
                    if (t.Identifier.HasValue == false)
                    {
                        var type = System.Type.GetType(t.Manifest, true);
                        var deserializer =
                            _serializer.FindSerializerForType(type, null);
                        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                        deserialized =
                            Akka.Serialization.Serialization.WithTransport(
                                _serializer.System,
                                () => deserializer.FromBinary(t.Message, type));
                    }
                    else
                    {
                        var serializerId = t.Identifier.Value;
                        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                        deserialized = _serializer.Deserialize(t.Message,
                            serializerId, t.Manifest);
                    }

                    return (
                        new Persistent(deserialized, t.SequenceNumber,
                            t.PersistenceId,
                            t.Manifest, t.Deleted, ActorRefs.NoSender, null),
                        t.Tags.Split(new[] {_separator},
                                StringSplitOptions.RemoveEmptyEntries)
                            .ToImmutableHashSet(),
                        t.Ordering);
                }
            );
        }
    }
}