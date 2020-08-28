using LinqToDB.Mapping;

namespace Akka.Persistence.Sql.Linq2Db
{
    public sealed class JournalRow
    {
        public long Ordering { get; set; }
        public bool Deleted { get; set; }
        [PrimaryKey, NotNull]
        public string PersistenceId { get; set; }
        [PrimaryKey]
        public long SequenceNumber { get; set; }
        public byte[] Message { get; set; }
        public string Tags { get; set; }
        public string Manifest { get; set; }
        public int? Identifier { get; set; }
    }
}