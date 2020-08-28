using System.Threading.Tasks;

namespace Akka.Persistence.Sql.Linq2Db
{
    /// <summary>
    /// A [[JournalDao]] with extended capabilities, such as updating payloads and tags of existing events.
    /// These operations should be used sparingly, for example for migrating data from un-encrypted to encrypted formats
    /// </summary>
    public interface IJournalDaoWithUpdates : IJournalDao
    {
        /// <summary>
        /// Update (!) an existing event with the passed in data.
        /// </summary>
        Task<Done> Update(string persistenceId, long sequenceNr, object payload);
    }
}