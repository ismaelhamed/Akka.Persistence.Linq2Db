using System;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Journal;
using Akka.Serialization;
using Akka.Streams;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;

namespace Akka.Persistence.Sql.Linq2Db
{
    public class ByteArrayJournalDao : BaseByteArrayJournalDao
    {
        public ByteArrayJournalDao(IAdvancedScheduler sched, IMaterializer mat, AkkaPersistenceDataConnectionFactory connection, JournalConfig journalConfig, Akka.Serialization.Serialization serializer)
            : base(sched, mat, connection, journalConfig, new ByteArrayJournalSerializer(journalConfig, serializer, journalConfig.PluginConfig.TagSeparator))
        {
        }

        public void InitializeTables()
        {
            using var conn = ConnectionFactory.GetConnection();
            conn.CreateTable<JournalRow>();
        }
    }

    public class AkkaPersistenceDataConnectionFactory
    {
        public AkkaPersistenceDataConnectionFactory(JournalConfig config)
        {
            var fmb = new MappingSchema(MappingSchema.Default).GetFluentMappingBuilder();
            fmb.Entity<JournalRow>()
                .HasSchemaName(config.TableConfiguration.SchemaName)
                .HasTableName(config.TableConfiguration.TableName)
                .Member(r => r.Deleted).HasColumnName(config.TableConfiguration.ColumnNames.Deleted)
                .Member(r => r.Manifest).HasColumnName(config.TableConfiguration.ColumnNames.Manifest)
                .Member(r => r.Message).HasColumnName(config.TableConfiguration.ColumnNames.Message)
                .Member(r => r.Ordering).HasColumnName(config.TableConfiguration.ColumnNames.Ordering)
                .Member(r => r.Tags).HasColumnName(config.TableConfiguration.ColumnNames.Tags)
                .Member(r => r.Identifier).HasColumnName(config.TableConfiguration.ColumnNames.Identifier)
                .Member(r => r.PersistenceId).HasColumnName(config.TableConfiguration.ColumnNames.PersistenceId)
                .Member(r => r.SequenceNumber).HasColumnName(config.TableConfiguration.ColumnNames.SequenceNumber);

            GetConnection = () => new DataConnection(config.ProviderName, config.ConnectionString, fmb.MappingSchema);
        }

        public Func<DataConnection> GetConnection { get; }
    }
}