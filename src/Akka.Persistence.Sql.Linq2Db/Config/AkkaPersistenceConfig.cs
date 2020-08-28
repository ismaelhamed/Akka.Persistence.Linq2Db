// //-----------------------------------------------------------------------
// // <copyright file="AkkaPersistenceConfig.cs" company="Akka.NET Project">
// //     Copyright (C) 2009-2020 Lightbend Inc. <http://www.lightbend.com>
// //     Copyright (C) 2013-2020 .NET Foundation <https://github.com/akkadotnet/akka.net>
// // </copyright>
// //-----------------------------------------------------------------------

using System;
using Akka.Configuration;

namespace Akka.Persistence.Sql.Linq2Db
{
    public class JournalConfig
    {
        public JournalConfig(Config config)
        {
            ConnectionString = config.GetString("connection-string");
            ProviderName = config.GetString("provider-name");
            TableConfiguration = new JournalTableConfiguration(config);
            PluginConfig = new JournalPluginConfig(config);
            DaoConfig = new BaseByteArrayJournalDaoConfig(config);
            var dbConf = config.GetString(ConfigKeys.useSharedDb);
            UseSharedDb = string.IsNullOrWhiteSpace(dbConf) ? null : dbConf;
        }

        public string UseSharedDb { get; }
        public BaseByteArrayJournalDaoConfig DaoConfig { get; }
        public JournalPluginConfig PluginConfig { get; }
        public JournalTableConfiguration TableConfiguration { get; }
        public string DefaultSerializer { get; }
        public string ProviderName { get; }
        public string ConnectionString { get; }
    }

    public class JournalPluginConfig
    {
        public JournalPluginConfig(Config config)
        {
            TagSeparator = config.GetString("tag-separator", ",");
            //TODO: FILL IN SANELY
            Dao = config.GetString("dao", "akka.persistence.sql.linq2db.dao");
        }

        public string TagSeparator { get; protected set; }
        public string Dao { get; protected set; }
    }

    public class JournalTableColumnNames
    {
        public string FallBack = @"tables.journal.column-names {}";

        public JournalTableColumnNames(Config config)
        {
            var cfg = config.GetConfig("tables.journal.column-names").SafeWithFallback(ConfigurationFactory.ParseString(FallBack).GetConfig("tables.journal.column-names"));
            Ordering = cfg.GetString("ordering", "ordering");
            Deleted = cfg.GetString("deleted", "deleted");
            PersistenceId = cfg.GetString("persistenceId", "persistence_id");
            SequenceNumber = cfg.GetString("sequenceNumber", "sequence_number");
            Created = cfg.GetString("created", "created");
            Tags = cfg.GetString("tags", "tags");
            Message = cfg.GetString("message", "message");
            Identifier = cfg.GetString("identifier", "identifier");
            Manifest = cfg.GetString("manifest", "manifest");
        }

        public string Ordering { get; }
        public string Deleted { get; }
        public string PersistenceId { get; }
        public string SequenceNumber { get; }
        public string Created { get; }
        public string Tags { get; }
        public string Message { get; }
        public string Identifier { get; }
        public string Manifest { get; }
    }

    public class JournalTableConfiguration
    {
        public JournalTableConfiguration(Config config)
        {
            var localcfg = config.GetConfig("tables.journal").SafeWithFallback(Config.Empty);
            ColumnNames = new JournalTableColumnNames(config);
            TableName = localcfg.GetString("table-name", "journal");
            SchemaName = localcfg.GetString("schema-name");
            AutoInitialize = localcfg.GetBoolean("auto-init");
        }

        public JournalTableColumnNames ColumnNames { get; }
        public string TableName { get; }
        public string SchemaName { get; }
        public bool AutoInitialize { get; }
    }

    public class JournalSequenceRetrievalConfig
    {
        public JournalSequenceRetrievalConfig(Config config)
        {
            BatchSize = config.GetInt("journal-sequence-retrieval.batch-size", 10000);
            MaxTries = config.GetInt("journal-sequence-retrieval.max-tries", 10);
            QueryDelay = config.GetTimeSpan("journal-sequence-retrieval.query-delay", TimeSpan.FromSeconds(1));
            MaxBackoffQueryDelay = config.GetTimeSpan("journal-sequence-retrieval.max-backoff-query-delay", TimeSpan.FromSeconds(60));
            AskTimeout = config.GetTimeSpan("journal-sequence-retrieval.ask-timeout", TimeSpan.FromSeconds(1));
        }

        public TimeSpan AskTimeout { get;}
        public TimeSpan MaxBackoffQueryDelay { get; }
        public TimeSpan QueryDelay { get; }
        public int MaxTries { get; }
        public int BatchSize { get; }

        public static JournalSequenceRetrievalConfig Apply(Config config) => new JournalSequenceRetrievalConfig(config);
    }
}