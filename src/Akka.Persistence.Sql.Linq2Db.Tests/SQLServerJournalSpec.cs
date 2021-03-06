﻿using System;
using Akka.Configuration;
using Akka.Persistence.TCK.Journal;
using LinqToDB;
using Xunit.Abstractions;

namespace Akka.Persistence.Sql.Linq2Db.Tests
{
    public class SQLServerJournalSpec : JournalSpec
    {
        private static string connString =
            "Data Source=(LocalDB)\\\\mssqllocaldb";

        private static readonly  Config conf = SQLServerJournalSpecConfig.Create(connString,"journalSpec");
        public SQLServerJournalSpec(ITestOutputHelper output)
            : base(conf, "SQLServer", output)
        {
            var connFactory = new AkkaPersistenceDataConnectionFactory(new JournalConfig(conf.GetConfig("akka.persistence.journal.testspec")));
            using (var conn = connFactory.GetConnection())
            {
                try
                {

                    conn.GetTable<JournalRow>().Delete();
                }
                catch (Exception e)
                {
                   
                }
            }

            Initialize();
        }
        // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
        protected override bool SupportsSerialization => false;
    }
}