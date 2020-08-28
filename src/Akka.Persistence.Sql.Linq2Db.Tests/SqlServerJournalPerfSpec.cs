using System;
using Akka.Configuration;
using Akka.Persistence.TestKit.Performance;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Sql.Linq2Db.Tests
{
    [Collection("SqlServerSpec")]
    public class SqlServerJournalPerfSpec : JournalPerfSpec
    {
        private static Config InitConfig(SqlServerFixture fixture)
        {
            // need to make sure db is created before the tests start
            DbUtils.Initialize(fixture.ConnectionString);
            
            var specString = $@"
                akka.persistence {{
                    publish-plugin-commands = on
                    journal {{
                        plugin = ""akka.persistence.journal.linq2db""
                        linq2db {{
                            class = ""{typeof(Linq2DbAsyncWriteJournal).AssemblyQualifiedName}""
                            plugin-dispatcher = ""akka.actor.default-dispatcher""
                            provider-name = SqlServer
                            connection-string = ""{DbUtils.ConnectionString}""
                            tables.journal {{ 
                               auto-init = true
                               table-name = EventJournal
                            }}                            
                        }}
                    }}
                }}";

            return ConfigurationFactory.ParseString(specString);
        }

        public SqlServerJournalPerfSpec(ITestOutputHelper output, SqlServerFixture fixture)
            : base(InitConfig(fixture), "SqlServerJournalPerfSpec", output)
        {
            EventsCount = 1000;
            ExpectDuration = TimeSpan.FromMinutes(10);
            MeasurementIterations = 10;
        }

        protected override void AfterAll()
        {
            base.AfterAll();
            DbUtils.Clean();
        }
    }
}