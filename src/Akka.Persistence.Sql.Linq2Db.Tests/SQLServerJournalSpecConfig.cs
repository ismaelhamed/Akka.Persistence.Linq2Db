using Akka.Configuration;

namespace Akka.Persistence.Sql.Linq2Db.Tests
{
    public static class SQLServerJournalSpecConfig
    {
        public static string JournalBaseConfig = @"
            akka.persistence {{
                publish-plugin-commands = on
                journal {{
                    plugin = ""akka.persistence.journal.testspec""
                    testspec {{
                        class = ""{0}""
                        plugin-dispatcher = ""akka.actor.default-dispatcher""
                        connection-string = ""{1}""
                        #connection-string = ""FullUri=file:test.db&cache=shared""
                        provider-name = """ + LinqToDB.ProviderName.SqlServer + @"""
                        tables.journal {{ 
                           auto-init = true
                           table-name = ""{2}"" 
                        }}
                    }}
                }}
            }}
        ";
        
        public static Config Create(string connString, string tableName) => 
            ConfigurationFactory.ParseString(string.Format(JournalBaseConfig, typeof(Linq2DbAsyncWriteJournal).AssemblyQualifiedName, connString,tableName));
    }
}