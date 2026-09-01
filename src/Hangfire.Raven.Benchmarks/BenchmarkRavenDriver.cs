using System;
using Raven.Client.Documents;
using Raven.TestDriver;

namespace Hangfire.Raven.Benchmarks
{
    public class BenchmarkRavenDriver : RavenTestDriver
    {
        static BenchmarkRavenDriver()
        {
            Environment.SetEnvironmentVariable("DOTNET_ROLL_FORWARD", "Major");
            ConfigureServer(new TestServerOptions
            {
                FrameworkVersion = null
            });
        }

        public IDocumentStore CreateStore(string databaseName = "HangfireBenchmarkDb")
        {
            var store = GetDocumentStore(database: databaseName);
            return store;
        }

        protected override void PreInitialize(IDocumentStore documentStore)
        {
            documentStore.Conventions.MaxNumberOfRequestsPerSession = 500;
            documentStore.Conventions.UseOptimisticConcurrency = false;
            documentStore.Conventions.IdentityPartsSeparator = '-';
            documentStore.Conventions.SaveEnumsAsIntegers = true;
        }
    }
}
