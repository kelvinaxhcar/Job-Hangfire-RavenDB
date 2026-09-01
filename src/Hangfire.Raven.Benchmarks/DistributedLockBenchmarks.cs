using System;
using BenchmarkDotNet.Attributes;
using Hangfire.Raven.Storage;
using Raven.Client.Documents;

namespace Hangfire.Raven.Benchmarks
{
    [MemoryDiagnoser]
    public class DistributedLockBenchmarks
    {
        private BenchmarkRavenDriver _driver = null!;
        private IDocumentStore _store = null!;
        private RavenStorage _storage = null!;

        [GlobalSetup]
        public void Setup()
        {
            _driver = new BenchmarkRavenDriver();
            _store = _driver.CreateStore("DistributedLockBenchmarkDb");
            _storage = new RavenStorage(_store, new RavenStorageOptions
            {
                EnableChangesApiQueueEvents = false
            });
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _storage?.Dispose();
            _store?.Dispose();
            _driver?.Dispose();
        }

        [Benchmark]
        public void AcquireAndReleaseDistributedLock()
        {
            using var connection = _storage.GetConnection();
            using var dLock = connection.AcquireDistributedLock("benchmark-resource", TimeSpan.FromSeconds(10));
        }
    }
}
