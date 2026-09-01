using System;
using System.Collections.Generic;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Hangfire.Common;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Raven.Client.Documents;

namespace Hangfire.Raven.Benchmarks
{
    [MemoryDiagnoser]
    public class JobStorageBenchmarks
    {
        private BenchmarkRavenDriver _driver = null!;
        private IDocumentStore _store = null!;
        private RavenStorage _storage = null!;
        private Job _sampleJob = null!;
        private Dictionary<string, string> _sampleParameters = null!;
        private RavenJobQueue _queue = null!;

        [Params(10, 50)]
        public int BatchSize { get; set; }

        [GlobalSetup]
        public void Setup()
        {
            _driver = new BenchmarkRavenDriver();
            _store = _driver.CreateStore("JobStorageBenchmarkDb");
            _storage = new RavenStorage(_store, new RavenStorageOptions
            {
                EnableChangesApiQueueEvents = false
            });
            _queue = new RavenJobQueue(_storage, new RavenStorageOptions
            {
                EnableChangesApiQueueEvents = false
            });

            _sampleJob = Job.FromExpression(() => SampleWorkload());
            _sampleParameters = new Dictionary<string, string>
            {
                ["Param1"] = "Value1",
                ["Param2"] = "Value2"
            };

            // Prepopulate some data for statistics
            using var connection = _storage.GetConnection();
            using var transaction = connection.CreateWriteTransaction();
            for (int i = 0; i < 20; i++)
            {
                var id = connection.CreateExpiredJob(_sampleJob, _sampleParameters, DateTime.UtcNow, TimeSpan.FromDays(1));
                transaction.SetJobState(id, new EnqueuedState("default"));
            }
            transaction.Commit();
        }

        [GlobalCleanup]
        public void Cleanup()
        {
            _queue?.Dispose();
            _storage?.Dispose();
            _store?.Dispose();
            _driver?.Dispose();
        }

        [Benchmark]
        public string CreateExpiredJob()
        {
            using var connection = _storage.GetConnection();
            return connection.CreateExpiredJob(_sampleJob, _sampleParameters, DateTime.UtcNow, TimeSpan.FromDays(1));
        }

        [Benchmark]
        public List<string> BatchEnqueue()
        {
            var jobs = new List<Job>(BatchSize);
            for (int i = 0; i < BatchSize; i++)
            {
                jobs.Add(_sampleJob);
            }

            return _storage.BulkEnqueue(jobs, "default");
        }

        [Benchmark]
        public IFetchedJob? FetchNextJob()
        {
            // Enqueue one item first so fetch succeeds
            using (var connection = _storage.GetConnection())
            {
                var id = connection.CreateExpiredJob(_sampleJob, _sampleParameters, DateTime.UtcNow, TimeSpan.FromDays(1));
                using var tx = connection.CreateWriteTransaction();
                tx.SetJobState(id, new EnqueuedState("benchmark-queue"));
                tx.Commit();
            }

            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
            using var fetched = _queue.Dequeue(new[] { "benchmark-queue" }, cts.Token);
            return fetched;
        }

        [Benchmark]
        public StatisticsDto GetStatistics()
        {
            var monitoring = _storage.GetMonitoringApi();
            return monitoring.GetStatistics();
        }

        public static void SampleWorkload()
        {
        }
    }
}
