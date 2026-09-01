using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Raven.Dashboard;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public static class IntegrationTestJobs
    {
        public static void ExecuteWorkload(string arg)
        {
        }
    }

    public class RavenStorageIntegrationFacts : TesteBase
    {
        public RavenStorageIntegrationFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void FullJobLifecycle_WithRealRavenDb_ExecutesCorrectly()
        {
            UseStorage(storage =>
            {
                using var connection = storage.GetConnection() as RavenConnection;
                Assert.NotNull(connection);

                // 1. Create expired job
                var job = Job.FromExpression(() => IntegrationTestJobs.ExecuteWorkload("Lifecycle-1"));
                var jobId = connection.CreateExpiredJob(job, new Dictionary<string, string> { ["Env"] = "Integration" }, DateTime.UtcNow, TimeSpan.FromDays(7));
                Assert.NotNull(jobId);

                // 2. Transition to EnqueuedState and add to queue
                using (var tx = connection.CreateWriteTransaction())
                {
                    tx.SetJobState(jobId, new EnqueuedState("default"));
                    tx.AddToQueue("default", jobId);
                    tx.Commit();
                }

                // 3. Dequeue / Fetch next job using queue provider
                var queue = new RavenJobQueue(storage, new RavenStorageOptions());
                var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                var fetchedJob = queue.Dequeue(new[] { "default" }, cts.Token);

                Assert.NotNull(fetchedJob);
                Assert.Equal(jobId, fetchedJob.JobId);

                // 4. Transition to Succeeded state
                using (var tx = connection.CreateWriteTransaction())
                {
                    tx.SetJobState(jobId, new SucceededState("Result-Data", 150, 150));
                    tx.Commit();
                }

                fetchedJob.RemoveFromQueue();

                // 5. Verify final Job Data & State Data from RavenDB
                var jobData = connection.GetJobData(jobId);
                Assert.NotNull(jobData);
                Assert.Equal("Succeeded", jobData.State);
                Assert.Equal("Lifecycle-1", jobData.Job.Args[0]);

                var stateData = connection.GetStateData(jobId);
                Assert.NotNull(stateData);
                Assert.Equal("Succeeded", stateData.Name);
            });
        }

        [Fact]
        public void JavaScriptProjection_GetFirstByLowestScore_WithRealRavenDb()
        {
            UseStorage(storage =>
            {
                using var connection = storage.GetConnection() as RavenConnection;
                Assert.NotNull(connection);

                using (var tx = connection.CreateWriteTransaction())
                {
                    tx.AddToSet("recurring-jobs-scores", "job-c", 300);
                    tx.AddToSet("recurring-jobs-scores", "job-a", 100);
                    tx.AddToSet("recurring-jobs-scores", "job-b", 200);
                    tx.Commit();
                }

                // Lowest score in range [50, 250] should be "job-a"
                var firstLowest = connection.GetFirstByLowestScoreFromSet("recurring-jobs-scores", 50, 250);
                Assert.Equal("job-a", firstLowest);

                // Lowest score in range [150, 350] should be "job-b"
                var secondLowest = connection.GetFirstByLowestScoreFromSet("recurring-jobs-scores", 150, 350);
                Assert.Equal("job-b", secondLowest);
            });
        }

        [Fact]
        public void BulkEnqueueAndBatchDeletion_WithRealRavenDb_ExecutesHighThroughput()
        {
            UseStorage(storage =>
            {
                // 1. Bulk Enqueue 50 jobs via RavenDB BulkInsert
                var tasks = Enumerable.Range(1, 50)
                    .Select(i => (System.Linq.Expressions.Expression<Action>)(() => IntegrationTestJobs.ExecuteWorkload($"Bulk-{i}")))
                    .ToList();

                var jobIds = storage.BulkEnqueue(tasks, queue: "bulk-queue");
                Assert.Equal(50, jobIds.Count);

                // Verify jobs exist
                using (var connection = storage.GetConnection() as RavenConnection)
                {
                    Assert.NotNull(connection);
                    var firstJob = connection.GetJobData(jobIds[0]);
                    Assert.NotNull(firstJob);
                    Assert.Equal("Enqueued", firstJob.State);

                    // 2. Batch Delete by Queue using DeleteByQueryOperation
                    var deleted = storage.DeleteJobsByQueue("bulk-queue");
                    Assert.True(deleted >= 50);

                    // Verify job no longer exists
                    var checkJob = connection.GetJobData(jobIds[0]);
                    Assert.Null(checkJob);
                }
            });
        }

        [Fact]
        public void RealDistributedLock_ConcurrentAcquire_EnforcesMutualExclusion()
        {
            UseStorage(storage =>
            {
                const string resource = "integration-shared-resource";
                int counter = 0;
                var tasks = new List<Task>();

                for (int i = 0; i < 5; i++)
                {
                    tasks.Add(Task.Run(() =>
                    {
                        using var connection = storage.GetConnection();
                        using (connection.AcquireDistributedLock(resource, TimeSpan.FromSeconds(10)))
                        {
                            var current = counter;
                            Thread.Sleep(50);
                            counter = current + 1;
                        }
                    }));
                }

                Task.WaitAll(tasks.ToArray());
                Assert.Equal(5, counter);
            });
        }

        [Fact]
        public void MonitoringApi_OnRealRavenDb_ReturnsAccurateMetrics()
        {
            UseStorage(storage =>
            {
                using (var connection = storage.GetConnection())
                {
                    var job1 = connection.CreateExpiredJob(Job.FromExpression(() => IntegrationTestJobs.ExecuteWorkload("M1")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                    var job2 = connection.CreateExpiredJob(Job.FromExpression(() => IntegrationTestJobs.ExecuteWorkload("M2")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

                    using (var tx = connection.CreateWriteTransaction())
                    {
                        tx.SetJobState(job1, new SucceededState("ok", 100, 100));
                        tx.SetJobState(job2, new FailedState(new Exception("Error"), "Failed"));
                        tx.Commit();
                    }

                    connection.AnnounceServer("server-test-1", new ServerContext
                    {
                        Queues = new[] { "default", "critical" },
                        WorkerCount = 10
                    });
                }

                var monitoring = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                Assert.NotNull(monitoring);

                var stats = monitoring.GetStatistics();
                Assert.NotNull(stats);
                Assert.True(stats.Succeeded >= 1);
                Assert.True(stats.Failed >= 1);
                Assert.True(stats.Servers >= 1);

                var ravenMetrics = monitoring.GetRavenMetrics();
                Assert.NotNull(ravenMetrics);
                Assert.NotNull(ravenMetrics.DatabaseName);
                Assert.True(ravenMetrics.DocumentsCount >= 2);
            });
        }
    }
}
