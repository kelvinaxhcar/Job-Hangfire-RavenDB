﻿using Hangfire.Raven.Entities;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using System;
using System.Linq;
using System.Threading;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenJobQueueFacts : TesteBase
    {
        private static readonly string[] DefaultQueues = { "default" };

        public RavenJobQueueFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new RavenJobQueue(null, new RavenStorageOptions()));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsValueIsNull()
        {
            UseStorage(storage =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                   () => new RavenJobQueue(storage, null));

                Assert.Equal("options", exception.ParamName);
            });
        }

        [Fact]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsNull()
        {
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                var exception = Assert.Throws<ArgumentNullException>(
                    () => queue.Dequeue(null, CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        public void Dequeue_ShouldThrowAnException_WhenQueuesCollectionIsEmpty()
        {
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                var exception = Assert.Throws<ArgumentException>(
                    () => queue.Dequeue(new string[0], CreateTimingOutCancellationToken()));

                Assert.Equal("queues", exception.ParamName);
            });
        }

        [Fact]
        public void Dequeue_ThrowsOperationCanceled_WhenCancellationTokenIsSetAtTheBeginning()
        {
            UseStorage(storage =>
            {
                var cts = new CancellationTokenSource();
                cts.Cancel();
                var queue = CreateJobQueue(storage);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact]
        public void Dequeue_ShouldWaitIndefinitely_WhenThereAreNoJobs()
        {
            UseStorage(storage =>
            {
                var cts = new CancellationTokenSource(200);
                var queue = CreateJobQueue(storage);

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, cts.Token));
            });
        }

        [Fact]
        public void Dequeue_ShouldFetchAJob_FromTheSpecifiedQueue()
        {
            // Arrange
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                var jobQueue = new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "default", "1"),
                    JobId = "1",
                    Queue = "default"
                };

                using (var _session = storage.Repository.OpenSession())
                {
                    _session.Store(jobQueue);
                    _session.SaveChanges();
                }

                // Act
                RavenFetchedJob payload = (RavenFetchedJob)queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var id = jobQueue.Id;
                Assert.Equal(id, payload.Id);
                Assert.Equal("1", payload.JobId);
                Assert.Equal("default", payload.Queue);
            });
        }

        [Fact]
        public void Dequeue_ShouldLeaveJobInTheQueue_ButSetItsFetchedAtValue()
        {
            // Arrange
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                var jobId = Guid.NewGuid().ToString();

                var job = new RavenJob
                {
                    Id = storage.Repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                var jobQueue = new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "default", jobId),
                    JobId = jobId,
                    Queue = "default"
                };

                using (var _session = storage.Repository.OpenSession())
                {
                    _session.Store(job);
                    _session.Store(jobQueue);
                    _session.SaveChanges();
                }

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotNull(payload);

                var fetchedAt = _session.Query<JobQueue>().Where(_ => _.JobId == payload.JobId).FirstOrDefault().FetchedAt;

                Assert.NotNull(fetchedAt);
                Assert.True(fetchedAt > DateTime.UtcNow.AddMinutes(-1));
            });
        }

        [Fact]
        public void Dequeue_ShouldFetchATimedOutJobs_FromTheSpecifiedQueue()
        {
            // Arrange
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                var jobId = Guid.NewGuid().ToString();

                var job = new RavenJob
                {
                    Id = storage.Repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                var jobQueue = new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "default", jobId),
                    JobId = jobId,
                    Queue = "default",
                    FetchedAt = DateTime.UtcNow.AddDays(-1)
                };

                _session.Store(job);
                _session.Store(jobQueue);
                _session.SaveChanges();

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                Assert.NotEmpty(payload.JobId);
            });
        }

        [Fact]
        public void Dequeue_ShouldFetchJobs_OnlyFromSpecifiedQueues()
        {
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                var job1Id = Guid.NewGuid().ToString();
                var job1 = new RavenJob
                {
                    Id = storage.Repository.GetId(typeof(RavenJob), job1Id),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };
                _session.Store(job1);

                _session.Store(new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "critical", job1Id),
                    JobId = job1Id,
                    Queue = "critical"
                });
                _session.SaveChanges();

                Assert.Throws<OperationCanceledException>(() => queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken()));
            });
        }

        [Fact]
        public void Dequeue_ShouldSetFetchedAt_OnlyForTheFetchedJob()
        {
            UseStorage(storage =>
            {
                // Arrange
                var queue = CreateJobQueue(storage);

                var job1Id = Guid.NewGuid().ToString();
                var job1 = new RavenJob
                {
                    Id = storage.Repository.GetId(typeof(RavenJob), job1Id),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };
                _session.Store(job1);

                var job2Id = Guid.NewGuid().ToString();
                var job2 = new RavenJob
                {
                    Id = storage.Repository.GetId(typeof(RavenJob), job2Id),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };
                _session.Store(job2);

                _session.Store(new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "default", job1Id),
                    JobId = job1Id,
                    Queue = "default"
                });

                _session.Store(new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "default", job2Id),
                    JobId = job2Id,
                    Queue = "default"
                });
                _session.SaveChanges();

                // Act
                var payload = queue.Dequeue(DefaultQueues, CreateTimingOutCancellationToken());

                // Assert
                var otherJobFetchedAt = _session.Query<JobQueue>().Where(_ => _.JobId != payload.JobId).FirstOrDefault().FetchedAt;

                Assert.Null(otherJobFetchedAt);
            });
        }

        [Fact]
        public void Dequeue_ShouldFetchJobs_FromMultipleQueuesBasedOnQueuePriority()
        {
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                var criticalJobId = Guid.NewGuid().ToString();
                var criticalJob = new RavenJob
                {
                    Id = storage.Repository.GetId(typeof(RavenJob), criticalJobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };
                _session.Store(criticalJob);

                var defaultJobId = Guid.NewGuid().ToString();
                var defaultJob = new RavenJob
                {
                    Id = storage.Repository.GetId(typeof(RavenJob), defaultJobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };
                _session.Store(defaultJob);

                _session.Store(new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "default", defaultJobId),
                    JobId = defaultJobId,
                    Queue = "default"
                });

                _session.Store(new JobQueue
                {
                    Id = storage.Repository.GetId(typeof(JobQueue), "critical", criticalJobId),
                    JobId = criticalJobId,
                    Queue = "critical"
                });
                _session.SaveChanges();

                var critical = (RavenFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(critical.JobId);
                Assert.Equal("critical", critical.Queue);

                var @default = (RavenFetchedJob)queue.Dequeue(
                    new[] { "critical", "default" },
                    CreateTimingOutCancellationToken());

                Assert.NotNull(@default.JobId);
                Assert.Equal("default", @default.Queue);
            });
        }

        [Fact]
        public void Enqueue_AddsAJobToTheQueue()
        {
            UseStorage(storage =>
            {
                var queue = CreateJobQueue(storage);

                queue.Enqueue("default", "1");

                using (var _session = storage.Repository.OpenSession())
                {
                    var record = _session.Query<JobQueue>().Single();
                    Assert.Equal("1", record.JobId.ToString());
                    Assert.Equal("default", record.Queue);
                    Assert.Null(record.FetchedAt);
                }
            });
        }

        private static CancellationToken CreateTimingOutCancellationToken()
        {
            var source = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            return source.Token;
        }

        private static RavenJobQueue CreateJobQueue(RavenStorage storage)
        {
            return new RavenJobQueue(storage, new RavenStorageOptions());
        }
    }
}
