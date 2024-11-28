using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Raven.Client.Documents.Session;
using System;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenFetchedJobFacts : TesteBase
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        public RavenFetchedJobFacts(ITestOutputHelper helper): base(helper)
        {

        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            UseStorage(storage =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => new RavenFetchedJob(null, new JobQueue()));

                Assert.Equal("storage", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobQueueIsNull()
        {
            UseStorage(storage =>
            {
                var exception = Assert.Throws<ArgumentNullException>(() => new RavenFetchedJob(storage, null));

                Assert.Equal("jobQueue", exception.ParamName);
            });
        }

        [Fact]
        public void Ctor_CorrectlySets_AllInstanceProperties()
        {
            UseStorage(storage =>
            {
                var fetchedJob = new RavenFetchedJob(storage, new JobQueue { Id = "1", JobId = JobId, Queue = Queue });

                Assert.Equal("1", fetchedJob.Id);
                Assert.Equal(JobId, fetchedJob.JobId);
                Assert.Equal(Queue, fetchedJob.Queue);
            });
        }

        [Fact]
        public void RemoveFromQueue_ReallyDeletesTheJobFromTheQueue()
        {
            UseStorage(storage =>
            {
                // Arrange
                var id = CreateJobQueueRecord(storage, "1", "default");
                var processingJob = new RavenFetchedJob(storage, new JobQueue { Id = id, JobId = "1", Queue = "default" });

                // Act
                processingJob.RemoveFromQueue();

                // Assert
                var count = _session.Query<JobQueue>().Where(x => x.Id == id).Count();
                Assert.Equal(0, count);
            });
        }

        [Fact]
        public void RemoveFromQueue_DoesNotDelete_UnrelatedJobs()
        {
            UseStorage(storage =>
            {
                // Arrange
                CreateJobQueueRecord(storage, "1", "default");
                CreateJobQueueRecord(storage, "1", "critical");
                CreateJobQueueRecord(storage, "2", "default");

                var fetchedJob = new RavenFetchedJob(storage, new JobQueue { Id = "999", JobId = "1", Queue = "default" });

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert

                var count = _session.Query<JobQueue>().Count();
                Assert.Equal(3, count);
            });
        }

        [Fact]
        public void Requeue_SetsFetchedAtValueToNull()
        {
            UseStorage(storage =>
            {
                // Arrange
                var id = CreateJobQueueRecord(storage, "1", "default");
                var processingJob = new RavenFetchedJob(storage, new JobQueue { Id = id, JobId = "1", Queue = "default" });

                // Act
                processingJob.Requeue();

                // Assert
                var record = _session.Load<JobQueue>(id);
                _session.Advanced.Refresh(record);
                Assert.Null(record.FetchedAt);
            });
        }

        [Fact]
        public void Dispose_SetsFetchedAtValueToNull_IfThereWereNoCallsToComplete()
        {
            UseStorage(storage =>
            {
                // Arrange
                var id = CreateJobQueueRecord(_session, "1", "default");
                var processingJob = new RavenFetchedJob(storage, new JobQueue { Id = id, JobId = "1", Queue = "default" });

                // Act
                processingJob.Dispose();

                // Assert
                var record = _session.Load<JobQueue>(id);
                _session.Advanced.Refresh(record);
                Assert.Null(record.FetchedAt);
            });
        }

        private static string CreateJobQueueRecord(RavenStorage storage, string jobId, string queue)
        {
            var jobQueue = new JobQueue
            {
                Id = storage.Repository.GetId(typeof(JobQueue), queue, jobId),
                JobId = jobId,
                Queue = queue,
                FetchedAt = DateTime.UtcNow
            };

            using (var session = storage.Repository.OpenSession())
            {
                session.Store(jobQueue);
                session.SaveChanges();
            }

            return jobQueue.Id;
        }

        private static string CreateJobQueueRecord(IDocumentSession session, string jobId, string queue)
        {
            var jobQueue = new JobQueue
            {
                Id = GetId(typeof(JobQueue), queue, jobId),
                JobId = jobId,
                Queue = queue,
                FetchedAt = DateTime.UtcNow
            };

            session.Store(jobQueue);
            session.SaveChanges();

            return jobQueue.Id;
        }

        public static string GetId(Type type, params string[] id)
        {
            return type.ToString() + "/" + string.Join("/", id);
        }
    }
}
