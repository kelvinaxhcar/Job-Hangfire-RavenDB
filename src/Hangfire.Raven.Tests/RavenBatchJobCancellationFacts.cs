using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Dashboard;
using Hangfire.Raven.Dashboard.UI5;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Newtonsoft.Json.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenBatchJobCancellationFacts : TesteBase
    {
        public RavenBatchJobCancellationFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void DeleteByState_DeletesAllJobsInGivenState()
        {
            UseStorage(storage =>
            {
                using (var connection = storage.GetConnection())
                {
                    var job1 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                    var job2 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                    var job3 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

                    using (var tx = connection.CreateWriteTransaction())
                    {
                        tx.SetJobState(job1, new FailedState(new Exception("Fail 1"), "Failed"));
                        tx.SetJobState(job2, new FailedState(new Exception("Fail 2"), "Failed"));
                        tx.SetJobState(job3, new SucceededState(null, 100, 100));
                        tx.Commit();
                    }

                    var batch = connection as IBatchJobCancellation;
                    Assert.NotNull(batch);

                    long deletedCount = batch.DeleteByState(FailedState.StateName);
                    Assert.True(deletedCount >= 2);

                    // Succeeded job should still exist
                    var job3Data = connection.GetJobData(job3);
                    Assert.NotNull(job3Data);
                }
            });
        }

        [Fact]
        public void DeleteByQueue_DeletesQueueAndEnqueuedJobs()
        {
            UseStorage(storage =>
            {
                using (var connection = storage.GetConnection())
                {
                    var job1 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                    var job2 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

                    using (var tx = connection.CreateWriteTransaction())
                    {
                        tx.SetJobState(job1, new EnqueuedState("critical-queue"));
                        tx.SetJobState(job2, new EnqueuedState("other-queue"));
                        tx.Commit();
                    }

                    var deletedCount = storage.DeleteJobsByQueue("critical-queue");
                    Assert.True(deletedCount >= 1);
                }
            });
        }

        [Fact]
        public void DeleteJobs_DeletesSpecificJobIds()
        {
            UseStorage(storage =>
            {
                using (var connection = storage.GetConnection())
                {
                    var job1 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                    var job2 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                    var job3 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

                    long deleted = storage.DeleteJobs(new[] { job1, job2 });
                    Assert.Equal(2, deleted);

                    Assert.Null(connection.GetJobData(job1));
                    Assert.Null(connection.GetJobData(job2));
                    Assert.NotNull(connection.GetJobData(job3));
                }
            });
        }

        [Fact]
        public async Task DeleteJobsByStateAsync_ExecutesSuccessfully()
        {
            UseStorage(async storage =>
            {
                using (var connection = storage.GetConnection())
                {
                    var job1 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

                    using (var tx = connection.CreateWriteTransaction())
                    {
                        tx.SetJobState(job1, new DeletedState());
                        tx.Commit();
                    }

                    var deleted = await storage.DeleteJobsByStateAsync(DeletedState.StateName);
                    Assert.True(deleted >= 1);
                }
            });
        }

        [Fact]
        public async Task UI5ApiDispatcher_HandlesBatchDeleteRequest()
        {
            UseStorage(async storage =>
            {
                using (var connection = storage.GetConnection())
                {
                    var job1 = connection.CreateExpiredJob(Job.FromExpression(() => SampleWorkload()), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

                    using (var tx = connection.CreateWriteTransaction())
                    {
                        tx.SetJobState(job1, new FailedState(new Exception("Error"), "Failed"));
                        tx.Commit();
                    }

                    var dispatcher = new RavenUI5ApiDispatcher();

                    var requestMock = new Mock<DashboardRequest>();
                    requestMock.Setup(r => r.Path).Returns("/api/ui5/batch/delete");
                    requestMock.Setup(r => r.GetQuery("state")).Returns("Failed");

                    var responseMock = new Mock<DashboardResponse>();
                    var output = new StringWriter();
                    responseMock.Setup(r => r.WriteAsync(It.IsAny<string>()))
                                .Callback<string>(s => output.Write(s))
                                .Returns(Task.CompletedTask);

                    var contextMock = new Mock<DashboardContext>(storage, new DashboardOptions(), null);
                    contextMock.Setup(c => c.Request).Returns(requestMock.Object);
                    contextMock.Setup(c => c.Response).Returns(responseMock.Object);
                    contextMock.Setup(c => c.Storage).Returns(storage);

                    await dispatcher.Dispatch(contextMock.Object);

                    var json = output.ToString();
                    Assert.Contains("\"status\":\"ok\"", json);
                    Assert.Contains("\"deletedCount\"", json);
                }
            });
        }

        public static void SampleWorkload()
        {
        }
    }
}
