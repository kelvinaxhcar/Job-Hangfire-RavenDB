using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenAsyncConnectionFacts
    {
        [Fact]
        public async Task CreateExpiredJobAsync_StoresAndSavesAsync()
        {
            var repositoryMock = new Mock<IRepository>();
            var asyncSessionMock = new Mock<IAsyncDocumentSession>();

            repositoryMock.Setup(r => r.OpenAsyncSession(It.IsAny<SessionOptions>())).Returns(asyncSessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), It.IsAny<string[]>())).Returns<Type, string[]>((t, ids) => $"RavenJobs/{ids[0]}");

            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            var job = Job.FromExpression(() => SampleAsyncMethod());
            var parameters = new Dictionary<string, string> { { "key1", "val1" } };

            var jobId = await connection.CreateExpiredJobAsync(job, parameters, DateTime.UtcNow, TimeSpan.FromDays(1));

            Assert.NotNull(jobId);
            asyncSessionMock.Verify(s => s.StoreAsync(It.IsAny<RavenJob>(), It.IsAny<CancellationToken>()), Times.Once);
            asyncSessionMock.Verify(s => s.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task GetJobDataAsync_LoadsAndDeserializesJob()
        {
            var repositoryMock = new Mock<IRepository>();
            var asyncSessionMock = new Mock<IAsyncDocumentSession>();

            var job = Job.FromExpression(() => SampleAsyncMethod());
            var ravenJob = new RavenJob
            {
                Id = "RavenJobs/job-async-1",
                InvocationData = InvocationData.SerializeJob(job),
                CreatedAt = DateTime.UtcNow,
                StateData = new StateData { Name = "Succeeded" }
            };

            repositoryMock.Setup(r => r.OpenAsyncSession(It.IsAny<SessionOptions>())).Returns(asyncSessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), "job-async-1")).Returns("RavenJobs/job-async-1");
            asyncSessionMock.Setup(s => s.LoadAsync<RavenJob>("RavenJobs/job-async-1", It.IsAny<CancellationToken>())).ReturnsAsync(ravenJob);

            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            var jobData = await connection.GetJobDataAsync("job-async-1");

            Assert.NotNull(jobData);
            Assert.Equal("Succeeded", jobData.State);
            Assert.NotNull(jobData.Job);
            Assert.Equal(nameof(SampleAsyncMethod), jobData.Job.Method.Name);
        }

        [Fact]
        public async Task SetJobParameterAsync_UpdatesParameterAndSavesAsync()
        {
            var repositoryMock = new Mock<IRepository>();
            var asyncSessionMock = new Mock<IAsyncDocumentSession>();

            var ravenJob = new RavenJob
            {
                Id = "RavenJobs/job-async-2",
                Parameters = new Dictionary<string, string>()
            };

            repositoryMock.Setup(r => r.OpenAsyncSession(It.IsAny<SessionOptions>())).Returns(asyncSessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), "job-async-2")).Returns("RavenJobs/job-async-2");
            asyncSessionMock.Setup(s => s.LoadAsync<RavenJob>("RavenJobs/job-async-2", It.IsAny<CancellationToken>())).ReturnsAsync(ravenJob);

            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            await connection.SetJobParameterAsync("job-async-2", "CustomParam", "CustomValue");

            Assert.Equal("CustomValue", ravenJob.Parameters["CustomParam"]);
            asyncSessionMock.Verify(s => s.SaveChangesAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [Fact]
        public async Task WriteOnlyAsyncTransaction_CommitAsync_Executes()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();

            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            var transaction = await connection.CreateWriteTransactionAsync();
            Assert.NotNull(transaction);

            await transaction.CommitAsync();
            sessionMock.Verify(s => s.SaveChanges(), Times.Once);
        }

        public static void SampleAsyncMethod()
        {
        }
    }
}
