using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Moq;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenAsyncConnectionFacts : TesteBase
    {
        public RavenAsyncConnectionFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public async Task CreateExpiredJobAsync_StoresAndSavesAsync()
        {
            var repositoryMock = new Mock<IRepository>();
            var asyncSessionMock = new Mock<IAsyncDocumentSession>();

            repositoryMock.Setup(r => r.OpenAsyncSession(It.IsAny<SessionOptions>())).Returns(asyncSessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), It.IsAny<string[]>())).Returns<Type, string[]>((t, ids) => $"RavenJobs/{ids[0]}");

            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            var job = Job.FromExpression(() => SampleAsyncTarget());
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

            var job = Job.FromExpression(() => SampleAsyncTarget());
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
            Assert.Equal(nameof(SampleAsyncTarget), jobData.Job.Method.Name);
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

        [Fact]
        public async Task AsyncMethods_WhenCancellationTokenCancelled_ThrowsOperationCanceledException()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            using var cts = new CancellationTokenSource();
            cts.Cancel();
            var token = cts.Token;

            var sampleJob = Job.FromExpression(() => SampleAsyncTarget());
            var sampleDict = new Dictionary<string, string>();
            var sampleKvp = new List<KeyValuePair<string, string>> { new KeyValuePair<string, string>("k", "v") };

            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.CreateExpiredJobAsync(sampleJob, sampleDict, DateTime.UtcNow, TimeSpan.FromHours(1), token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetJobDataAsync("j1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetStateDataAsync("j1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.SetJobParameterAsync("j1", "p", "v", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetJobParameterAsync("j1", "p", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetAllItemsFromSetAsync("s1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetFirstByLowestScoreFromSetAsync("s1", 0, 10, token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.SetRangeInHashAsync("h1", sampleKvp, token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetAllEntriesFromHashAsync("h1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.AnnounceServerAsync("srv1", new ServerContext(), token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.RemoveServerAsync("srv1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.HeartbeatAsync("srv1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.RemoveTimedOutServersAsync(TimeSpan.FromMinutes(5), token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetSetCountAsync("s1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetRangeFromSetAsync("s1", 0, 10, token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetSetTtlAsync("s1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetCounterAsync("c1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetHashCountAsync("h1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetHashTtlAsync("h1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetValueFromHashAsync("h1", "f", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetListCountAsync("l1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetListTtlAsync("l1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetRangeFromListAsync("l1", 0, 10, token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.GetAllItemsFromListAsync("l1", token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.CreateWriteTransactionAsync(token));
            await Assert.ThrowsAsync<OperationCanceledException>(() => connection.BatchEnqueueAsync(new List<BatchJobItem>(), "default", token));
        }

        [Fact]
        public void AsyncMethods_ReturnIdenticalResultsToSyncMethods()
        {
            UseStorage(storage =>
            {
                using var connection = storage.GetConnection() as RavenConnection;
                Assert.NotNull(connection);

                // 1. Hash operations
                var hashData = new Dictionary<string, string>
                {
                    ["field1"] = "val1",
                    ["field2"] = "val2"
                };
                connection.SetRangeInHash("test-hash-async", hashData);

                var syncHash = connection.GetAllEntriesFromHash("test-hash-async");
                var asyncHash = connection.GetAllEntriesFromHashAsync("test-hash-async").GetAwaiter().GetResult();
                Assert.Equal(syncHash, asyncHash);

                var syncVal = connection.GetValueFromHash("test-hash-async", "field1");
                var asyncVal = connection.GetValueFromHashAsync("test-hash-async", "field1").GetAwaiter().GetResult();
                Assert.Equal(syncVal, asyncVal);

                var syncHashCount = connection.GetHashCount("test-hash-async");
                var asyncHashCount = connection.GetHashCountAsync("test-hash-async").GetAwaiter().GetResult();
                Assert.Equal(syncHashCount, asyncHashCount);

                // 2. Set operations
                using (var tx = connection.CreateWriteTransaction())
                {
                    tx.AddToSet("test-set-async", "item1");
                    tx.AddToSet("test-set-async", "item2");
                    tx.AddToSet("test-set-async", "item3");
                    tx.Commit();
                }

                var syncSet = connection.GetAllItemsFromSet("test-set-async");
                var asyncSet = connection.GetAllItemsFromSetAsync("test-set-async").GetAwaiter().GetResult();
                Assert.Equal(syncSet, asyncSet);

                var syncSetCount = connection.GetSetCount("test-set-async");
                var asyncSetCount = connection.GetSetCountAsync("test-set-async").GetAwaiter().GetResult();
                Assert.Equal(syncSetCount, asyncSetCount);

                // 3. List operations
                using (var tx = connection.CreateWriteTransaction())
                {
                    tx.InsertToList("test-list-async", "list-item-1");
                    tx.InsertToList("test-list-async", "list-item-2");
                    tx.Commit();
                }

                var syncList = connection.GetAllItemsFromList("test-list-async");
                var asyncList = connection.GetAllItemsFromListAsync("test-list-async").GetAwaiter().GetResult();
                Assert.Equal(syncList, asyncList);

                var syncListCount = connection.GetListCount("test-list-async");
                var asyncListCount = connection.GetListCountAsync("test-list-async").GetAwaiter().GetResult();
                Assert.Equal(syncListCount, asyncListCount);

                var syncRange = connection.GetRangeFromList("test-list-async", 0, 1);
                var asyncRange = connection.GetRangeFromListAsync("test-list-async", 0, 1).GetAwaiter().GetResult();
                Assert.Equal(syncRange, asyncRange);
            });
        }

        [Fact]
        public static void SampleAsyncTarget()
        {
        }
    }
}
