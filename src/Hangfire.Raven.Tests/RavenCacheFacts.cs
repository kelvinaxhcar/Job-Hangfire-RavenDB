using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Microsoft.Extensions.Caching.Memory;
using Moq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenCacheFacts
    {
        [Fact]
        public void RavenStorage_InitializesInternalCache_WhenNotConfigured()
        {
            var repositoryMock = new Mock<IRepository>();
            var options = new RavenStorageOptions { EnableCache = true };

            using var storage = new RavenStorage(repositoryMock.Object, options);

            Assert.NotNull(storage.Cache);
            Assert.IsType<MemoryCache>(storage.Cache);
        }

        [Fact]
        public void RavenStorage_UsesCustomCache_WhenConfigured()
        {
            using var customCache = new MemoryCache(new MemoryCacheOptions());
            var repositoryMock = new Mock<IRepository>();
            var options = new RavenStorageOptions
            {
                EnableCache = true,
                MemoryCache = customCache
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);

            Assert.Same(customCache, storage.Cache);
        }

        [Fact]
        public void RavenStorage_Dispose_DisposesInternalCache()
        {
            var repositoryMock = new Mock<IRepository>();
            var options = new RavenStorageOptions { EnableCache = true };
            var storage = new RavenStorage(repositoryMock.Object, options);
            var cache = storage.Cache;

            storage.Dispose();

            // Calling Set or Get on a disposed MemoryCache throws ObjectDisposedException
            Assert.Throws<ObjectDisposedException>(() => cache.Set("test", "val"));
        }

        [Fact]
        public void RavenConnection_GetCounter_CachesResult_OnSubsequentCalls()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            repositoryMock.Setup(r => r.GetId(typeof(Counter), "test-counter"))
                          .Returns("counters/test-counter");
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>()))
                          .Returns(sessionMock.Object);

            sessionMock.Setup(s => s.Load<Counter>("counters/test-counter"))
                       .Returns(new Counter { Id = "counters/test-counter", Value = 42 });

            var options = new RavenStorageOptions
            {
                EnableCache = true,
                CacheSlidingExpiration = TimeSpan.FromSeconds(5)
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var connection = new RavenConnection(storage);

            var value1 = connection.GetCounter("test-counter");
            var value2 = connection.GetCounter("test-counter");

            Assert.Equal(42, value1);
            Assert.Equal(42, value2);

            // Repository.OpenSession should only have been called once due to caching
            repositoryMock.Verify(r => r.OpenSession(It.IsAny<SessionOptions>()), Times.Once);
        }

        [Fact]
        public void RavenConnection_GetCounter_BypassesCache_WhenEnableCacheIsFalse()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            repositoryMock.Setup(r => r.GetId(typeof(Counter), "test-counter"))
                          .Returns("counters/test-counter");
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>()))
                          .Returns(sessionMock.Object);

            sessionMock.Setup(s => s.Load<Counter>("counters/test-counter"))
                       .Returns(new Counter { Id = "counters/test-counter", Value = 42 });

            var options = new RavenStorageOptions
            {
                EnableCache = false
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var connection = new RavenConnection(storage);

            var value1 = connection.GetCounter("test-counter");
            var value2 = connection.GetCounter("test-counter");

            Assert.Equal(42, value1);
            Assert.Equal(42, value2);

            // Should open session every time
            repositoryMock.Verify(r => r.OpenSession(It.IsAny<SessionOptions>()), Times.Exactly(2));
        }

        [Fact]
        public void RavenConnection_GetJobData_CachesResult_OnSubsequentCalls()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            var invocationData = InvocationData.SerializeJob(Job.FromExpression<DummyJobTarget>(x => x.SampleMethod()));
            var ravenJob = new RavenJob
            {
                Id = "RavenJobs/123",
                InvocationData = invocationData,
                CreatedAt = DateTime.UtcNow,
                StateData = new StateData { Name = "Processing" }
            };

            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), "123"))
                          .Returns("RavenJobs/123");
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>()))
                          .Returns(sessionMock.Object);

            sessionMock.Setup(s => s.Load<RavenJob>("RavenJobs/123"))
                       .Returns(ravenJob);

            var options = new RavenStorageOptions
            {
                EnableCache = true,
                CacheSlidingExpiration = TimeSpan.FromSeconds(5)
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var connection = new RavenConnection(storage);

            var jobData1 = connection.GetJobData("123");
            var jobData2 = connection.GetJobData("123");

            Assert.NotNull(jobData1);
            Assert.NotNull(jobData2);
            Assert.Equal("Processing", jobData1.State);
            Assert.Equal("Processing", jobData2.State);

            repositoryMock.Verify(r => r.OpenSession(It.IsAny<SessionOptions>()), Times.Once);
        }

        [Fact]
        public void RavenConnection_SetJobParameter_InvalidatesJobCache()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            var invocationData = InvocationData.SerializeJob(Job.FromExpression<DummyJobTarget>(x => x.SampleMethod()));
            var ravenJob = new RavenJob
            {
                Id = "RavenJobs/123",
                InvocationData = invocationData,
                CreatedAt = DateTime.UtcNow,
                Parameters = new Dictionary<string, string> { ["Param1"] = "Val1" }
            };

            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), "123"))
                          .Returns("RavenJobs/123");
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>()))
                          .Returns(sessionMock.Object);

            sessionMock.Setup(s => s.Load<RavenJob>("RavenJobs/123"))
                       .Returns(ravenJob);

            var options = new RavenStorageOptions
            {
                EnableCache = true,
                CacheSlidingExpiration = TimeSpan.FromSeconds(5)
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var connection = new RavenConnection(storage);

            // Read parameter into cache
            var paramValue1 = connection.GetJobParameter("123", "Param1");
            Assert.Equal("Val1", paramValue1);

            // Set new parameter value (invalidates cache)
            connection.SetJobParameter("123", "Param1", "Val2");

            // Next read should open session again
            var paramValue2 = connection.GetJobParameter("123", "Param1");
            Assert.Equal("Val2", paramValue2);

            repositoryMock.Verify(r => r.OpenSession(It.IsAny<SessionOptions>()), Times.Exactly(3));
        }

        [Fact]
        public void RavenConnection_SetRangeInHash_InvalidatesHashCache()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            var hash = new RavenHash
            {
                Id = "RavenHashes/myhash",
                Fields = new Dictionary<string, string> { ["Field1"] = "Value1" }
            };

            repositoryMock.Setup(r => r.GetId(typeof(RavenHash), "myhash"))
                          .Returns("RavenHashes/myhash");
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>()))
                          .Returns(sessionMock.Object);

            sessionMock.Setup(s => s.Load<RavenHash>("RavenHashes/myhash"))
                       .Returns(hash);

            var options = new RavenStorageOptions
            {
                EnableCache = true,
                CacheSlidingExpiration = TimeSpan.FromSeconds(5)
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var connection = new RavenConnection(storage);

            // Read hash value into cache
            var val1 = connection.GetValueFromHash("myhash", "Field1");
            Assert.Equal("Value1", val1);

            // Mutate hash
            connection.SetRangeInHash("myhash", new[] { new KeyValuePair<string, string>("Field1", "UpdatedValue") });

            // Next read should open session again
            var val2 = connection.GetValueFromHash("myhash", "Field1");
            Assert.Equal("UpdatedValue", val2);

            repositoryMock.Verify(r => r.OpenSession(It.IsAny<SessionOptions>()), Times.Exactly(3));
        }

        [Fact]
        public void RavenConnection_GetSetCount_CachesResult()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            var set = new RavenSet
            {
                Id = "RavenSets/myset",
                Scores = new Dictionary<string, double> { ["Item1"] = 1.0, ["Item2"] = 2.0 }
            };

            repositoryMock.Setup(r => r.GetId(typeof(RavenSet), "myset"))
                          .Returns("RavenSets/myset");
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>()))
                          .Returns(sessionMock.Object);

            sessionMock.Setup(s => s.Load<RavenSet>("RavenSets/myset"))
                       .Returns(set);

            var options = new RavenStorageOptions
            {
                EnableCache = true,
                CacheSlidingExpiration = TimeSpan.FromSeconds(5)
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var connection = new RavenConnection(storage);

            var count1 = connection.GetSetCount("myset");
            var count2 = connection.GetSetCount("myset");

            Assert.Equal(2, count1);
            Assert.Equal(2, count2);

            repositoryMock.Verify(r => r.OpenSession(It.IsAny<SessionOptions>()), Times.Once);
        }

        [Fact]
        public async Task RavenConnection_GetJobDataAsync_CachesResult()
        {
            var repositoryMock = new Mock<IRepository>();
            var asyncSessionMock = new Mock<IAsyncDocumentSession>();

            var invocationData = InvocationData.SerializeJob(Job.FromExpression<DummyJobTarget>(x => x.SampleMethod()));
            var ravenJob = new RavenJob
            {
                Id = "RavenJobs/async123",
                InvocationData = invocationData,
                CreatedAt = DateTime.UtcNow,
                StateData = new StateData { Name = "Enqueued" }
            };

            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), "async123"))
                          .Returns("RavenJobs/async123");
            repositoryMock.Setup(r => r.OpenAsyncSession(It.IsAny<SessionOptions>()))
                          .Returns(asyncSessionMock.Object);

            asyncSessionMock.Setup(s => s.LoadAsync<RavenJob>("RavenJobs/async123", default))
                            .ReturnsAsync(ravenJob);

            var options = new RavenStorageOptions
            {
                EnableCache = true,
                CacheSlidingExpiration = TimeSpan.FromSeconds(5)
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var connection = new RavenConnection(storage);

            var job1 = await connection.GetJobDataAsync("async123");
            var job2 = await connection.GetJobDataAsync("async123");

            Assert.NotNull(job1);
            Assert.NotNull(job2);
            Assert.Equal("Enqueued", job1.State);
            Assert.Equal("Enqueued", job2.State);

            repositoryMock.Verify(r => r.OpenAsyncSession(It.IsAny<SessionOptions>()), Times.Once);
        }

        [Fact]
        public void RavenStorageMonitoringApi_GetRavenMetrics_CachesResult()
        {
            var repositoryMock = new Mock<IRepository>();
            var stats = new DatabaseStatistics
            {
                DatabaseId = "db-123",
                CountOfDocuments = 100,
                CountOfIndexes = 5
            };

            repositoryMock.Setup(r => r.GetDatabaseStatistics()).Returns(stats);
            repositoryMock.Setup(r => r.DatabaseName).Returns("TestDb");

            var options = new RavenStorageOptions
            {
                EnableCache = true,
                CacheSlidingExpiration = TimeSpan.FromSeconds(5)
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var monitoringApi = new RavenStorageMonitoringApi(storage);

            var metrics1 = monitoringApi.GetRavenMetrics();
            var metrics2 = monitoringApi.GetRavenMetrics();

            Assert.NotNull(metrics1);
            Assert.NotNull(metrics2);
            Assert.Equal("db-123", metrics1.DatabaseId);
            Assert.Equal(100, metrics1.DocumentsCount);

            // Repository.GetDatabaseStatistics should only be called once
            repositoryMock.Verify(r => r.GetDatabaseStatistics(), Times.Once);
        }

        [Fact]
        public void RavenStorageMonitoringApi_GetRavenMetrics_BypassesCache_WhenEnableCacheIsFalse()
        {
            var repositoryMock = new Mock<IRepository>();
            var stats = new DatabaseStatistics
            {
                DatabaseId = "db-123",
                CountOfDocuments = 100,
                CountOfIndexes = 5
            };

            repositoryMock.Setup(r => r.GetDatabaseStatistics()).Returns(stats);
            repositoryMock.Setup(r => r.DatabaseName).Returns("TestDb");

            var options = new RavenStorageOptions
            {
                EnableCache = false
            };

            using var storage = new RavenStorage(repositoryMock.Object, options);
            var monitoringApi = new RavenStorageMonitoringApi(storage);

            var metrics1 = monitoringApi.GetRavenMetrics();
            var metrics2 = monitoringApi.GetRavenMetrics();

            Assert.NotNull(metrics1);
            Assert.NotNull(metrics2);

            repositoryMock.Verify(r => r.GetDatabaseStatistics(), Times.Exactly(2));
        }
    }

    public class DummyJobTarget
    {
        public void SampleMethod()
        {
        }
    }
}
