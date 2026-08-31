using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Moq;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenNoTrackingFacts
    {
        private readonly Mock<IRepository> _repositoryMock;
        private readonly Mock<IDocumentSession> _sessionMock;
        private readonly Mock<IAsyncDocumentSession> _asyncSessionMock;
        private readonly RavenStorage _storage;

        public RavenNoTrackingFacts()
        {
            _repositoryMock = new Mock<IRepository>();
            _sessionMock = new Mock<IDocumentSession>();
            _asyncSessionMock = new Mock<IAsyncDocumentSession>();

            _repositoryMock.Setup(r => r.GetId(It.IsAny<Type>(), It.IsAny<string[]>()))
                           .Returns((Type t, string[] ids) => $"{t.Name}/{string.Join("/", ids)}");

            _repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>()))
                           .Returns(_sessionMock.Object);
            _repositoryMock.Setup(r => r.OpenAsyncSession(It.IsAny<SessionOptions>()))
                           .Returns(_asyncSessionMock.Object);

            var options = new RavenStorageOptions { EnableCache = false };
            _storage = new RavenStorage(_repositoryMock.Object, options);
        }

        [Fact]
        public void GetJobData_UsesSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            connection.GetJobData("123");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public void GetStateData_UsesSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            connection.GetStateData("123");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public void GetSetCount_UsesSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            connection.GetSetCount("test-set");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public void GetAllEntriesFromHash_UsesSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            connection.GetAllEntriesFromHash("test-hash");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public void GetCounter_UsesSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            connection.GetCounter("test-counter");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public void GetListCount_UsesSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            connection.GetListCount("test-list");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public async Task GetJobDataAsync_UsesAsyncSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            await connection.GetJobDataAsync("123");

            _repositoryMock.Verify(r => r.OpenAsyncSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public async Task GetStateDataAsync_UsesAsyncSessionWithNoTracking()
        {
            var connection = new RavenConnection(_storage);

            await connection.GetStateDataAsync("123");

            _repositoryMock.Verify(r => r.OpenAsyncSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public void JobDetails_InMonitoringApi_UsesSessionWithNoTracking()
        {
            var monitoringApi = new RavenStorageMonitoringApi(_storage);

            monitoringApi.JobDetails("123");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o != null && o.NoTracking)), Times.Once);
        }

        [Fact]
        public void SetJobParameter_UsesTrackingSession()
        {
            var connection = new RavenConnection(_storage);
            _sessionMock.Setup(s => s.Load<RavenJob>(It.IsAny<string>()))
                        .Returns(new RavenJob { Parameters = new Dictionary<string, string>() });

            connection.SetJobParameter("123", "param", "val");

            _repositoryMock.Verify(r => r.OpenSession(It.Is<SessionOptions>(o => o == null || !o.NoTracking)), Times.Once);
        }
    }
}
