using System;
using System.Threading.Tasks;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Moq;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenServerHeartbeatFacts
    {
        [Fact]
        public void Heartbeat_ThrowsException_WhenServerIdIsNull()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object, new RavenStorageOptions());
            var connection = new RavenConnection(storage);

            Assert.Throws<ArgumentNullException>("serverId", () => connection.Heartbeat(null));
        }

        [Fact]
        public void Heartbeat_DoesNotCreateGhostServerOrSaveChanges_WhenServerNotFound()
        {
            var serverId = "ghost-server-123";
            var serverDocId = "ravenservers/ghost-server-123";

            var sessionMock = new Mock<IDocumentSession>();
            sessionMock.Setup(s => s.Load<RavenServer>(serverDocId)).Returns((RavenServer)null);

            var repositoryMock = new Mock<IRepository>();
            repositoryMock.Setup(r => r.GetId(typeof(RavenServer), serverId)).Returns(serverDocId);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object, new RavenStorageOptions());
            var connection = new RavenConnection(storage);

            connection.Heartbeat(serverId);

            // Must NOT store a new empty server
            sessionMock.Verify(s => s.Store(It.IsAny<RavenServer>()), Times.Never);
            // Must NOT call SaveChanges
            sessionMock.Verify(s => s.SaveChanges(), Times.Never);
        }

        [Fact]
        public void Heartbeat_UpdatesLastHeartbeatAndSavesChanges_WhenServerExists()
        {
            var serverId = "active-server-456";
            var serverDocId = "ravenservers/active-server-456";

            var existingServer = new RavenServer
            {
                Id = serverDocId,
                LastHeartbeat = DateTime.UtcNow.AddMinutes(-5),
                Data = new RavenServer.ServerData
                {
                    WorkerCount = 10,
                    Queues = new[] { "default" },
                    StartedAt = DateTime.UtcNow.AddHours(-1)
                }
            };

            var sessionMock = new Mock<IDocumentSession>();
            sessionMock.Setup(s => s.Load<RavenServer>(serverDocId)).Returns(existingServer);

            var repositoryMock = new Mock<IRepository>();
            repositoryMock.Setup(r => r.GetId(typeof(RavenServer), serverId)).Returns(serverDocId);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object, new RavenStorageOptions());
            var connection = new RavenConnection(storage);

            var beforeHeartbeat = DateTime.UtcNow;
            connection.Heartbeat(serverId);
            var afterHeartbeat = DateTime.UtcNow;

            Assert.InRange(existingServer.LastHeartbeat, beforeHeartbeat.AddSeconds(-1), afterHeartbeat.AddSeconds(1));
            // Preserve original server metadata
            Assert.Equal(10, existingServer.Data.WorkerCount);
            Assert.Contains("default", existingServer.Data.Queues);

            // Store must not be called (entity was loaded in change tracking)
            sessionMock.Verify(s => s.Store(It.IsAny<RavenServer>()), Times.Never);
            // SaveChanges must be called once
            sessionMock.Verify(s => s.SaveChanges(), Times.Once);
        }

        [Fact]
        public async Task HeartbeatAsync_DelegatesDefensively_WhenServerNotFound()
        {
            var serverId = "ghost-server-async";
            var serverDocId = "ravenservers/ghost-server-async";

            var sessionMock = new Mock<IDocumentSession>();
            sessionMock.Setup(s => s.Load<RavenServer>(serverDocId)).Returns((RavenServer)null);

            var repositoryMock = new Mock<IRepository>();
            repositoryMock.Setup(r => r.GetId(typeof(RavenServer), serverId)).Returns(serverDocId);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object, new RavenStorageOptions());
            var connection = new RavenConnection(storage);

            await connection.HeartbeatAsync(serverId);

            sessionMock.Verify(s => s.Store(It.IsAny<RavenServer>()), Times.Never);
            sessionMock.Verify(s => s.SaveChanges(), Times.Never);
        }
    }
}
