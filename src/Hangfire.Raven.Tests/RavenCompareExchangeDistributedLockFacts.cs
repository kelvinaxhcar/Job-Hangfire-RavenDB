using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Raven.DistributedLocks;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Moq;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenCompareExchangeDistributedLockFacts
    {
        [Fact]
        public void Ctor_ThrowsAnException_WhenResourceIsNull()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object);

            var exception = Assert.Throws<ArgumentNullException>(() =>
                new RavenDistributedLock(storage, null, TimeSpan.Zero, new RavenStorageOptions()));

            Assert.Equal("resource", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(() =>
                new RavenDistributedLock(null, "resource1", TimeSpan.Zero, new RavenStorageOptions()));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenOptionsIsNull()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object);

            var exception = Assert.Throws<ArgumentNullException>(() =>
                new RavenDistributedLock(storage, "resource1", TimeSpan.Zero, null));

            Assert.Equal("options", exception.ParamName);
        }

        [Fact]
        public void Ctor_AcquiresLockViaCompareExchange_WhenNotLocked()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();
            var clusterTxMock = new Mock<IClusterTransactionOperations>();

            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(DistributedLock), "resource1")).Returns("DistributedLocks/resource1");
            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            advancedMock.Setup(a => a.ClusterTransaction).Returns(clusterTxMock.Object);

            var options = new RavenStorageOptions();
            var storage = new RavenStorage(repositoryMock.Object, options);

            var createdCompareExchange = new CompareExchangeValue<DistributedLock>("DistributedLocks/resource1", 1, new DistributedLock
            {
                ClientId = options.ClientId,
                Resource = "resource1",
                AcquiredAt = DateTime.UtcNow,
                ExpiresAt = DateTime.UtcNow.Add(options.DistributedLockLifetime)
            });

            // First call (during Acquire) returns null, subsequent calls (during Release) return the created lock
            var callCount = 0;
            clusterTxMock.Setup(c => c.GetCompareExchangeValue<DistributedLock>("DistributedLocks/resource1"))
                         .Returns(() => callCount++ == 0 ? null : createdCompareExchange);

            using (new RavenDistributedLock(storage, "resource1", TimeSpan.FromSeconds(1), options))
            {
                // Verify CreateCompareExchangeValue was called
                clusterTxMock.Verify(c => c.CreateCompareExchangeValue("DistributedLocks/resource1", It.IsAny<DistributedLock>()), Times.Once);
                sessionMock.Verify(s => s.SaveChanges(), Times.Once);
            }

            // On dispose / release, DeleteCompareExchangeValue should be invoked
            clusterTxMock.Verify(c => c.DeleteCompareExchangeValue("DistributedLocks/resource1", 1), Times.Once);
        }

        [Fact]
        public void Ctor_AllowsReentrantLock_WithinSameThread()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();
            var clusterTxMock = new Mock<IClusterTransactionOperations>();

            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(DistributedLock), "reentrant_res")).Returns("DistributedLocks/reentrant_res");
            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            advancedMock.Setup(a => a.ClusterTransaction).Returns(clusterTxMock.Object);

            clusterTxMock.Setup(c => c.GetCompareExchangeValue<DistributedLock>("DistributedLocks/reentrant_res"))
                         .Returns((CompareExchangeValue<DistributedLock>)null);

            var options = new RavenStorageOptions();
            var storage = new RavenStorage(repositoryMock.Object, options);

            using (new RavenDistributedLock(storage, "reentrant_res", TimeSpan.FromSeconds(1), options))
            {
                // Second acquisition on same thread should not invoke CreateCompareExchangeValue again
                using (new RavenDistributedLock(storage, "reentrant_res", TimeSpan.FromSeconds(1), options))
                {
                    clusterTxMock.Verify(c => c.CreateCompareExchangeValue("DistributedLocks/reentrant_res", It.IsAny<DistributedLock>()), Times.Once);
                }
            }
        }

        [Fact]
        public void Ctor_ThrowsDistributedLockTimeoutException_WhenLockedByAnotherClient()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();
            var clusterTxMock = new Mock<IClusterTransactionOperations>();

            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(DistributedLock), "busy_res")).Returns("DistributedLocks/busy_res");
            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            advancedMock.Setup(a => a.ClusterTransaction).Returns(clusterTxMock.Object);

            // Existing active lock belonging to another client (not expired)
            var activeLock = new CompareExchangeValue<DistributedLock>("DistributedLocks/busy_res", 1, new DistributedLock
            {
                ClientId = "another-client",
                Resource = "busy_res",
                AcquiredAt = DateTime.UtcNow,
                ExpiresAt = DateTime.UtcNow.AddMinutes(5)
            });

            clusterTxMock.Setup(c => c.GetCompareExchangeValue<DistributedLock>("DistributedLocks/busy_res"))
                         .Returns(activeLock);

            var options = new RavenStorageOptions();
            var storage = new RavenStorage(repositoryMock.Object, options);

            Assert.Throws<DistributedLockTimeoutException>(() =>
                new RavenDistributedLock(storage, "busy_res", TimeSpan.FromMilliseconds(100), options));
        }

        [Fact]
        public void Ctor_ClaimsLock_WhenExistingLockIsExpired()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();
            var clusterTxMock = new Mock<IClusterTransactionOperations>();

            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(DistributedLock), "expired_res")).Returns("DistributedLocks/expired_res");
            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            advancedMock.Setup(a => a.ClusterTransaction).Returns(clusterTxMock.Object);

            // Existing lock that is expired
            var expiredLock = new CompareExchangeValue<DistributedLock>("DistributedLocks/expired_res", 1, new DistributedLock
            {
                ClientId = "dead-client",
                Resource = "expired_res",
                AcquiredAt = DateTime.UtcNow.AddMinutes(-10),
                ExpiresAt = DateTime.UtcNow.AddMinutes(-2) // Already expired
            });

            clusterTxMock.Setup(c => c.GetCompareExchangeValue<DistributedLock>("DistributedLocks/expired_res"))
                         .Returns(expiredLock);

            var options = new RavenStorageOptions();
            var storage = new RavenStorage(repositoryMock.Object, options);

            using (new RavenDistributedLock(storage, "expired_res", TimeSpan.FromSeconds(1), options))
            {
                Assert.Equal(options.ClientId, expiredLock.Value.ClientId);
                sessionMock.Verify(s => s.SaveChanges(), Times.Once);
            }
        }
    }
}
