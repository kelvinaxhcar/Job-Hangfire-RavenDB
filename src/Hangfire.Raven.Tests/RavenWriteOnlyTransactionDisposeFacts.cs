using System;
using System.Threading.Tasks;
using Hangfire.Raven.Storage;
using Moq;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenWriteOnlyTransactionDisposeFacts
    {
        [Fact]
        public void Dispose_DisposesUnderlyingSession()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();

            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object);
            var transaction = new RavenWriteOnlyTransaction(storage);

            transaction.Dispose();

            sessionMock.Verify(s => s.Dispose(), Times.Once);
        }

        [Fact]
        public void Dispose_CanBeCalledMultipleTimesSafely()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();

            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object);
            var transaction = new RavenWriteOnlyTransaction(storage);

            transaction.Dispose();
            transaction.Dispose();
            transaction.Dispose();

            sessionMock.Verify(s => s.Dispose(), Times.Once);
        }

        [Fact]
        public void UsingBlock_DisposesSessionOnExit()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();

            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object);

            using (var transaction = new RavenWriteOnlyTransaction(storage))
            {
                // inside block
                sessionMock.Verify(s => s.Dispose(), Times.Never);
            }

            // after block exit
            sessionMock.Verify(s => s.Dispose(), Times.Once);
        }

        [Fact]
        public async Task DisposeAsync_DisposesUnderlyingSession()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();

            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object);
            var transaction = new RavenWriteOnlyTransaction(storage);

            await transaction.DisposeAsync();

            sessionMock.Verify(s => s.Dispose(), Times.Once);
        }

        [Fact]
        public async Task AwaitUsingBlock_DisposesSessionOnExit()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();

            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);

            var storage = new RavenStorage(repositoryMock.Object);

            await using (var transaction = new RavenWriteOnlyTransaction(storage))
            {
                // inside block
                sessionMock.Verify(s => s.Dispose(), Times.Never);
            }

            // after block exit
            sessionMock.Verify(s => s.Dispose(), Times.Once);
        }
    }
}
