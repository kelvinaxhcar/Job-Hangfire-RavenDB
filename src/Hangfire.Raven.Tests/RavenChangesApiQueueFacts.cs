using System;
using System.Threading;
using Hangfire.Raven.Entities;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Moq;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenChangesApiQueueFacts
    {
        [Fact]
        public void RavenStorageOptions_HasChangesApiDefaultEnabled()
        {
            var options = new RavenStorageOptions();
            Assert.True(options.EnableChangesApiQueueEvents);
        }

        [Fact]
        public void RavenJobQueue_InitializesAndDisposesSafely_WhenDocumentStoreNull()
        {
            var repositoryMock = new Mock<IRepository>();
            repositoryMock.Setup(r => r.DocumentStore).Returns((IDocumentStore)null);

            var storage = new RavenStorage(repositoryMock.Object);
            var queue = new RavenJobQueue(storage, new RavenStorageOptions());

            // Should not throw
            queue.Dispose();
        }

        [Fact]
        public void RavenJobQueue_Enqueue_SetsNewItemInQueueEvent()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(JobQueue), "default", "job-1")).Returns("JobQueues/default/job-1");

            var storage = new RavenStorage(repositoryMock.Object);
            using var queue = new RavenJobQueue(storage, new RavenStorageOptions { EnableChangesApiQueueEvents = false });

            // Reset event
            RavenJobQueue.NewItemInQueueEvent.Reset();

            queue.Enqueue("default", "job-1");

            // Event should be signaled
            bool signaled = RavenJobQueue.NewItemInQueueEvent.WaitOne(500);
            Assert.True(signaled);

            sessionMock.Verify(s => s.Store(It.IsAny<JobQueue>()), Times.Once);
            sessionMock.Verify(s => s.SaveChanges(), Times.Once);
        }

        [Fact]
        public void RavenJobQueue_InitializesChangesSubscription_WhenChangesApiAvailable()
        {
            var repositoryMock = new Mock<IRepository>();
            var docStoreMock = new Mock<IDocumentStore>();
            var changesMock = new Mock<IDatabaseChanges>();
            var observableMock = new Mock<IChangesObservable<DocumentChange>>();

            docStoreMock.Setup(d => d.Changes(It.IsAny<string>())).Returns(changesMock.Object);
            changesMock.Setup(c => c.ForDocumentsInCollection<JobQueue>()).Returns(observableMock.Object);
            observableMock.Setup(o => o.Subscribe(It.IsAny<IObserver<DocumentChange>>())).Returns(Mock.Of<IDisposable>());

            repositoryMock.Setup(r => r.DocumentStore).Returns(docStoreMock.Object);
            repositoryMock.Setup(r => r.DatabaseName).Returns("TestDb");

            var storage = new RavenStorage(repositoryMock.Object, new RavenStorageOptions { EnableChangesApiQueueEvents = true });

            changesMock.Verify(c => c.ForDocumentsInCollection<JobQueue>(), Times.AtLeastOnce);
            changesMock.Verify(c => c.EnsureConnectedNow(), Times.AtLeastOnce);
        }
    }
}
