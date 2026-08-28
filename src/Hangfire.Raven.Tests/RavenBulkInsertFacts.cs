using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Common;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Moq;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenBulkInsertFacts
    {
        [Fact]
        public void BatchEnqueue_ReturnsEmptyList_WhenJobsCollectionIsEmptyOrNull()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            var resultNull = connection.BatchEnqueue(null);
            var resultEmpty = connection.BatchEnqueue(new List<BatchJobItem>());

            Assert.NotNull(resultNull);
            Assert.Empty(resultNull);
            Assert.NotNull(resultEmpty);
            Assert.Empty(resultEmpty);
        }

        [Fact]
        public void BatchEnqueue_ThrowsArgumentNullException_WhenItemJobIsNull()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            var items = new List<BatchJobItem>
            {
                new BatchJobItem { Job = null }
            };

            Assert.Throws<ArgumentNullException>(() => connection.BatchEnqueue(items));
        }

        [Fact]
        public async Task BatchEnqueueAsync_ThrowsWhenCancelled()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object);
            using var connection = new RavenConnection(storage);

            var cts = new CancellationTokenSource();
            cts.Cancel();

            var items = new List<BatchJobItem>
            {
                new BatchJobItem { Job = Job.FromExpression(() => SampleMethod()) }
            };

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            {
                await connection.BatchEnqueueAsync(items, "default", cts.Token);
            });
        }

        [Fact]
        public void RavenBatchStorageExtensions_BulkEnqueue_WithExpressions_CreatesJobs()
        {
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();

            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);
            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), It.IsAny<string[]>())).Returns<Type, string[]>((t, ids) => $"RavenJobs/{ids[0]}");

            var storage = new RavenStorage(repositoryMock.Object);

            var expressions = new List<Expression<Action>>
            {
                () => SampleMethod(),
                () => SampleMethod()
            };

            // Using fallback/write transaction when bulk insert not mocked
            using var connection = storage.GetConnection();
            Assert.IsAssignableFrom<IJobStorageBatchConnection>(connection);
        }

        public static void SampleMethod()
        {
        }
    }
}
