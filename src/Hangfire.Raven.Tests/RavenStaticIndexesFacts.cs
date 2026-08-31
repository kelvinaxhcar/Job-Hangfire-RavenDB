using System.Collections.Generic;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.Storage;
using Moq;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenStaticIndexesFacts
    {
        [Fact]
        public void JobQueue_ByQueueAndFetchedAt_HasCorrectNameAndMap()
        {
            var index = new JobQueue_ByQueueAndFetchedAt();
            var definition = index.CreateIndexDefinition();

            Assert.Equal("JobQueue/ByQueueAndFetchedAt", index.IndexName);
            Assert.NotEmpty(definition.Maps);
        }

        [Fact]
        public void RavenJobs_ByStateAndCreatedAt_HasCorrectNameAndMap()
        {
            var index = new RavenJobs_ByStateAndCreatedAt();
            var definition = index.CreateIndexDefinition();

            Assert.Equal("RavenJobs/ByStateAndCreatedAt", index.IndexName);
            Assert.NotEmpty(definition.Maps);
        }

        [Fact]
        public void JobQueue_Stats_HasCorrectNameMapAndReduce()
        {
            var index = new JobQueue_Stats();
            var definition = index.CreateIndexDefinition();

            Assert.Equal("JobQueue/Stats", index.IndexName);
            Assert.NotEmpty(definition.Maps);
            Assert.NotNull(definition.Reduce);
        }

        [Fact]
        public void RavenStorage_Ctor_ExecutesStaticIndexesOnRepository()
        {
            var repositoryMock = new Mock<IRepository>();
            List<AbstractIndexCreationTask> executedIndexes = null;

            repositoryMock.Setup(r => r.ExecuteIndexes(It.IsAny<List<AbstractIndexCreationTask>>()))
                          .Callback<List<AbstractIndexCreationTask>>(indexes => executedIndexes = indexes);

            var storage = new RavenStorage(repositoryMock.Object);

            repositoryMock.Verify(r => r.ExecuteIndexes(It.IsAny<List<AbstractIndexCreationTask>>()), Times.Once);
            Assert.NotNull(executedIndexes);
            Assert.Equal(3, executedIndexes.Count);
            Assert.Contains(executedIndexes, i => i is JobQueue_ByQueueAndFetchedAt);
            Assert.Contains(executedIndexes, i => i is RavenJobs_ByStateAndCreatedAt);
            Assert.Contains(executedIndexes, i => i is JobQueue_Stats);
        }
    }
}
