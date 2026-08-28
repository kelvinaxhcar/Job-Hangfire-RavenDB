using System;
using System.Collections.Generic;
using Hangfire.Dashboard;
using Hangfire.Raven.Dashboard;
using Hangfire.Raven.Storage;
using Moq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenDashboardMetricsFacts
    {
        [Fact]
        public void GetRavenMetrics_ReturnsExpectedDto_WhenStatsExist()
        {
            var repositoryMock = new Mock<IRepository>();
            repositoryMock.Setup(r => r.DatabaseName).Returns("HangfireTestDB");

            var dbStats = new DatabaseStatistics
            {
                DatabaseId = "guid-123",
                CountOfDocuments = 450,
                CountOfIndexes = 2,
                SizeOnDisk = new Size { SizeInBytes = 16148070 },
                Indexes = new[]
                {
                    new IndexInformation
                    {
                        Name = "JobQueue/ByQueueAndFetchedAt",
                        IsStale = false,
                        State = IndexState.Normal,
                        Type = IndexType.Map
                    },
                    new IndexInformation
                    {
                        Name = "StaleIndex1",
                        IsStale = true,
                        State = IndexState.Normal,
                        Type = IndexType.Map
                    }
                }
            };

            repositoryMock.Setup(r => r.GetDatabaseStatistics()).Returns(dbStats);

            var storage = new RavenStorage(repositoryMock.Object);
            var monitoringApi = new RavenStorageMonitoringApi(storage);

            var metrics = monitoringApi.GetRavenMetrics();

            Assert.NotNull(metrics);
            Assert.Equal("HangfireTestDB", metrics.DatabaseName);
            Assert.Equal("guid-123", metrics.DatabaseId);
            Assert.Equal(450, metrics.DocumentsCount);
            Assert.Equal(2, metrics.IndexesCount);
            Assert.Equal(1, metrics.StaleIndexesCount);
            Assert.NotNull(metrics.SizeOnDisk);
            Assert.NotEmpty(metrics.SizeOnDisk);
            Assert.Equal(2, metrics.Indexes.Count);
            Assert.False(metrics.Indexes[0].IsStale);
            Assert.True(metrics.Indexes[1].IsStale);
        }

        [Fact]
        public void GetRavenMetrics_ReturnsFallbackDto_WhenStatsNull()
        {
            var repositoryMock = new Mock<IRepository>();
            repositoryMock.Setup(r => r.DatabaseName).Returns("EmptyDB");
            repositoryMock.Setup(r => r.GetDatabaseStatistics()).Returns((DatabaseStatistics)null);

            var storage = new RavenStorage(repositoryMock.Object);
            var monitoringApi = new RavenStorageMonitoringApi(storage);

            var metrics = monitoringApi.GetRavenMetrics();

            Assert.NotNull(metrics);
            Assert.Equal("EmptyDB", metrics.DatabaseName);
            Assert.Equal(0, metrics.DocumentsCount);
            Assert.Equal(0, metrics.IndexesCount);
        }

        [Fact]
        public void RavenDashboardExtensions_UseRavenDashboard_RegistersRouteAndMenuItem()
        {
            var config = GlobalConfiguration.Configuration;

            config.UseRavenDashboard();

            // Calling it twice should be idempotent
            config.UseRavenDashboard();

            Assert.NotEmpty(NavigationMenu.Items);
            Assert.NotNull(DashboardRoutes.Routes);
        }
    }
}
