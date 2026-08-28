using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Hangfire.Dashboard;
using Hangfire.Raven.Dashboard;
using Hangfire.Raven.Dashboard.UI5;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Raven.Client.Documents.Session;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenJobRevisionsFacts
    {
        [Fact]
        public void RavenStorageOptions_HasRevisionsDefaultsEnabled()
        {
            var options = new RavenStorageOptions();

            Assert.True(options.EnableJobRevisions);
            Assert.Equal(50, options.MinimumJobRevisionsToKeep);
            Assert.Equal(TimeSpan.FromDays(14), options.MinimumJobRevisionAgeToKeep);
            Assert.False(options.PurgeJobRevisionsOnDelete);
        }

        [Fact]
        public void RavenStorage_Constructor_CallsEnsureRevisionsConfigured()
        {
            var repositoryMock = new Mock<IRepository>();
            var options = new RavenStorageOptions { EnableJobRevisions = true };

            var storage = new RavenStorage(repositoryMock.Object, options);

            repositoryMock.Verify(r => r.EnsureRevisionsConfigured(options), Times.Once);
        }

        [Fact]
        public void GetJobRevisions_ReturnsEmptyList_WhenJobIdIsNullOrEmpty()
        {
            var repositoryMock = new Mock<IRepository>();
            var storage = new RavenStorage(repositoryMock.Object);
            var monitoringApi = new RavenStorageMonitoringApi(storage);

            var result = monitoringApi.GetJobRevisions(null);

            Assert.NotNull(result);
            Assert.Empty(result);
        }

        [Fact]
        public async Task RavenUI5ApiDispatcher_JobRevisionsEndpoint_ReturnsJsonRevisions()
        {
            var dispatcher = new RavenUI5ApiDispatcher();
            var repositoryMock = new Mock<IRepository>();
            var sessionMock = new Mock<IDocumentSession>();
            var advancedMock = new Mock<IAdvancedSessionOperations>();
            var revisionsMock = new Mock<IRevisionsSessionOperations>();

            repositoryMock.Setup(r => r.GetId(typeof(RavenJob), "job-100")).Returns("RavenJobs/job-100");
            repositoryMock.Setup(r => r.OpenSession(It.IsAny<SessionOptions>())).Returns(sessionMock.Object);
            sessionMock.Setup(s => s.Advanced).Returns(advancedMock.Object);
            advancedMock.Setup(a => a.Revisions).Returns(revisionsMock.Object);

            var revisionsList = new List<RavenJob>
            {
                new RavenJob
                {
                    Id = "RavenJobs/job-100",
                    CreatedAt = DateTime.UtcNow.AddMinutes(-5),
                    StateData = new Hangfire.Storage.StateData { Name = "Enqueued" }
                },
                new RavenJob
                {
                    Id = "RavenJobs/job-100",
                    CreatedAt = DateTime.UtcNow.AddMinutes(-4),
                    StateData = new Hangfire.Storage.StateData { Name = "Processing" }
                },
                new RavenJob
                {
                    Id = "RavenJobs/job-100",
                    CreatedAt = DateTime.UtcNow.AddMinutes(-1),
                    StateData = new Hangfire.Storage.StateData { Name = "Succeeded" }
                }
            };

            revisionsMock.Setup(r => r.GetFor<RavenJob>("RavenJobs/job-100", 0, 25)).Returns(revisionsList);
            revisionsMock.Setup(r => r.GetMetadataFor("RavenJobs/job-100", 0, 25)).Returns(new List<IMetadataDictionary>());

            var storage = new RavenStorage(repositoryMock.Object);

            var services = new ServiceCollection();
            var httpContext = new DefaultHttpContext
            {
                RequestServices = services.BuildServiceProvider()
            };
            httpContext.Request.Path = "/api/ui5/job-revisions";
            httpContext.Request.QueryString = new QueryString("?id=job-100");
            httpContext.Response.Body = new MemoryStream();

            var context = new AspNetCoreDashboardContext(storage, new DashboardOptions(), httpContext);

            await dispatcher.Dispatch(context);

            Assert.Equal("application/json", httpContext.Response.ContentType);
            httpContext.Response.Body.Seek(0, SeekOrigin.Begin);
            using var reader = new StreamReader(httpContext.Response.Body, Encoding.UTF8);
            var json = await reader.ReadToEndAsync();

            Assert.Contains("\"id\":\"job-100\"", json);
            Assert.Contains("\"items\":", json);
            Assert.Contains("Enqueued", json);
            Assert.Contains("Processing", json);
            Assert.Contains("Succeeded", json);
        }
    }
}
