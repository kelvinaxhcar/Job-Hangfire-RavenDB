using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Hangfire.Dashboard;
using Hangfire.Raven.Dashboard;
using Hangfire.Raven.Dashboard.UI5;
using Hangfire.Raven.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenUI5DashboardFacts
    {
        [Fact]
        public async Task RavenUI5PageDispatcher_ReturnsHtmlPageWithOpenUI5Bootstrap()
        {
            var dispatcher = new RavenUI5PageDispatcher();
            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            var httpContext = new DefaultHttpContext
            {
                RequestServices = services.BuildServiceProvider()
            };
            httpContext.Response.Body = new MemoryStream();

            var storageMock = new Mock<JobStorage>();
            var context = new AspNetCoreDashboardContext(storageMock.Object, new DashboardOptions(), httpContext);

            await dispatcher.Dispatch(context);

            Assert.Equal("text/html; charset=utf-8", httpContext.Response.ContentType);
            httpContext.Response.Body.Seek(0, SeekOrigin.Begin);
            using var reader = new StreamReader(httpContext.Response.Body, Encoding.UTF8);
            var html = await reader.ReadToEndAsync();

            Assert.Contains("sap-ui-bootstrap", html);
            Assert.Contains("sap_horizon", html);
            Assert.Contains("Hangfire Dashboard — SAP Fiori / OpenUI5", html);
        }

        [Fact]
        public async Task RavenUI5ApiDispatcher_OverviewEndpoint_ReturnsJsonOverview()
        {
            var dispatcher = new RavenUI5ApiDispatcher();
            var repositoryMock = new Mock<IRepository>();

            repositoryMock.Setup(r => r.DatabaseName).Returns("HangfireUI5DB");
            repositoryMock.Setup(r => r.GetDatabaseStatistics()).Returns(new DatabaseStatistics
            {
                DatabaseId = "test-db-id",
                CountOfDocuments = 120,
                CountOfIndexes = 3,
                SizeOnDisk = new Size { SizeInBytes = 1048576 }
            });

            var storage = new RavenStorage(repositoryMock.Object);

            var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
            var httpContext = new DefaultHttpContext
            {
                RequestServices = services.BuildServiceProvider()
            };
            httpContext.Request.Path = "/api/ui5/overview";
            httpContext.Response.Body = new MemoryStream();

            var context = new AspNetCoreDashboardContext(storage, new DashboardOptions(), httpContext);

            await dispatcher.Dispatch(context);

            Assert.Equal("application/json", httpContext.Response.ContentType);
            httpContext.Response.Body.Seek(0, SeekOrigin.Begin);
            using var reader = new StreamReader(httpContext.Response.Body, Encoding.UTF8);
            var json = await reader.ReadToEndAsync();

            Assert.Contains("\"stats\"", json);
            Assert.Contains("\"ravendb\"", json);
            Assert.Contains("HangfireUI5DB", json);
        }

        [Fact]
        public void RavenDashboardExtensions_RegistersUI5NavigationItem()
        {
            var config = GlobalConfiguration.Configuration;
            config.UseRavenUI5Dashboard();

            Assert.NotEmpty(NavigationMenu.Items);
            Assert.NotNull(DashboardRoutes.Routes);
        }
    }
}
