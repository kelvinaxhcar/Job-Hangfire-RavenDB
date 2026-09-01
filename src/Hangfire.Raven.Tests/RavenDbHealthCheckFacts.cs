using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.HealthChecks;
using Hangfire.Raven.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenDbHealthCheckFacts : TesteBase
    {
        public RavenDbHealthCheckFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public async Task CheckHealthAsync_ReturnsHealthy_WhenRavenDbIsHealthy()
        {
            await Task.Yield();
            UseStorage(storage =>
            {
                var healthCheck = new RavenDbHealthCheck(storage);
                var context = new HealthCheckContext
                {
                    Registration = new HealthCheckRegistration("ravendb", healthCheck, HealthStatus.Unhealthy, null)
                };

                var result = healthCheck.CheckHealthAsync(context).GetAwaiter().GetResult();

                Assert.Equal(HealthStatus.Healthy, result.Status);
                Assert.Equal("RavenDB storage is healthy.", result.Description);
                Assert.NotNull(result.Data);
                Assert.True(result.Data.ContainsKey("Database"));
                Assert.True(result.Data.ContainsKey("DocumentsCount"));
                Assert.True(result.Data.ContainsKey("IndexesCount"));
                Assert.True(result.Data.ContainsKey("StaleIndexesCount"));
            });
        }

        [Fact]
        public async Task CheckHealthAsync_ReturnsUnhealthy_WhenStoreCannotBeResolved()
        {
            var services = new ServiceCollection();
            var sp = services.BuildServiceProvider();

            var healthCheck = new RavenDbHealthCheck(sp);
            var context = new HealthCheckContext
            {
                Registration = new HealthCheckRegistration("ravendb", healthCheck, HealthStatus.Unhealthy, null)
            };

            var result = await healthCheck.CheckHealthAsync(context);

            Assert.Equal(HealthStatus.Unhealthy, result.Status);
            Assert.Contains("could not be resolved", result.Description);
        }

        [Fact]
        public async Task CheckHealthAsync_ReturnsHealthy_UsingDocumentStoreDirectly()
        {
            await Task.Yield();
            var healthCheck = new RavenDbHealthCheck(_store);
            var context = new HealthCheckContext
            {
                Registration = new HealthCheckRegistration("ravendb", healthCheck, HealthStatus.Unhealthy, null)
            };

            var result = await healthCheck.CheckHealthAsync(context);

            Assert.Equal(HealthStatus.Healthy, result.Status);
            Assert.Equal("RavenDB storage is healthy.", result.Description);
        }

        [Fact]
        public async Task AddRavenDb_RegistersAndResolvesViaHealthCheckService()
        {
            await Task.Yield();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IDocumentStore>(_store);

            services.AddHealthChecks()
                .AddRavenDb(name: "custom-ravendb", tags: new[] { "db", "ready" });

            var serviceProvider = services.BuildServiceProvider();
            var healthCheckService = serviceProvider.GetRequiredService<HealthCheckService>();

            var report = await healthCheckService.CheckHealthAsync();

            Assert.Equal(HealthStatus.Healthy, report.Status);
            Assert.True(report.Entries.ContainsKey("custom-ravendb"));
            Assert.Equal(HealthStatus.Healthy, report.Entries["custom-ravendb"].Status);
            Assert.Contains("ready", report.Entries["custom-ravendb"].Tags);
        }

        [Fact]
        public async Task AddRavenDb_WithRavenStorageInstance_ResolvesCorrectly()
        {
            await Task.Yield();
            UseStorage(storage =>
            {
                var services = new ServiceCollection();
                services.AddLogging();
                services.AddHealthChecks()
                    .AddRavenDb(storage, name: "storage-check");

                var serviceProvider = services.BuildServiceProvider();
                var healthCheckService = serviceProvider.GetRequiredService<HealthCheckService>();

                var report = healthCheckService.CheckHealthAsync().GetAwaiter().GetResult();

                Assert.Equal(HealthStatus.Healthy, report.Status);
                Assert.True(report.Entries.ContainsKey("storage-check"));
            });
        }

        [Fact]
        public void AddRavenDb_ThrowsArgumentNullException_WhenParametersAreNull()
        {
            IHealthChecksBuilder builder = null;
            Assert.Throws<ArgumentNullException>(() => builder.AddRavenDb());

            var services = new ServiceCollection();
            var validBuilder = services.AddHealthChecks();

            RavenStorage nullStorage = null;
            Assert.Throws<ArgumentNullException>(() => validBuilder.AddRavenDb(nullStorage));

            IDocumentStore nullStore = null;
            Assert.Throws<ArgumentNullException>(() => validBuilder.AddRavenDb(nullStore));

            IRepository nullRepo = null;
            Assert.Throws<ArgumentNullException>(() => validBuilder.AddRavenDb(nullRepo));

            Func<IServiceProvider, RavenStorage> nullFactory = null;
            Assert.Throws<ArgumentNullException>(() => validBuilder.AddRavenDb(nullFactory));
        }
    }
}
