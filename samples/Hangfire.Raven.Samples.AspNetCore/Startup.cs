using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.Raven;
using Hangfire.Raven.Dashboard;
using Hangfire.Raven.Diagnostics;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raven.Samples.AspNetCore
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllersWithViews();

            var ravenUrls = new[]
            {
                Configuration["ConnectionStrings:RavenDebugUrl"] ?? "http://localhost:9090"
            };
            var databaseName = Configuration["ConnectionStrings:RavenDebugDatabase"] ?? "HangfireAspNetCore";

            // 1. Configure Hangfire with RavenDB Storage using all modern features
            services.AddHangfire(config =>
            {
                config.SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
                      .UseSimpleAssemblyNameTypeSerializer()
                      .UseRecommendedSerializerSettings()
                      .UseRavenStorage(ravenUrls, databaseName, new RavenStorageOptions
                      {
                          // Performance: In-memory sliding cache for high-frequency reads
                          EnableCache = true,
                          CacheSlidingExpiration = TimeSpan.FromSeconds(5),

                          // Resilience: Polly retry policy with exponential backoff and jitter
                          EnableRetryPolicy = true,
                          MaxRetryAttempts = 3,
                          RetryInitialDelay = TimeSpan.FromMilliseconds(100),
                          RetryMaxDelay = TimeSpan.FromSeconds(2),

                          // Compliance & Audit: Document revisions for complete state transition history
                          EnableJobRevisions = true,
                          PurgeJobRevisionsOnDelete = false,
                          MinimumJobRevisionsToKeep = 50,

                          // Cluster & Lock tuning
                          QueuePollInterval = TimeSpan.FromSeconds(1),
                          InvisibilityTimeout = TimeSpan.FromMinutes(15),
                          DistributedLockLifetime = TimeSpan.FromMinutes(1)
                      })
                      .UseRavenDashboard(); // Activates RavenDB Metrics and OpenUI5 / SAP Fiori Dashboard
            });

            // 2. Add Hangfire Server with multi-queue processing
            services.AddHangfireServer(options =>
            {
                options.WorkerCount = Math.Max(Environment.ProcessorCount * 2, 4);
                options.Queues = new[] { "critical", "default", "low" };
                options.ServerName = "SampleServer:Worker";
            });

            // 3. Health Checks: Monitor RavenDB connectivity, database presence, and index health
            services.AddHealthChecks()
                .AddRavenDb(name: "ravendb", configureOptions: options =>
                {
                    options.CheckStaleIndexes = true;
                    options.MaxAllowedStaleIndexes = 0;
                }, tags: new[] { "db", "ready", "hangfire" });
        }

        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, ILogger<Startup> logger)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseStaticFiles();
            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                // 4. Hangfire Dashboard Endpoints:
                // - Classic Dashboard: http://localhost:5005/hangfire
                // - RavenDB Metrics:   http://localhost:5005/hangfire/ravendb
                // - SAP Fiori UI5:     http://localhost:5005/hangfire/ui5
                endpoints.MapHangfireDashboard("/hangfire", new DashboardOptions
                {
                    DashboardTitle = "Hangfire RavenDB Enterprise Dashboard"
                });

                // 5. Prometheus Metrics Endpoint: GET /metrics and /hangfire/metrics
                endpoints.MapRavenMetrics("/metrics");
                endpoints.MapRavenMetrics("/hangfire/metrics");

                // 6. Health Checks Endpoint: GET /healthz
                endpoints.MapHealthChecks("/healthz");

                endpoints.MapControllerRoute(
                    name: "default",
                    pattern: "{controller=Home}/{action=Index}/{id?}");
            });

            // Seed Sample Background Jobs showcasing all functionalities:
            EnqueueSampleJobs(logger);
        }

        private void EnqueueSampleJobs(ILogger<Startup> logger)
        {
            // 1. Fire-and-Forget Job on 'critical' queue
            BackgroundJob.Enqueue(() => Console.WriteLine("[Hangfire Sample] => Fire-and-Forget job executed on default queue!"));

            // 2. Delayed / Scheduled Job
            BackgroundJob.Schedule(() => Console.WriteLine("[Hangfire Sample] => Delayed job executed after 10 seconds!"), TimeSpan.FromSeconds(10));

            // 3. Recurring Job with Cron expression
            RecurringJob.AddOrUpdate("sample-heartbeat-job", () => HeartbeatJob(), "*/1 * * * *");

            // 4. Multiple Job states (Success, Processing, and Retried)
            BackgroundJob.Enqueue(() => ProcessSampleOrder(101, "SAP-Enterprise-Order"));

            logger.LogInformation("===================================================================");
            logger.LogInformation(" Hangfire RavenDB Sample Application Started successfully!");
            logger.LogInformation(" - Classic Dashboard:       http://localhost:5005/hangfire");
            logger.LogInformation(" - RavenDB Metrics:         http://localhost:5005/hangfire/ravendb");
            logger.LogInformation(" - SAP Fiori UI5 Dashboard: http://localhost:5005/hangfire/ui5");
            logger.LogInformation(" - Prometheus Metrics:      http://localhost:5005/hangfire/metrics");
            logger.LogInformation(" - Health Checks:           http://localhost:5005/healthz");
            logger.LogInformation("===================================================================");
        }

        [AutomaticRetry(Attempts = 3, LogEvents = true)]
        public static void HeartbeatJob()
        {
            Console.WriteLine($"[Heartbeat Cron] Execution timestamp: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        }

        public static void ProcessSampleOrder(int orderId, string orderName)
        {
            Console.WriteLine($"[Order Processing] Processing Order #{orderId} ({orderName})...");
            Thread.Sleep(500);
            Console.WriteLine($"[Order Processing] Order #{orderId} completed successfully.");
        }

        public static void Test()
        {
            Console.WriteLine("[Legacy Test Cron Executed]");
        }
    }
}
