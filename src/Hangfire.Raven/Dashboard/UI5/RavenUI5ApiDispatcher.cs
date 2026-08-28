using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Dashboard;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Hangfire.Raven.Dashboard.UI5
{
    public class RavenUI5ApiDispatcher : IDashboardDispatcher
    {
        private static readonly JsonSerializerSettings JsonSettings = new JsonSerializerSettings
        {
            ContractResolver = new CamelCasePropertyNamesContractResolver(),
            Formatting = Formatting.None,
            NullValueHandling = NullValueHandling.Include
        };

        public async Task Dispatch(DashboardContext context)
        {
            context.Response.ContentType = "application/json";

            if (!(context.Storage is RavenStorage storage))
            {
                await context.Response.WriteAsync(JsonConvert.SerializeObject(new { error = "RavenStorage not available" }, JsonSettings));
                return;
            }

            var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
            if (monitoringApi == null)
            {
                await context.Response.WriteAsync(JsonConvert.SerializeObject(new { error = "RavenStorageMonitoringApi not available" }, JsonSettings));
                return;
            }

            var path = context.Request.Path ?? string.Empty;

            if (path.EndsWith("/overview", StringComparison.OrdinalIgnoreCase))
            {
                StatisticsDto stats;
                RavenStorageMetricsDto ravenMetrics;
                IList<ServerDto> servers;
                IList<QueueWithTopEnqueuedJobsDto> queues;
                IDictionary<DateTime, long> hourlySucceeded = null;
                IDictionary<DateTime, long> hourlyFailed = null;

                try { stats = monitoringApi.GetStatistics(); } catch { stats = new StatisticsDto(); }
                try { ravenMetrics = monitoringApi.GetRavenMetrics(); } catch { ravenMetrics = new RavenStorageMetricsDto(); }
                try { servers = monitoringApi.Servers(); } catch { servers = new List<ServerDto>(); }
                try { queues = monitoringApi.Queues(); } catch { queues = new List<QueueWithTopEnqueuedJobsDto>(); }
                try { hourlySucceeded = monitoringApi.HourlySucceededJobs(); } catch { }
                try { hourlyFailed = monitoringApi.HourlyFailedJobs(); } catch { }

                var overview = new
                {
                    stats = stats ?? new StatisticsDto(),
                    ravendb = ravenMetrics ?? new RavenStorageMetricsDto(),
                    timeline = new
                    {
                        hourlySucceeded = (hourlySucceeded ?? new Dictionary<DateTime, long>())
                            .OrderBy(kv => kv.Key)
                            .Select(kv => new { time = kv.Key.ToString("HH:mm"), count = kv.Value }),
                        hourlyFailed = (hourlyFailed ?? new Dictionary<DateTime, long>())
                            .OrderBy(kv => kv.Key)
                            .Select(kv => new { time = kv.Key.ToString("HH:mm"), count = kv.Value })
                    },
                    servers = (servers ?? new List<ServerDto>()).Select(s => new
                    {
                        name = s.Name,
                        workersCount = s.WorkersCount,
                        queues = s.Queues,
                        startedAt = s.StartedAt,
                        heartbeat = s.Heartbeat
                    }),
                    queues = (queues ?? new List<QueueWithTopEnqueuedJobsDto>()).Select(q => new
                    {
                        name = q.Name,
                        length = q.Length,
                        fetched = q.Fetched
                    }),
                    timestamp = DateTime.UtcNow
                };

                await context.Response.WriteAsync(JsonConvert.SerializeObject(overview, JsonSettings));
                return;
            }

            if (path.EndsWith("/jobs", StringComparison.OrdinalIgnoreCase))
            {
                var state = context.Request.GetQuery("state") ?? "succeeded";
                int.TryParse(context.Request.GetQuery("from") ?? "0", out var from);
                int.TryParse(context.Request.GetQuery("count") ?? "50", out var count);
                count = Math.Min(Math.Max(1, count), 200);

                object jobsResult = null;
                switch (state.ToLowerInvariant())
                {
                    case "succeeded":
                        jobsResult = monitoringApi.SucceededJobs(from, count).Select(j => new
                        {
                            id = j.Key,
                            job = FormatJob(j.Value.Job),
                            result = j.Value.Result,
                            totalDuration = j.Value.TotalDuration,
                            succeededAt = j.Value.SucceededAt,
                            inSucceededState = j.Value.InSucceededState
                        });
                        break;
                    case "failed":
                        jobsResult = monitoringApi.FailedJobs(from, count).Select(j => new
                        {
                            id = j.Key,
                            job = FormatJob(j.Value.Job),
                            reason = j.Value.Reason,
                            failedAt = j.Value.FailedAt,
                            exceptionType = j.Value.ExceptionType,
                            exceptionMessage = j.Value.ExceptionMessage,
                            exceptionDetails = j.Value.ExceptionDetails,
                            inFailedState = j.Value.InFailedState
                        });
                        break;
                    case "processing":
                        jobsResult = monitoringApi.ProcessingJobs(from, count).Select(j => new
                        {
                            id = j.Key,
                            job = FormatJob(j.Value.Job),
                            serverId = j.Value.ServerId,
                            startedAt = j.Value.StartedAt,
                            inProcessingState = j.Value.InProcessingState
                        });
                        break;
                    case "enqueued":
                        var queue = context.Request.GetQuery("queue") ?? "default";
                        jobsResult = monitoringApi.EnqueuedJobs(queue, from, count).Select(j => new
                        {
                            id = j.Key,
                            job = FormatJob(j.Value.Job),
                            enqueuedAt = j.Value.EnqueuedAt,
                            inEnqueuedState = j.Value.InEnqueuedState,
                            state = j.Value.State
                        });
                        break;
                    case "scheduled":
                        jobsResult = monitoringApi.ScheduledJobs(from, count).Select(j => new
                        {
                            id = j.Key,
                            job = FormatJob(j.Value.Job),
                            enqueueAt = j.Value.EnqueueAt,
                            scheduledAt = j.Value.ScheduledAt,
                            inScheduledState = j.Value.InScheduledState
                        });
                        break;
                    case "deleted":
                        jobsResult = monitoringApi.DeletedJobs(from, count).Select(j => new
                        {
                            id = j.Key,
                            job = FormatJob(j.Value.Job),
                            deletedAt = j.Value.DeletedAt,
                            inDeletedState = j.Value.InDeletedState
                        });
                        break;
                }

                await context.Response.WriteAsync(JsonConvert.SerializeObject(new
                {
                    state,
                    from,
                    count,
                    items = jobsResult ?? Array.Empty<object>()
                }, JsonSettings));
                return;
            }

            if (path.EndsWith("/recurring", StringComparison.OrdinalIgnoreCase))
            {
                using var connection = storage.GetConnection();
                var recurring = connection.GetRecurringJobs();
                await context.Response.WriteAsync(JsonConvert.SerializeObject(new
                {
                    items = recurring.Select(r => new
                    {
                        id = r.Id,
                        cron = r.Cron,
                        queue = r.Queue,
                        nextExecution = r.NextExecution,
                        lastExecution = r.LastExecution,
                        lastJobId = r.LastJobId,
                        lastJobState = r.LastJobState,
                        createdAt = r.CreatedAt,
                        timeZoneId = r.TimeZoneId,
                        job = FormatJob(r.Job)
                    })
                }, JsonSettings));
                return;
            }

            if (path.EndsWith("/servers", StringComparison.OrdinalIgnoreCase))
            {
                var servers = monitoringApi.Servers();
                await context.Response.WriteAsync(JsonConvert.SerializeObject(new { items = servers }, JsonSettings));
                return;
            }

            await context.Response.WriteAsync(JsonConvert.SerializeObject(new { status = "ok" }, JsonSettings));
        }

        private static object FormatJob(Common.Job job)
        {
            if (job == null) return null;
            return new
            {
                type = job.Type?.FullName ?? job.Type?.Name,
                method = job.Method?.Name,
                arguments = job.Args != null ? string.Join(", ", job.Args.Select(a => a?.ToString() ?? "null")) : string.Empty
            };
        }
    }
}
