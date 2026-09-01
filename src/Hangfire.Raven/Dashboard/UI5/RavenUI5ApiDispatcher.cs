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
            try
            {
                context.Response.ContentType = "application/json";

                var storage = (context.Storage as RavenStorage) ?? (JobStorage.Current as RavenStorage);
                if (storage == null)
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
                IList<ServerDto> servers = null;
                try { servers = monitoringApi.Servers(); } catch { servers = new List<ServerDto>(); }
                await context.Response.WriteAsync(JsonConvert.SerializeObject(new
                {
                    items = (servers ?? new List<ServerDto>()).Select(s => new
                    {
                        name = s.Name,
                        workersCount = s.WorkersCount,
                        queues = s.Queues,
                        startedAt = s.StartedAt,
                        heartbeat = s.Heartbeat
                    })
                }, JsonSettings));
                return;
            }

            if (path.EndsWith("/job-revisions", StringComparison.OrdinalIgnoreCase))
            {
                var jobId = context.Request.GetQuery("id");
                var revisions = monitoringApi.GetJobRevisions(jobId);
                await context.Response.WriteAsync(JsonConvert.SerializeObject(new
                {
                    id = jobId,
                    items = revisions
                }, JsonSettings));
                return;
            }

            if (path.EndsWith("/batch-cancel", StringComparison.OrdinalIgnoreCase) ||
                path.EndsWith("/batch/delete", StringComparison.OrdinalIgnoreCase) ||
                path.EndsWith("/batch-delete", StringComparison.OrdinalIgnoreCase))
            {
                var state = context.Request.GetQuery("state");
                var queue = context.Request.GetQuery("queue");
                var idsParam = context.Request.GetQuery("jobs") ?? context.Request.GetQuery("jobIds") ?? context.Request.GetQuery("ids");

                using var connection = storage.GetConnection();
                if (connection is IBatchJobCancellation batchCancellation)
                {
                    long deleted = 0;
                    if (!string.IsNullOrEmpty(state))
                    {
                        deleted = batchCancellation.DeleteByState(state);
                    }
                    else if (!string.IsNullOrEmpty(queue))
                    {
                        deleted = batchCancellation.DeleteByQueue(queue);
                    }
                    else if (!string.IsNullOrEmpty(idsParam))
                    {
                        var ids = idsParam.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                                          .Select(x => x.Trim());
                        deleted = batchCancellation.DeleteJobs(ids);
                    }

                    await context.Response.WriteAsync(JsonConvert.SerializeObject(new
                    {
                        status = "ok",
                        deletedCount = deleted
                    }, JsonSettings));
                    return;
                }
            }

            await context.Response.WriteAsync(JsonConvert.SerializeObject(new { status = "ok" }, JsonSettings));
            }
            catch (Exception ex)
            {
                await context.Response.WriteAsync(JsonConvert.SerializeObject(new { error = ex.Message, stackTrace = ex.StackTrace }, JsonSettings));
            }
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
