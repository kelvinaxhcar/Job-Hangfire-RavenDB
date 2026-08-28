using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Raven.Dashboard;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.JobQueues;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Raven.Storage
{
    public class RavenStorageMonitoringApi : IMonitoringApi
    {
        private readonly RavenStorage _storage;
        private const int DefaultBatchSize = 1000;

        public RavenStorageMonitoringApi([NotNull] RavenStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counts = queueApi.GetEnqueuedAndFetchedCount(queue);
            return counts.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counts = queueApi.GetEnqueuedAndFetchedCount(queue);
            return counts.FetchedCount ?? 0;
        }

        public long DeletedListCount() => GetNumberOfJobsByStateName(DeletedState.StateName);
        public long FailedCount() => GetNumberOfJobsByStateName(FailedState.StateName);
        public long ProcessingCount() => GetNumberOfJobsByStateName(ProcessingState.StateName);
        public long ScheduledCount() => GetNumberOfJobsByStateName(ScheduledState.StateName);
        public long SucceededListCount() => GetNumberOfJobsByStateName(SucceededState.StateName);

        private long GetNumberOfJobsByStateName(string stateName)
        {
            using var session = _storage.Repository.OpenSession();
            return session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>()
                         .Count(x => x.StateData.Name == stateName);
        }

        public IDictionary<DateTime, long> FailedByDatesCount() => GetTimelineStats("failed");
        public IDictionary<DateTime, long> SucceededByDatesCount() => GetTimelineStats("succeeded");
        public IDictionary<DateTime, long> HourlyFailedJobs() => GetHourlyTimelineStats("failed");
        public IDictionary<DateTime, long> HourlySucceededJobs() => GetHourlyTimelineStats("succeeded");

        private Dictionary<DateTime, long> GetHourlyTimelineStats(string type)
        {
            var dates = Enumerable.Range(0, 24)
                                .Select(i => DateTime.UtcNow.AddHours(-i))
                                .ToList();

            return GetTimelineStats(
                dates,
                x => $"stats:{type}:{x:yyyy-MM-dd-HH}");
        }

        private Dictionary<DateTime, long> GetTimelineStats(string type)
        {
            var dates = Enumerable.Range(0, 7)
                                .Select(i => DateTime.UtcNow.Date.AddDays(-i))
                                .ToList();

            return GetTimelineStats(
                dates,
                x => $"stats:{type}:{x:yyyy-MM-dd}");
        }

        private Dictionary<DateTime, long> GetTimelineStats(
            List<DateTime> dates,
            Func<DateTime, string> formatAction)
        {
            using var session = _storage.Repository.OpenSession();
            var ids = dates.Select(d => _storage.Repository.GetId(typeof(Counter), formatAction(d))).ToList();
            var counters = session.Load<Counter>(ids);
            var result = new Dictionary<DateTime, long>();

            for (int i = 0; i < dates.Count; i++)
            {
                var id = ids[i];
                var counter = counters.TryGetValue(id, out var c) ? c : null;
                result[dates[i]] = counter?.Value ?? 0;
            }

            return result;
        }

        public StatisticsDto GetStatistics()
        {
            using var session = _storage.Repository.OpenSession();

            var serverLazy = session.Query<RavenServer>()
                   .Statistics(out var serverStats)
                   .Take(0)
                   .Lazily();

            var recurringJobsSetLazy = session.Advanced.Lazily.Load<RavenSet>(
                _storage.Repository.GetId(typeof(RavenSet), "recurring-jobs"));

            var succeededLazy = session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>().Statistics(out var succeededStats).Where(x => x.StateData.Name == SucceededState.StateName).Take(0).Lazily();
            var scheduledLazy = session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>().Statistics(out var scheduledStats).Where(x => x.StateData.Name == ScheduledState.StateName).Take(0).Lazily();
            var enqueuedLazy = session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>().Statistics(out var enqueuedStats).Where(x => x.StateData.Name == EnqueuedState.StateName).Take(0).Lazily();
            var failedLazy = session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>().Statistics(out var failedStats).Where(x => x.StateData.Name == FailedState.StateName).Take(0).Lazily();
            var processingLazy = session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>().Statistics(out var processingStats).Where(x => x.StateData.Name == ProcessingState.StateName).Take(0).Lazily();
            var deletedLazy = session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>().Statistics(out var deletedStats).Where(x => x.StateData.Name == DeletedState.StateName).Take(0).Lazily();
            var queueCountLazy = session.Query<JobQueue, JobQueue_ByQueueAndFetchedAt>().Statistics(out var queueStats).Take(0).Lazily();

            _ = serverLazy.Value; // Triggers batch execution of all lazy queries

            return new StatisticsDto
            {
                Servers = serverStats.TotalResults,
                Queues = queueStats.TotalResults,
                Recurring = recurringJobsSetLazy.Value?.Scores?.Count ?? 0,
                Succeeded = succeededStats.TotalResults,
                Scheduled = scheduledStats.TotalResults,
                Enqueued = enqueuedStats.TotalResults,
                Failed = failedStats.TotalResults,
                Processing = processingStats.TotalResults,
                Deleted = deletedStats.TotalResults
            };
        }

        public RavenStorageMetricsDto GetRavenMetrics()
        {
            var stats = _storage.Repository.GetDatabaseStatistics();
            if (stats == null)
            {
                return new RavenStorageMetricsDto
                {
                    DatabaseName = _storage.Repository.DatabaseName ?? "Unknown"
                };
            }

            var staleCount = stats.StaleIndexes?.Length ?? (stats.Indexes?.Count(i => i.IsStale) ?? 0);

            var dto = new RavenStorageMetricsDto
            {
                DatabaseName = _storage.Repository.DatabaseName,
                DatabaseId = stats.DatabaseId,
                DocumentsCount = stats.CountOfDocuments,
                IndexesCount = stats.CountOfIndexes,
                StaleIndexesCount = staleCount,
                StaleIndexes = stats.StaleIndexes,
                SizeOnDisk = stats.SizeOnDisk?.HumaneSize ?? "N/A"
            };

            if (stats.Indexes != null)
            {
                foreach (var idx in stats.Indexes)
                {
                    dto.Indexes.Add(new RavenIndexMetricsDto
                    {
                        Name = idx.Name,
                        IsStale = idx.IsStale,
                        State = idx.State.ToString(),
                        Type = idx.Type.ToString()
                    });
                }
            }

            return dto;
        }

        public JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return GetJobs(from, count, DeletedState.StateName, (job, deserializedJob, stateData) =>
                new DeletedJobDto
                {
                    Job = deserializedJob,
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x=> x.Key == "DeletedAt").Value)
                });
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int from, int perPage)
        {
            var jobIds = GetQueueApi(queue).GetEnqueuedJobIds(queue, from, perPage);
            return GetJobsById<EnqueuedJobDto>(jobIds, CreateEnqueuedJobDto);
        }


        private EnqueuedJobDto CreateEnqueuedJobDto(RavenJob job, Job deserializedJob, Dictionary<string, string> stateData)
        {
            return new EnqueuedJobDto
            {
                Job = deserializedJob,
                State = job.StateData?.Name,
                EnqueuedAt = job.StateData?.Name == EnqueuedState.StateName
                    ? JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "EnqueuedAt").Value)
                    : null
            };
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int from, int perPage)
        {
            var jobIds = GetQueueApi(queue).GetFetchedJobIds(queue, from, perPage);
            return GetJobsById<FetchedJobDto>(jobIds, CreateFetchedJobDto);
        }

        private FetchedJobDto CreateFetchedJobDto(RavenJob job, Job deserializedJob, Dictionary<string, string> stateData)
        {
            return new FetchedJobDto
            {
                Job = deserializedJob,
                State = job.StateData?.Name,
                FetchedAt = job.StateData?.Name == ProcessingState.StateName
                    ? JobHelper.DeserializeNullableDateTime(stateData.FirstOrDefault(x => x.Key == "StartedAt").Value)
                    : null
            };
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            using var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
            var job = session.Load<RavenJob>(id);

            if (job == null) return null;

            return new JobDetailsDto
            {
                CreatedAt = job.CreatedAt,
                ExpireAt = session.GetExpiry<RavenJob>(job),
                Job = DeserializeJob(job.InvocationData),
                History = job.History,
                Properties = job.Parameters
            };
        }

        private Job DeserializeJob(InvocationData invocationData)
        {
            try
            {
                return invocationData.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            using var session = _storage.Repository.OpenSession();

            var queueGroups = session.Query<JobQueue>()
                                   .ToList()
                                   .GroupBy(x => x.Queue)
                                   .Select(g => new QueueWithTopEnqueuedJobsDto
                                   {
                                       Name = g.Key,
                                       Length = g.Count(x => !x.FetchedAt.HasValue),
                                       Fetched = g.Count(x => x.FetchedAt.HasValue),
                                       FirstJobs = GetJobsById<EnqueuedJobDto>(
                                           g.Take(5).Select(x => x.JobId),
                                           CreateEnqueuedJobDto)
                                   })
                                   .ToList();

            return queueGroups;
        }

        public IList<ServerDto> Servers()
        {
            using var session = _storage.Repository.OpenSession();

            return session.Query<RavenServer>()
                         .ToList()
                         .Select(server => new ServerDto
                         {
                             Name = server.Id != null && server.Id.Contains("/") ? server.Id.Split(new[] { '/' }, 2)[1] : (server.Id ?? "Server"),
                             Heartbeat = server.LastHeartbeat,
                             Queues = server.Data?.Queues?.ToList() ?? new List<string>(),
                             StartedAt = server.Data?.StartedAt ?? DateTime.MinValue,
                             WorkersCount = server.Data?.WorkerCount ?? 0
                         })
                         .ToList();
        }

        private JobList<T> GetJobs<T>(
            int from,
            int count,
            string stateName,
            Func<RavenJob, Job, Dictionary<string, string>, T> selector)
        {
            using var session = _storage.Repository.OpenSession();

            var jobs = session.Query<RavenJob, RavenJobs_ByStateAndCreatedAt>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.StateData.Name == stateName)
                            .OrderByDescending(x => x.CreatedAt)
                            .Skip(from)
                            .Take(count)
                            .ToList();

            return new JobList<T>(jobs.Select(job =>
            {
                var stateData = job.StateData?.Data != null
                    ? new Dictionary<string, string>(job.StateData.Data, StringComparer.OrdinalIgnoreCase)
                    : null;
                var dto = selector(job, DeserializeJob(job.InvocationData), stateData);
                return new KeyValuePair<string, T>(job.Id.Split('/')[1], dto);
            }));
        }

        private JobList<T> GetJobsById<T>(
            IEnumerable<string> jobIds,
            Func<RavenJob, Job, Dictionary<string, string>, T> selector)
        {
            using var session = _storage.Repository.OpenSession();

            var jobs = session.Load<RavenJob>(
                jobIds.Select(id => _storage.Repository.GetId(typeof(RavenJob), id)))
                .Where(kvp => kvp.Value != null)
                .Select(kvp => kvp.Value)
                .ToList();

            return new JobList<T>(jobs.Select(job =>
            {
                var stateData = job.StateData?.Data != null
                    ? new Dictionary<string, string>(job.StateData.Data, StringComparer.OrdinalIgnoreCase)
                    : null;
                var dto = selector(job, DeserializeJob(job.InvocationData), stateData);
                return new KeyValuePair<string, T>(job.Id.Split('/')[1], dto);
            }));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return GetJobs<ProcessingJobDto>(from, count, ProcessingState.StateName, (jsonJob, job, stateData) => new ProcessingJobDto
            {
                Job = job,
                ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                StartedAt = new DateTime?(JobHelper.DeserializeDateTime(stateData["StartedAt"]))
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return GetJobs<ScheduledJobDto>(from, count, ScheduledState.StateName, (jsonJob, job, stateData) => new ScheduledJobDto
            {
                Job = job,
                EnqueueAt = JobHelper.DeserializeDateTime(stateData["EnqueueAt"]),
                ScheduledAt = new DateTime?(JobHelper.DeserializeDateTime(stateData["ScheduledAt"]))
            });
        }

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return GetJobs<SucceededJobDto>(from, count, SucceededState.StateName, (jsonJob, job, stateData) => new SucceededJobDto
            {
                Job = job,
                InSucceededState = true,
                Result = stateData.ContainsKey("Result") ? (object)stateData["Result"] : (object)(string)null,
                TotalDuration = !stateData.ContainsKey("PerformanceDuration") || !stateData.ContainsKey("Latency")
                    ? new long?()
                    : new long?(long.Parse(stateData["PerformanceDuration"]) + long.Parse(stateData["Latency"])),
                SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return GetJobs<FailedJobDto>(from, count, FailedState.StateName, (jsonJob, job, stateData) => new FailedJobDto
            {
                Job = job,
                Reason = jsonJob.StateData.Reason,
                ExceptionDetails = stateData["ExceptionDetails"],
                ExceptionMessage = stateData["ExceptionMessage"],
                ExceptionType = stateData["ExceptionType"],
                FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
            });
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            return _storage.QueueProviders.GetProvider(queueName).GetJobQueueMonitoringApi();
        }

        public List<RavenJobRevisionDto> GetJobRevisions(string jobId)
        {
            if (string.IsNullOrEmpty(jobId))
                return new List<RavenJobRevisionDto>();

            try
            {
                using var session = _storage.Repository.OpenSession();
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);

                var revisions = session.Advanced.Revisions.GetFor<RavenJob>(id);
                if (revisions == null || revisions.Count == 0)
                    return new List<RavenJobRevisionDto>();

                var metadataList = session.Advanced.Revisions.GetMetadataFor(id);
                var result = new List<RavenJobRevisionDto>();

                for (int i = 0; i < revisions.Count; i++)
                {
                    var rev = revisions[i];
                    if (rev == null) continue;

                    var metadata = metadataList != null && i < metadataList.Count ? metadataList[i] : null;
                    var lastModified = metadata != null && metadata.TryGetValue("@last-modified", out var lm) && lm != null
                        ? (DateTime.TryParse(lm.ToString(), out var parsedDt) ? parsedDt : (DateTime?)null)
                        : (DateTime?)null;
                    var changeVector = metadata != null && metadata.TryGetValue("@change-vector", out var cv)
                        ? cv?.ToString()
                        : null;

                    var dataDict = rev.StateData?.Data != null
                        ? new Dictionary<string, string>(rev.StateData.Data, StringComparer.OrdinalIgnoreCase)
                        : new Dictionary<string, string>();

                    result.Add(new RavenJobRevisionDto
                    {
                        Id = rev.Id,
                        StateName = rev.StateData?.Name ?? "Created",
                        Reason = rev.StateData?.Reason,
                        Timestamp = lastModified ?? rev.CreatedAt,
                        StateData = dataDict,
                        ChangeVector = changeVector
                    });
                }

                return result;
            }
            catch
            {
                return new List<RavenJobRevisionDto>();
            }
        }
    }
}