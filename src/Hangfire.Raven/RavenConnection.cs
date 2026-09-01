using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Raven.DistributedLocks;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Server;
using Hangfire.Storage;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace Hangfire.Raven
{
    public class RavenConnection : JobStorageConnection, IJobStorageBatchConnection, IStorageConnectionAsync
    {
        private static readonly ILog Logger = LogProvider.For<RavenConnection>();
        private static readonly SessionOptions NoTrackingOptions = new SessionOptions { NoTracking = true };
        private readonly RavenStorage _storage;

        public RavenConnection(RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            _storage = storage;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RavenWriteOnlyTransaction(_storage);
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new RavenDistributedLock(_storage, "HangFire/" + resource, timeout, _storage.Options);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
                throw new ArgumentNullException(nameof(queues));
            var array = queues.Select(_storage.QueueProviders.GetProvider).Distinct().ToArray();
            if (array.Length != 1)
                throw new InvalidOperationException("Multiple provider instances registered for queues: " + string.Join(", ", queues) + ". You should choose only one type of persistent queues per server instance.");
            return array[0].GetJobQueue().Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
          Job job,
          IDictionary<string, string> parameters,
          DateTime createdAt,
          TimeSpan expireIn)
        {
            job.ThrowIfNull(nameof(job));
            parameters.ThrowIfNull(nameof(parameters));
            var invocationData = InvocationData.SerializeJob(job);
            var expiredJob = Guid.NewGuid().ToString();
            var entity = new RavenJob()
            {
                Id = _storage.Repository.GetId(typeof(RavenJob), expiredJob),
                InvocationData = invocationData,
                CreatedAt = createdAt,
                Parameters = parameters
            };
            return ExecuteWithRetry(() =>
            {
                using (IDocumentSession session = _storage.Repository.OpenSession())
                {
                    session.Store(entity);
                    session.SetExpiry(entity, createdAt + expireIn);
                    session.SaveChanges();
                    return expiredJob;
                }
            });
        }

        public override JobData GetJobData(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("RavenJobData", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenJob), key);
                var ravenJob = documentSession.Load<RavenJob>(id);
                if (ravenJob == null)
                    return null;
                var job = (Job)null;
                var jobLoadException = (JobLoadException)null;
                try
                {
                    job = ravenJob.InvocationData.DeserializeJob();
                }
                catch (JobLoadException ex)
                {
                    jobLoadException = ex;
                }
                return new JobData()
                {
                    Job = job,
                    State = ravenJob.StateData?.Name,
                    CreatedAt = ravenJob.CreatedAt,
                    LoadException = jobLoadException
                };
            });
        }

        public override StateData GetStateData(string jobId)
        {
            jobId.ThrowIfNull(nameof(jobId));
            return GetOrCreateCached("RavenJobState", jobId, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                return documentSession.Load<RavenJob>(id)?.StateData;
            });
        }

        public override void SetJobParameter(string jobId, string name, string value)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            ExecuteWithRetry(() =>
            {
                using var documentSession = _storage.Repository.OpenSession();
                string id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                documentSession.Load<RavenJob>(id).Parameters[name] = value;
                documentSession.SaveChanges();
            });

            RemoveCache("RavenJobData", jobId);
            RemoveCache("RavenJobState", jobId);
            RemoveCache("JobParam", $"{jobId}:{name}");
        }

        public override string GetJobParameter(string jobId, string name)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            return GetOrCreateCached("JobParam", $"{jobId}:{name}", () =>
            {
                using var documentSession = _storage.Repository.OpenSession();
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var ravenJob = documentSession.Load<RavenJob>(id);
                if (ravenJob == null)
                    return null;
                if (ravenJob.Parameters.TryGetValue(name, out string jobParameter))
                    return jobParameter;
                if (!(name == "RetryCount"))
                    return null;
                ravenJob.Parameters["RetryCount"] = "0";
                documentSession.SaveChanges();
                return "0";
            });
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("SetItems", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenSet), key);
                var ravenSet = documentSession.Load<RavenSet>(id);
                return ravenSet == null ? new HashSet<string>() : new HashSet<string>(ravenSet.Scores.Keys);
            });
        }

        private const string LowestScoreQuery = @"
            declare function getLowestScore(doc, fromScore, toScore) {
                if (!doc || !doc.Scores) return null;
                var minScore = null;
                var minKey = null;
                for (var k in doc.Scores) {
                    if (Object.prototype.hasOwnProperty.call(doc.Scores, k)) {
                        var v = doc.Scores[k];
                        if (v >= fromScore && v <= toScore) {
                            if (minScore === null || v < minScore) {
                                minScore = v;
                                minKey = k;
                            }
                        }
                    }
                }
                return { Value: minKey };
            }
            from RavenSets as s
            where id() = $id
            select getLowestScore(s, $fromScore, $toScore)";

        private sealed class LowestScoreResult
        {
            public string Value { get; set; }
        }

        public override string GetFirstByLowestScoreFromSet(
          string key,
          double fromScore,
          double toScore)
        {
            key.ThrowIfNull(nameof(key));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return ExecuteWithRetry(() =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                string id = _storage.Repository.GetId(typeof(RavenSet), key);
                var result = documentSession.Advanced.RawQuery<LowestScoreResult>(LowestScoreQuery)
                    .AddParameter("id", id)
                    .AddParameter("fromScore", fromScore)
                    .AddParameter("toScore", toScore)
                    .FirstOrDefault();

                return result?.Value;
            });
        }

        public override void SetRangeInHash(
          string key,
          IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull(nameof(key));
            keyValuePairs.ThrowIfNull(nameof(keyValuePairs));
            ExecuteWithRetry(() =>
            {
                using var documentSession = _storage.Repository.OpenSession();
                var id = _storage.Repository.GetId(typeof(RavenHash), key);
                var entity = documentSession.Load<RavenHash>(id);
                if (entity == null)
                {
                    entity = new RavenHash() { Id = id };
                    documentSession.Store(entity);
                }
                foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
                    entity.Fields[keyValuePair.Key] = keyValuePair.Value;
                documentSession.SaveChanges();
            });

            RemoveCache("HashCount", key);
            RemoveCache("HashEntries", key);
            foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
                RemoveCache("HashValue", $"{key}:{keyValuePair.Key}");
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("HashEntries", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                return documentSession.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key))?.Fields;
            });
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            serverId.ThrowIfNull(nameof(serverId));
            context.ThrowIfNull(nameof(context));
            ExecuteWithRetry(() =>
            {
                using var documentSession = _storage.Repository.OpenSession();
                var id = _storage.Repository.GetId(typeof(RavenServer), serverId);
                var entity = documentSession.Load<RavenServer>(id);
                if (entity == null)
                {
                    entity = new RavenServer()
                    {
                        Id = id,
                        Data = new RavenServer.ServerData()
                        {
                            StartedAt = new DateTime?(DateTime.UtcNow)
                        }
                    };
                    documentSession.Store(entity);
                }
                entity.Data.WorkerCount = context.WorkerCount;
                entity.Data.Queues = context.Queues;
                entity.Data.StartedAt = new DateTime?(DateTime.UtcNow);
                entity.LastHeartbeat = DateTime.UtcNow;
                documentSession.SaveChanges();
            });
        }

        public override void RemoveServer(string serverId)
        {
            serverId.ThrowIfNull(nameof(serverId));
            ExecuteWithRetry(() =>
            {
                using var documentSession = _storage.Repository.OpenSession();
                var id = _storage.Repository.GetId(typeof(RavenServer), serverId);
                documentSession.Delete(id);
                documentSession.SaveChanges();
            });
        }

        public override void Heartbeat(string serverId)
        {
            serverId.ThrowIfNull(nameof(serverId));
            ExecuteWithRetry(() =>
            {
                using var documentSession = _storage.Repository.OpenSession();
                var id = _storage.Repository.GetId(typeof(RavenServer), serverId);
                var entity = documentSession.Load<RavenServer>(id);
                if (entity == null)
                {
                    Logger.WarnFormat("Server '{0}' was not found to update heartbeat.", serverId);
                    return;
                }
                entity.LastHeartbeat = DateTime.UtcNow;
                documentSession.SaveChanges();
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            return ExecuteWithRetry(() =>
            {
                using var documentSession = _storage.Repository.OpenSession();
                var heartBeatCutOff = DateTime.UtcNow.Add(timeOut.Negate());
                List<RavenServer> list = documentSession.Query<RavenServer>()
                    .Where(t => t.LastHeartbeat < heartBeatCutOff)
                    .ToList();

                foreach (RavenServer entity in list)
                    documentSession.Delete(entity);
                documentSession.SaveChanges();
                return list.Count;
            });
        }

        public override long GetSetCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("SetCount", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenSet), key);
                var ravenSet = documentSession.Load<RavenSet>(id);
                return ravenSet == null ? 0L : (long)ravenSet.Scores.Count;
            });
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var ravenSet = documentSession.Load<RavenSet>(id);

            return ravenSet == null
                ? new List<string>()
                : ravenSet.Scores
                    .Skip(startingFrom)
                    .Take(endingAt - startingFrom + 1)
                    .Select(t => t.Key)
                    .ToList();
        }

        public override TimeSpan GetSetTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var ravenSet = session.Load<RavenSet>(id);
            if (ravenSet == null)
                return TimeSpan.FromSeconds(-1.0);
            DateTime? expiry = session.GetExpiry<RavenSet>(ravenSet);
            return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
        }

        public override long GetCounter(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("Counter", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(Counter), key);
                var counter = documentSession.Load<Counter>(id);
                return counter == null ? 0L : (long)counter.Value;
            });
        }

        public override long GetHashCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("HashCount", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var ravenHash = documentSession.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key));
                return ravenHash == null ? 0L : (long)ravenHash.Fields.Count;
            });
        }

        public override TimeSpan GetHashTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenHash), key);
            var ravenHash = session.Load<RavenHash>(id);
            if (ravenHash == null)
                return TimeSpan.FromSeconds(-1.0);
            DateTime? expiry = session.GetExpiry<RavenHash>(ravenHash);
            return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
        }

        public override string GetValueFromHash(string key, string name)
        {
            key.ThrowIfNull(nameof(key));
            name.ThrowIfNull(nameof(name));
            return GetOrCreateCached("HashValue", $"{key}:{name}", () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var ravenHash = documentSession.Load<RavenHash>(_storage.Repository.GetId(typeof(RavenHash), key));
                return ravenHash == null || !ravenHash.Fields.TryGetValue(name, out string str) ? null : str;
            });
        }

        public override long GetListCount(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("ListCount", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenList), key);
                var ravenList = documentSession.Load<RavenList>(id);
                return ravenList == null ? 0L : ravenList.Values.Count;
            });
        }

        public override TimeSpan GetListTtl(string key)
        {
            key.ThrowIfNull(nameof(key));
            using var session = _storage.Repository.OpenSession();
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var ravenList = session.Load<RavenList>(id);
            if (ravenList == null)
                return TimeSpan.FromSeconds(-1.0);
            DateTime? expiry = session.GetExpiry(ravenList);
            return !expiry.HasValue ? TimeSpan.FromSeconds(-1.0) : expiry.Value - DateTime.UtcNow;
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            key.ThrowIfNull(nameof(key));
            using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var ravenList = documentSession.Load<RavenList>(id);
            return ravenList == null ? new List<string>() : ravenList.Values.Skip(startingFrom).Take(endingAt - startingFrom + 1).ToList();
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            key.ThrowIfNull(nameof(key));
            return GetOrCreateCached("ListItems", key, () =>
            {
                using var documentSession = _storage.Repository.OpenSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenList), key);
                var ravenList = documentSession.Load<RavenList>(id);
                return ravenList == null ? new List<string>() : ravenList.Values;
            });
        }

        public List<string> BatchEnqueue(IEnumerable<BatchJobItem> jobs, string queue = "default")
        {
            if (jobs == null) return new List<string>();

            var jobList = jobs.ToList();
            if (jobList.Count == 0) return new List<string>();

            var resultJobIds = new List<string>(jobList.Count);
            var queueName = string.IsNullOrEmpty(queue) ? "default" : queue;

            using (var bulk = _storage.Repository.BulkInsert())
            {
                foreach (var item in jobList)
                {
                    if (item.Job == null)
                        throw new ArgumentNullException(nameof(item.Job), "Job cannot be null in batch item.");

                    var jobId = item.JobId ?? Guid.NewGuid().ToString();
                    var invocationData = InvocationData.SerializeJob(item.Job);
                    var state = item.InitialState ?? new Hangfire.States.EnqueuedState(queueName);
                    var createdAt = DateTime.UtcNow;

                    var ravenJob = new RavenJob
                    {
                        Id = _storage.Repository.GetId(typeof(RavenJob), jobId),
                        InvocationData = invocationData,
                        CreatedAt = createdAt,
                        Parameters = item.Parameters ?? new Dictionary<string, string>(),
                        StateData = new StateData
                        {
                            Name = state.Name,
                            Reason = state.Reason,
                            Data = state.SerializeData()
                        },
                        History = new List<Hangfire.Storage.Monitoring.StateHistoryDto>
                        {
                            new Hangfire.Storage.Monitoring.StateHistoryDto
                            {
                                StateName = state.Name,
                                Reason = state.Reason,
                                Data = state.SerializeData(),
                                CreatedAt = createdAt
                            }
                        }
                    };

                    bulk.Store(ravenJob, ravenJob.Id);

                    if (string.Equals(state.Name, Hangfire.States.EnqueuedState.StateName, StringComparison.OrdinalIgnoreCase))
                    {
                        var queueEntity = new JobQueue
                        {
                            Id = _storage.Repository.GetId(typeof(JobQueue), queueName, jobId),
                            JobId = jobId,
                            Queue = queueName,
                            FetchedAt = null
                        };
                        bulk.Store(queueEntity, queueEntity.Id);
                    }

                    resultJobIds.Add(jobId);
                }
            }

            return resultJobIds;
        }

        public async Task<List<string>> BatchEnqueueAsync(IEnumerable<BatchJobItem> jobs, string queue = "default", CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return await Task.Run(() => BatchEnqueue(jobs, queue), cancellationToken);
        }

        public async Task<string> CreateExpiredJobAsync(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn, CancellationToken cancellationToken = default)
        {
            job.ThrowIfNull(nameof(job));
            parameters.ThrowIfNull(nameof(parameters));
            cancellationToken.ThrowIfCancellationRequested();

            var invocationData = InvocationData.SerializeJob(job);
            var expiredJob = Guid.NewGuid().ToString();
            var entity = new RavenJob()
            {
                Id = _storage.Repository.GetId(typeof(RavenJob), expiredJob),
                InvocationData = invocationData,
                CreatedAt = createdAt,
                Parameters = parameters
            };

            return await ExecuteWithRetryAsync(async ct =>
            {
                using var session = _storage.Repository.OpenAsyncSession();
                await session.StoreAsync(entity, ct).ConfigureAwait(false);
                session.SetExpiry(entity, createdAt + expireIn);
                await session.SaveChangesAsync(ct).ConfigureAwait(false);
                return expiredJob;
            }, cancellationToken).ConfigureAwait(false);
        }

        public async Task<JobData> GetJobDataAsync(string key, CancellationToken cancellationToken = default)
        {
            key.ThrowIfNull(nameof(key));
            cancellationToken.ThrowIfCancellationRequested();

            return await GetOrCreateCachedAsync("RavenJobData", key, async () =>
            {
                using var documentSession = _storage.Repository.OpenAsyncSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenJob), key);
                var ravenJob = await documentSession.LoadAsync<RavenJob>(id, cancellationToken);
                if (ravenJob == null)
                    return null;

                Job job = null;
                JobLoadException jobLoadException = null;
                try
                {
                    job = ravenJob.InvocationData.DeserializeJob();
                }
                catch (JobLoadException ex)
                {
                    jobLoadException = ex;
                }

                return new JobData()
                {
                    Job = job,
                    State = ravenJob.StateData?.Name,
                    CreatedAt = ravenJob.CreatedAt,
                    LoadException = jobLoadException
                };
            });
        }

        public async Task<StateData> GetStateDataAsync(string jobId, CancellationToken cancellationToken = default)
        {
            jobId.ThrowIfNull(nameof(jobId));
            cancellationToken.ThrowIfCancellationRequested();

            return await GetOrCreateCachedAsync("RavenJobState", jobId, async () =>
            {
                using var documentSession = _storage.Repository.OpenAsyncSession(NoTrackingOptions);
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var ravenJob = await documentSession.LoadAsync<RavenJob>(id, cancellationToken);
                return ravenJob?.StateData;
            });
        }

        public async Task SetJobParameterAsync(string jobId, string name, string value, CancellationToken cancellationToken = default)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            cancellationToken.ThrowIfCancellationRequested();

            await ExecuteWithRetryAsync(async ct =>
            {
                using var documentSession = _storage.Repository.OpenAsyncSession();
                string id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var ravenJob = await documentSession.LoadAsync<RavenJob>(id, ct);
                if (ravenJob != null)
                {
                    ravenJob.Parameters[name] = value;
                    await documentSession.SaveChangesAsync(ct);
                }
            }, cancellationToken).ConfigureAwait(false);

            RemoveCache("RavenJobData", jobId);
            RemoveCache("RavenJobState", jobId);
            RemoveCache("JobParam", $"{jobId}:{name}");
        }

        public async Task<string> GetJobParameterAsync(string jobId, string name, CancellationToken cancellationToken = default)
        {
            jobId.ThrowIfNull(nameof(jobId));
            name.ThrowIfNull(nameof(name));
            cancellationToken.ThrowIfCancellationRequested();

            return await GetOrCreateCachedAsync("JobParam", $"{jobId}:{name}", async () =>
            {
                using var documentSession = _storage.Repository.OpenAsyncSession();
                var id = _storage.Repository.GetId(typeof(RavenJob), jobId);
                var ravenJob = await documentSession.LoadAsync<RavenJob>(id, cancellationToken);
                if (ravenJob == null)
                    return null;

                if (ravenJob.Parameters.TryGetValue(name, out string jobParameter))
                    return jobParameter;

                if (name == "RetryCount")
                {
                    ravenJob.Parameters["RetryCount"] = "0";
                    await documentSession.SaveChangesAsync(cancellationToken);
                    return "0";
                }

                return null;
            });
        }

        public Task<HashSet<string>> GetAllItemsFromSetAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetAllItemsFromSet(key));
        }

        public async Task<string> GetFirstByLowestScoreFromSetAsync(string key, double fromScore, double toScore, CancellationToken cancellationToken = default)
        {
            key.ThrowIfNull(nameof(key));
            if (toScore < fromScore)
                throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            cancellationToken.ThrowIfCancellationRequested();

            return await ExecuteWithRetryAsync(async ct =>
            {
                using var asyncSession = _storage.Repository.OpenAsyncSession(NoTrackingOptions);
                string id = _storage.Repository.GetId(typeof(RavenSet), key);
                var result = await asyncSession.Advanced.AsyncRawQuery<LowestScoreResult>(LowestScoreQuery)
                    .AddParameter("id", id)
                    .AddParameter("fromScore", fromScore)
                    .AddParameter("toScore", toScore)
                    .FirstOrDefaultAsync(ct)
                    .ConfigureAwait(false);

                return result?.Value;
            }, cancellationToken).ConfigureAwait(false);
        }

        public Task SetRangeInHashAsync(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SetRangeInHash(key, keyValuePairs);
            return Task.CompletedTask;
        }

        public Task<Dictionary<string, string>> GetAllEntriesFromHashAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetAllEntriesFromHash(key));
        }

        public Task AnnounceServerAsync(string serverId, ServerContext context, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AnnounceServer(serverId, context);
            return Task.CompletedTask;
        }

        public Task RemoveServerAsync(string serverId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RemoveServer(serverId);
            return Task.CompletedTask;
        }

        public Task HeartbeatAsync(string serverId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Heartbeat(serverId);
            return Task.CompletedTask;
        }

        public Task<int> RemoveTimedOutServersAsync(TimeSpan timeOut, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(RemoveTimedOutServers(timeOut));
        }

        public Task<long> GetSetCountAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetSetCount(key));
        }

        public Task<List<string>> GetRangeFromSetAsync(string key, int startingFrom, int endingAt, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetRangeFromSet(key, startingFrom, endingAt));
        }

        public Task<TimeSpan> GetSetTtlAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetSetTtl(key));
        }

        public Task<long> GetCounterAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetCounter(key));
        }

        public Task<long> GetHashCountAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetHashCount(key));
        }

        public Task<TimeSpan> GetHashTtlAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetHashTtl(key));
        }

        public Task<string> GetValueFromHashAsync(string key, string name, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetValueFromHash(key, name));
        }

        public Task<long> GetListCountAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetListCount(key));
        }

        public Task<TimeSpan> GetListTtlAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetListTtl(key));
        }

        public Task<List<string>> GetRangeFromListAsync(string key, int startingFrom, int endingAt, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetRangeFromList(key, startingFrom, endingAt));
        }

        public Task<List<string>> GetAllItemsFromListAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(GetAllItemsFromList(key));
        }

        public Task<IWriteOnlyTransactionAsync> CreateWriteTransactionAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult<IWriteOnlyTransactionAsync>(new RavenWriteOnlyTransaction(_storage));
        }

        private string GetCacheKey(string type, string key) => $"Hangfire:Raven:{_storage.Options.ClientId}:{type}:{key}";

        private T ExecuteWithRetry<T>(Func<T> action)
        {
            var policy = _storage?.Options?.RetryPolicy;
            if (policy != null)
            {
                return policy.Execute(_ => action());
            }
            return action();
        }

        private void ExecuteWithRetry(Action action)
        {
            var policy = _storage?.Options?.RetryPolicy;
            if (policy != null)
            {
                policy.Execute(_ => action());
            }
            else
            {
                action();
            }
        }

        private async Task<T> ExecuteWithRetryAsync<T>(Func<CancellationToken, Task<T>> action, CancellationToken cancellationToken = default)
        {
            var policy = _storage?.Options?.RetryPolicy;
            if (policy != null)
            {
                return await policy.ExecuteAsync(async ct => await action(ct).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
            }
            return await action(cancellationToken).ConfigureAwait(false);
        }

        private async Task ExecuteWithRetryAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default)
        {
            var policy = _storage?.Options?.RetryPolicy;
            if (policy != null)
            {
                await policy.ExecuteAsync(async ct => await action(ct).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await action(cancellationToken).ConfigureAwait(false);
            }
        }

        private T GetOrCreateCached<T>(string type, string key, Func<T> factory)
        {
            if (!_storage.Options.EnableCache || _storage.Cache == null)
            {
                return ExecuteWithRetry(factory);
            }

            var cacheKey = GetCacheKey(type, key);
            if (_storage.Cache.TryGetValue(cacheKey, out T cachedValue))
            {
                return cachedValue;
            }

            var value = ExecuteWithRetry(factory);
            var options = new MemoryCacheEntryOptions
            {
                SlidingExpiration = _storage.Options.CacheSlidingExpiration
            };
            _storage.Cache.Set(cacheKey, value, options);
            return value;
        }

        private async Task<T> GetOrCreateCachedAsync<T>(string type, string key, Func<Task<T>> factory)
        {
            if (!_storage.Options.EnableCache || _storage.Cache == null)
            {
                return await ExecuteWithRetryAsync(_ => factory()).ConfigureAwait(false);
            }

            var cacheKey = GetCacheKey(type, key);
            if (_storage.Cache.TryGetValue(cacheKey, out T cachedValue))
            {
                return cachedValue;
            }

            var value = await ExecuteWithRetryAsync(_ => factory()).ConfigureAwait(false);
            var options = new MemoryCacheEntryOptions
            {
                SlidingExpiration = _storage.Options.CacheSlidingExpiration
            };
            _storage.Cache.Set(cacheKey, value, options);
            return value;
        }

        private void RemoveCache(string type, string key)
        {
            if (_storage.Options.EnableCache && _storage.Cache != null)
            {
                var cacheKey = GetCacheKey(type, key);
                _storage.Cache.Remove(cacheKey);
            }
        }
    }
}
