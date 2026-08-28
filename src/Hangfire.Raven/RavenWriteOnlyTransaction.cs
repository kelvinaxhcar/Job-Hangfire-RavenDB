using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;

using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Raven
{
    public class RavenWriteOnlyTransaction : JobStorageTransaction, IWriteOnlyTransactionAsync
    {
        private static readonly ILog Logger = LogProvider.For<RavenWriteOnlyTransaction>();
        private readonly RavenStorage _storage;
        private readonly IDocumentSession _session;
        private readonly Queue<Action> _afterCommitCommandQueue = new Queue<Action>();

        public RavenWriteOnlyTransaction([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            _storage = storage;
            _session = _storage.Repository.OpenSession();
            _session.Advanced.UseOptimisticConcurrency = false;
            _session.Advanced.MaxNumberOfRequestsPerSession = int.MaxValue;
        }

        public override void Commit()
        {
            try
            {
                _session.SaveChanges();
            }
            catch (ConcurrencyException ex)
            {
                Logger.Error($"- Concurrency exception: {ex.Message}");
                throw;
            }

            foreach (var afterCommitCommand in _afterCommitCommandQueue)
                afterCommitCommand();
        }


        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            _session.SetExpiry<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId), expireIn);
        }

        public override void PersistJob(string jobId)
        {
            _session.RemoveExpiry<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId));
        }

        public override void SetJobState(string jobId, IState state)
        {
            var ravenJob = _session.Load<RavenJob>(_storage.Repository.GetId(typeof(RavenJob), jobId));
            ravenJob.History.Insert(0, new StateHistoryDto()
            {
                StateName = state.Name,
                Data = state.SerializeData(),
                Reason = state.Reason,
                CreatedAt = DateTime.UtcNow
            });
            ravenJob.StateData = new StateData()
            {
                Name = state.Name,
                Data = state.SerializeData(),
                Reason = state.Reason
            };
        }

        public override void AddJobState(string jobId, IState state) => SetJobState(jobId, state);

        public override void AddRangeToSet(string key, IList<string> items)
        {
            key.ThrowIfNull(nameof(key));
            items.ThrowIfNull(nameof(items));
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var patchRequest = new PatchRequest
            {
                Script = "for (var i = 0; i < args.items.length; i++) { this.Scores[args.items[i]] = 0.0; }",
                Values = new Dictionary<string, object> { { "items", items } }
            };
            var patchIfMissing = new PatchRequest
            {
                Script = "this['@metadata'] = { '@collection': 'RavenSets' }; this.Scores = {}; for (var i = 0; i < args.items.length; i++) { this.Scores[args.items[i]] = 0.0; }",
                Values = new Dictionary<string, object> { { "items", items } }
            };
            _session.Advanced.Defer(new PatchCommandData(id, null, patchRequest, patchIfMissing));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var jobQueue = _storage.QueueProviders.GetProvider(queue).GetJobQueue();
            jobQueue.Enqueue(queue, jobId);
            if (!(jobQueue.GetType() == typeof(RavenJobQueue)))
                return;
            _afterCommitCommandQueue.Enqueue(() => RavenJobQueue.NewItemInQueueEvent.Set());
        }

        public override void IncrementCounter(string key)
        {
            IncrementCounter(key, TimeSpan.MinValue);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            UpdateCounter(key, expireIn, 1);
        }

        public override void DecrementCounter(string key)
        {
            DecrementCounter(key, TimeSpan.MinValue);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            UpdateCounter(key, expireIn, -1);
        }

        public void UpdateCounter(string key, TimeSpan expireIn, int value)
        {
            string id = _storage.Repository.GetId(typeof(Counter), key);
            var expires = expireIn != TimeSpan.MinValue ? (object)(DateTime.UtcNow + expireIn).ToString("O") : null;

            var patchRequest = new PatchRequest
            {
                Script = "this.Value += args.val; if (args.expires) { this['@metadata']['@expires'] = args.expires; }",
                Values = new Dictionary<string, object> { { "val", (double)value }, { "expires", expires } }
            };

            var patchIfMissing = new PatchRequest
            {
                Script = "this.Value = args.val; this['@metadata'] = { '@collection': 'Counters' }; if (args.expires) { this['@metadata']['@expires'] = args.expires; }",
                Values = new Dictionary<string, object> { { "val", (double)value }, { "expires", expires } }
            };

            _session.Advanced.Defer(new PatchCommandData(id, null, patchRequest, patchIfMissing));
        }

        public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0);

        public override void AddToSet(string key, string value, double score)
        {
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var patchRequest = new PatchRequest
            {
                Script = "this.Scores[args.value] = args.score;",
                Values = new Dictionary<string, object> { { "value", value }, { "score", score } }
            };
            var patchIfMissing = new PatchRequest
            {
                Script = "this['@metadata'] = { '@collection': 'RavenSets' }; this.Scores = { [args.value]: args.score };",
                Values = new Dictionary<string, object> { { "value", value }, { "score", score } }
            };
            _session.Advanced.Defer(new PatchCommandData(id, null, patchRequest, patchIfMissing));
        }

        public override void RemoveFromSet(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            var id = _storage.Repository.GetId(typeof(RavenSet), key);
            var patchRequest = new PatchRequest
            {
                Script = "delete this.Scores[args.value];",
                Values = new Dictionary<string, object> { { "value", value } }
            };
            _session.Advanced.Defer(new PatchCommandData(id, null, patchRequest, null));
        }

        public override void RemoveSet(string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.Delete(_storage.Repository.GetId(typeof(RavenSet), key));
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            _session.SetExpiry(FindOrCreateSet(_storage.Repository.GetId(typeof(RavenSet), key)), expireIn);
        }

        public override void PersistSet([NotNull] string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.RemoveExpiry(FindOrCreateSet(_storage.Repository.GetId(typeof(RavenSet), key)));
        }

        public override void InsertToList(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var patchRequest = new PatchRequest
            {
                Script = "this.Values.push(args.value);",
                Values = new Dictionary<string, object> { { "value", value } }
            };
            var patchIfMissing = new PatchRequest
            {
                Script = "this['@metadata'] = { '@collection': 'RavenLists' }; this.Values = [args.value];",
                Values = new Dictionary<string, object> { { "value", value } }
            };
            _session.Advanced.Defer(new PatchCommandData(id, null, patchRequest, patchIfMissing));
        }

        public override void RemoveFromList(string key, string value)
        {
            key.ThrowIfNull(nameof(key));
            var id = _storage.Repository.GetId(typeof(RavenList), key);
            var patchRequest = new PatchRequest
            {
                Script = "if (this.Values) { this.Values = this.Values.filter(function(v) { return v !== args.value; }); }",
                Values = new Dictionary<string, object> { { "value", value } }
            } ;
            _session.Advanced.Defer(new PatchCommandData(id, null, patchRequest, null));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            var orCreateList = FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key));
            orCreateList.Values = orCreateList.Values.Skip(keepStartingFrom).Take(keepEndingAt - keepStartingFrom + 1).ToList();
            if (orCreateList.Values.Count != 0)
                return;
            _session.Delete(orCreateList);
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            _session.SetExpiry(FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key)), expireIn);
        }

        public override void PersistList(string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.RemoveExpiry(FindOrCreateList(_storage.Repository.GetId(typeof(RavenList), key)));
        }

        public override void SetRangeInHash(
          string key,
          IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            key.ThrowIfNull(nameof(key));
            keyValuePairs.ThrowIfNull(nameof(keyValuePairs));
            var id = _storage.Repository.GetId(typeof(RavenHash), key);
            
            var script = "for (var i = 0; i < args.pairs.length; i++) { this.Fields[args.pairs[i].Key] = args.pairs[i].Value; }";
            var values = new Dictionary<string, object> { { "pairs", keyValuePairs.ToList() } };

            var patchRequest = new PatchRequest { Script = script, Values = values };
            var patchIfMissing = new PatchRequest { 
                Script = "this['@metadata'] = { '@collection': 'RavenHashes' }; this.Fields = {}; " + script, 
                Values = values 
            };

            _session.Advanced.Defer(new PatchCommandData(id, null, patchRequest, patchIfMissing));
        }

        public override void RemoveHash(string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.Delete(_storage.Repository.GetId(typeof(RavenHash), key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            key.ThrowIfNull(nameof(key));
            _session.SetExpiry(FindOrCreateHash(_storage.Repository.GetId(typeof(RavenHash), key)), expireIn);
        }

        public override void PersistHash([NotNull] string key)
        {
            key.ThrowIfNull(nameof(key));
            _session.RemoveExpiry(FindOrCreateHash(_storage.Repository.GetId(typeof(RavenHash), key)));
        }

        private RavenSet FindOrCreateSet(string id)
        {
            return FindOrCreate(id, () => new RavenSet()
            {
                Id = id
            });
        }

        private RavenHash FindOrCreateHash(string id)
        {
            return FindOrCreate(id, () => new RavenHash()
            {
                Id = id
            });
        }

        private RavenList FindOrCreateList(string id)
        {
            return FindOrCreate(id, () => new RavenList()
            {
                Id = id
            });
        }

        private T FindOrCreate<T>(string id, Func<T> builder)
        {
            T entity = _session.Load<T>(id);
            if (entity == null)
            {
                entity = builder();
                _session.Store(entity);
            }
            return entity;
        }

        public Task CommitAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Commit();
            return Task.CompletedTask;
        }

        public Task ExpireJobAsync(string jobId, TimeSpan expireIn, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ExpireJob(jobId, expireIn);
            return Task.CompletedTask;
        }

        public Task PersistJobAsync(string jobId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PersistJob(jobId);
            return Task.CompletedTask;
        }

        public Task SetJobStateAsync(string jobId, IState state, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SetJobState(jobId, state);
            return Task.CompletedTask;
        }

        public Task AddJobStateAsync(string jobId, IState state, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AddJobState(jobId, state);
            return Task.CompletedTask;
        }

        public Task AddToQueueAsync(string queue, string jobId, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AddToQueue(queue, jobId);
            return Task.CompletedTask;
        }

        public Task IncrementCounterAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IncrementCounter(key);
            return Task.CompletedTask;
        }

        public Task IncrementCounterAsync(string key, TimeSpan expireIn, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IncrementCounter(key, expireIn);
            return Task.CompletedTask;
        }

        public Task DecrementCounterAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DecrementCounter(key);
            return Task.CompletedTask;
        }

        public Task DecrementCounterAsync(string key, TimeSpan expireIn, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DecrementCounter(key, expireIn);
            return Task.CompletedTask;
        }

        public Task AddToSetAsync(string key, string value, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AddToSet(key, value);
            return Task.CompletedTask;
        }

        public Task AddToSetAsync(string key, string value, double score, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            AddToSet(key, value, score);
            return Task.CompletedTask;
        }

        public Task RemoveFromSetAsync(string key, string value, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RemoveFromSet(key, value);
            return Task.CompletedTask;
        }

        public Task InsertToListAsync(string key, string value, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            InsertToList(key, value);
            return Task.CompletedTask;
        }

        public Task RemoveFromListAsync(string key, string value, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RemoveFromList(key, value);
            return Task.CompletedTask;
        }

        public Task TrimListAsync(string key, int keepStartingFrom, int keepEndingAt, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            TrimList(key, keepStartingFrom, keepEndingAt);
            return Task.CompletedTask;
        }

        public Task SetRangeInHashAsync(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SetRangeInHash(key, keyValuePairs);
            return Task.CompletedTask;
        }

        public Task RemoveHashAsync(string key, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            RemoveHash(key);
            return Task.CompletedTask;
        }
    }
}
