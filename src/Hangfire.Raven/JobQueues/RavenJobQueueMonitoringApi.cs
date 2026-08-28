using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private RavenStorage _storage;

        public RavenJobQueueMonitoringApi([NotNull] RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            _storage = storage;
        }

        public IEnumerable<string> GetQueues()
        {
            using var documentSession = _storage.Repository.OpenSession();
            return documentSession
                .Query<JobQueue>()
                .Select(x => x.Queue)
                .Distinct()
                .ToList();
        }

        public IEnumerable<string> GetEnqueuedJobIds(string queue, int pageFrom, int perPage)
        {
            using var documentSession = _storage.Repository.OpenSession();
            return documentSession
                .Query<JobQueue>()
                .Where(a => a.Queue == queue && a.FetchedAt == new DateTime?())
                .Skip(pageFrom)
                .Take(perPage)
                .Select(a => a.JobId)
                .ToList();
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int pageFrom, int perPage)
        {
            using var documentSession = _storage.Repository.OpenSession();
            return documentSession
                .Query<JobQueue>()
                .Where(a => a.Queue == queue && a.FetchedAt != new DateTime?()).Skip(pageFrom)
                .Take(perPage)
                .Select(a => a.JobId)
                .ToList();
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using var documentSession = _storage.Repository.OpenSession();
            var fetchedLazy = documentSession.Query<JobQueue>().Statistics(out var fetchedStats).Where(a => a.FetchedAt != null && a.Queue == queue).Take(0).Lazily();
            var enqueuedLazy = documentSession.Query<JobQueue>().Statistics(out var enqueuedStats).Where(a => a.FetchedAt == null && a.Queue == queue).Take(0).Lazily();
            
            _ = fetchedLazy.Value; // Triggers batch

            return new EnqueuedAndFetchedCount()
            {
                EnqueuedCount = (int)enqueuedStats.TotalResults,
                FetchedCount = (int)fetchedStats.TotalResults
            };
        }
    }
}
