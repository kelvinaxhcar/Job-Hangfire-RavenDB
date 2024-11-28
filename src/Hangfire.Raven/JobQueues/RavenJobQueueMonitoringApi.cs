using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Raven.Client.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private readonly RavenStorage _storage;

        public RavenJobQueueMonitoringApi([NotNull] RavenStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
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
            return GetJobIdsByQueueAndFetchStatus(queue, isFetched: false, pageFrom, perPage);
        }

        public IEnumerable<string> GetFetchedJobIds(string queue, int pageFrom, int perPage)
        {
            return GetJobIdsByQueueAndFetchStatus(queue, isFetched: true, pageFrom, perPage);
        }

        public EnqueuedAndFetchedCount GetEnqueuedAndFetchedCount(string queue)
        {
            using var documentSession = _storage.Repository.OpenSession();
            var enqueuedCount = documentSession
                .Query<JobQueue>()
                .Count(a => a.Queue == queue && a.FetchedAt == null);

            var fetchedCount = documentSession
                .Query<JobQueue>()
                .Count(a => a.Queue == queue && a.FetchedAt != null);

            return new EnqueuedAndFetchedCount
            {
                EnqueuedCount = enqueuedCount,
                FetchedCount = fetchedCount
            };
        }

        public IEnumerable<string> GetJobIdsByQueueAndFetchStatus(string queue, bool isFetched, int pageFrom, int perPage)
        {
            using var documentSession = _storage.Repository.OpenSession();

            var query = documentSession.Query<JobQueue>()
                                       .Where(job => job.Queue == queue);

            if (isFetched)
            {
                query = query.Where(job => job.FetchedAt != null);
            }
            else
            {
                query = query.Where(job => job.FetchedAt == null);
            }

            return query
                .Skip(pageFrom)
                .Take(perPage)
                .Select(job => job.JobId)
                .ToList();
        }
    }
}
