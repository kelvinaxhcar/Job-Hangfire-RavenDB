using Hangfire.Annotations;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Client.Exceptions;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue : IPersistentJobQueue
    {
        private readonly RavenStorage _storage;
        private readonly RavenStorageOptions _options;
        private static readonly object _lockObject = new();
        internal static readonly AutoResetEvent NewItemInQueueEvent = new(true);

        public RavenJobQueue([NotNull] RavenStorage storage, [NotNull] RavenStorageOptions options)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? throw new ArgumentNullException(nameof(options));
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0)
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            var fetchConditions = new Expression<Func<JobQueue, bool>>[]
            {
                job => job.FetchedAt == null,
                job => job.FetchedAt < DateTime.UtcNow.AddSeconds(-_options.InvisibilityTimeout.TotalSeconds)
            };

            int conditionIndex = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var currentCondition = fetchConditions[conditionIndex];

                using var documentSession = _storage.Repository.OpenSession();
                documentSession.Advanced.UseOptimisticConcurrency = true;

                lock (_lockObject)
                {
                    foreach (var queue in queues)
                    {
                        var jobQueue = documentSession
                            .Query<JobQueue>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(currentCondition)
                            .Where(j => j.Queue == queue)
                            .FirstOrDefault();

                        if (jobQueue != null)
                        {
                            try
                            {
                                jobQueue.FetchedAt = DateTime.UtcNow;
                                documentSession.SaveChanges();

                                return new RavenFetchedJob(_storage, jobQueue);
                            }
                            catch (ConcurrencyException)
                            {
                                // Log or handle the concurrency exception if necessary.
                            }
                        }
                    }
                }

                // Cycle through conditions
                conditionIndex = (conditionIndex + 1) % fetchConditions.Length;

                // Wait if no jobs are found
                if (conditionIndex == fetchConditions.Length - 1)
                {
                    WaitHandle.WaitAny(new[]
                    {
                        cancellationToken.WaitHandle,
                        NewItemInQueueEvent
                    }, _options.QueuePollInterval);

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
        }

        public void Enqueue(string queue, string jobId)
        {
            if (string.IsNullOrWhiteSpace(queue))
                throw new ArgumentException("Queue name must be provided.", nameof(queue));

            if (string.IsNullOrWhiteSpace(jobId))
                throw new ArgumentException("Job ID must be provided.", nameof(jobId));

            using var documentSession = _storage.Repository.OpenSession();

            var entity = new JobQueue
            {
                Id = _storage.Repository.GetId(typeof(JobQueue), queue, jobId),
                JobId = jobId,
                Queue = queue
            };

            documentSession.Store(entity);
            documentSession.SaveChanges();

            // Notify waiting threads that a new job has been added.
            NewItemInQueueEvent.Set();
        }
    }
}
