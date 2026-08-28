using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

namespace Hangfire.Raven.JobQueues
{
    public class RavenJobQueue : IPersistentJobQueue, IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<RavenJobQueue>();
        private readonly RavenStorage _storage;
        private readonly RavenStorageOptions _options;
        public static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(true);
        private IDatabaseChanges _databaseChanges;
        private IDisposable _changesSubscription;

        public RavenJobQueue([NotNull] RavenStorage storage, RavenStorageOptions options)
        {
            storage.ThrowIfNull(nameof(storage));
            options.ThrowIfNull(nameof(options));
            _storage = storage;
            _options = options;

            SubscribeToChanges();
        }

        private void SubscribeToChanges()
        {
            if (!_options.EnableChangesApiQueueEvents) return;

            try
            {
                var docStore = _storage.Repository.DocumentStore;
                if (docStore == null) return;

                _databaseChanges = docStore.Changes(_storage.Repository.DatabaseName);
                _changesSubscription = _databaseChanges.ForDocumentsInCollection<JobQueue>()
                    .Subscribe(new ActionObserver<DocumentChange>(change =>
                    {
                        if (change != null && change.Type == DocumentChangeTypes.Put)
                        {
                            NewItemInQueueEvent.Set();
                        }
                    }));

                _databaseChanges.EnsureConnectedNow();
            }
            catch (Exception ex)
            {
                Logger.WarnException("Could not initialize RavenDB Changes API for JobQueue real-time notifications. Falling back to periodic polling.", ex);
            }
        }

        private class ActionObserver<T> : IObserver<T>
        {
            private readonly Action<T> _onNext;

            public ActionObserver(Action<T> onNext)
            {
                _onNext = onNext;
            }

            public void OnCompleted() { }
            public void OnError(Exception error) { }
            public void OnNext(T value) => _onNext?.Invoke(value);
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            queues.ThrowIfNull(nameof(queues));

            if (queues.Length == 0)
                throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            int index = 0;

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var timeoutThreshold = DateTime.UtcNow.AddSeconds(-_options.InvisibilityTimeout.TotalSeconds);
                Expression<Func<JobQueue, bool>> expression = index == 0
                    ? (job => job.FetchedAt == null)
                    : (job => job.FetchedAt < timeoutThreshold);

                using (IDocumentSession documentSession = _storage.Repository.OpenSession())
                {
                    documentSession.Advanced.UseOptimisticConcurrency = true;

                    var lazyQueries = queues.Select(queue => documentSession
                        .Query<JobQueue, JobQueue_ByQueueAndFetchedAt>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(expression)
                        .Where(j => j.Queue == queue)
                        .Take(1)
                        .Lazily()
                    ).ToArray();

                    foreach (var lazyLoad in lazyQueries)
                    {
                        var jobQueue = lazyLoad.Value.FirstOrDefault();
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
                                // Someone else got the job, try next queue or next poll
                            }
                        }
                    }
                }

                index = (index + 1) % 2;

                if (index == 0)
                {
                    WaitHandle.WaitAny(new WaitHandle[]
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
            using (IDocumentSession documentSession = _storage.Repository.OpenSession())
            {
                JobQueue entity = new JobQueue()
                {
                    Id = _storage.Repository.GetId(typeof(JobQueue), queue, jobId),
                    JobId = jobId,
                    Queue = queue
                };
                documentSession.Store((object)entity);
                documentSession.SaveChanges();
            }
            NewItemInQueueEvent.Set();
        }

        public void Dispose()
        {
            try
            {
                _changesSubscription?.Dispose();
                _databaseChanges?.Dispose();
            }
            catch
            {
            }
        }
    }
}
