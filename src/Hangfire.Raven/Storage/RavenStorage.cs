using System;
using System.Collections.Generic;
using Hangfire.Logging;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Indexes;
using Hangfire.Raven.JobQueues;
using Hangfire.Storage;
using Microsoft.Extensions.Caching.Memory;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;

namespace Hangfire.Raven.Storage
{
    public class RavenStorage : JobStorage, IDisposable
    {
        private readonly RavenStorageOptions _options;
        private readonly IRepository _repository;
        private readonly IMemoryCache _cache;
        private readonly bool _disposeCache;

        public RavenStorage(RepositoryConfig config)
          : this(config, new RavenStorageOptions())
        {
        }

        public RavenStorage(RepositoryConfig config, RavenStorageOptions options)
          : this(new Repository(config), options)
        {
        }

        public RavenStorage(IRepository repository)
          : this(repository, new RavenStorageOptions())
        {
        }

        public RavenStorage(IDocumentStore documentStore)
          : this(new Repository(documentStore), new RavenStorageOptions())
        {
        }

        public RavenStorage(IDocumentStore documentStore, RavenStorageOptions options)
          : this(new Repository(documentStore), options)
        {
        }

        public RavenStorage(IRepository repository, RavenStorageOptions options)
        {
            repository.ThrowIfNull(nameof(repository));
            options.ThrowIfNull(nameof(options));
            _options = options;
            _repository = repository;

            if (_options.MemoryCache != null)
            {
                _cache = _options.MemoryCache;
                _disposeCache = false;
            }
            else
            {
                _cache = new MemoryCache(new MemoryCacheOptions());
                _disposeCache = true;
            }

            _repository.Create();
            _repository.EnsureRevisionsConfigured(_options);
            InitializeIndexes();
            InitializeQueueProviders();
        }

        public RavenStorageOptions Options => _options;

        public IRepository Repository => _repository;

        public IMemoryCache Cache => _cache;

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new RavenStorageMonitoringApi(this);
        }

        public override IStorageConnection GetConnection()
        {
            return new RavenConnection(this);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Raven job storage:");
        }

        private void InitializeIndexes()
        {
            _repository.ExecuteIndexes(new List<AbstractIndexCreationTask>
            {
                new JobQueue_ByQueueAndFetchedAt(),
                new RavenJobs_ByStateAndCreatedAt(),
                new JobQueue_Stats()
            });
        }

        private void InitializeQueueProviders()
        {
            QueueProviders = new PersistentJobQueueProviderCollection(new RavenJobQueueProvider(this, _options));
        }

        public void Dispose()
        {
            if (_disposeCache && _cache is IDisposable disposableCache)
            {
                disposableCache.Dispose();
            }
        }
    }
}
