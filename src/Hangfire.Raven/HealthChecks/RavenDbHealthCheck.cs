using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;

namespace Hangfire.Raven.HealthChecks
{
    public class RavenDbHealthCheck : IHealthCheck
    {
        private readonly RavenStorage _storage;
        private readonly IDocumentStore _documentStore;
        private readonly IRepository _repository;
        private readonly IServiceProvider _serviceProvider;
        private readonly Func<IServiceProvider, RavenStorage> _storageFactory;
        private readonly RavenDbHealthCheckOptions _options;

        public RavenDbHealthCheck(RavenStorage storage, RavenDbHealthCheckOptions options = null)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? new RavenDbHealthCheckOptions();
        }

        public RavenDbHealthCheck(IDocumentStore documentStore, string database = null, RavenDbHealthCheckOptions options = null)
        {
            _documentStore = documentStore ?? throw new ArgumentNullException(nameof(documentStore));
            _options = options ?? new RavenDbHealthCheckOptions();
            if (!string.IsNullOrEmpty(database))
            {
                _options.Database = database;
            }
        }

        public RavenDbHealthCheck(IRepository repository, RavenDbHealthCheckOptions options = null)
        {
            _repository = repository ?? throw new ArgumentNullException(nameof(repository));
            _options = options ?? new RavenDbHealthCheckOptions();
        }

        public RavenDbHealthCheck(IServiceProvider serviceProvider, RavenDbHealthCheckOptions options = null)
        {
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            _options = options ?? new RavenDbHealthCheckOptions();
        }

        public RavenDbHealthCheck(Func<IServiceProvider, RavenStorage> storageFactory, IServiceProvider serviceProvider, RavenDbHealthCheckOptions options = null)
        {
            _storageFactory = storageFactory ?? throw new ArgumentNullException(nameof(storageFactory));
            _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            _options = options ?? new RavenDbHealthCheckOptions();
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            var data = new Dictionary<string, object>();

            try
            {
                var (store, database) = ResolveStoreAndDatabase();

                if (store == null)
                {
                    return new HealthCheckResult(
                        context?.Registration?.FailureStatus ?? HealthStatus.Unhealthy,
                        "RavenDB DocumentStore or RavenStorage could not be resolved from services.",
                        null,
                        data);
                }

                data["Database"] = database ?? store.Database ?? "default";
                data["Urls"] = store.Urls != null ? string.Join(", ", store.Urls) : string.Empty;

                var op = new GetStatisticsOperation();
                var stats = string.IsNullOrEmpty(database)
                    ? await store.Maintenance.SendAsync(op, cancellationToken).ConfigureAwait(false)
                    : await store.Maintenance.ForDatabase(database).SendAsync(op, cancellationToken).ConfigureAwait(false);

                if (stats == null)
                {
                    return new HealthCheckResult(
                        context?.Registration?.FailureStatus ?? HealthStatus.Unhealthy,
                        $"RavenDB returned null statistics for database '{data["Database"]}'.",
                        null,
                        data);
                }

                data["DocumentsCount"] = stats.CountOfDocuments;
                data["IndexesCount"] = stats.CountOfIndexes;
                data["StaleIndexesCount"] = stats.StaleIndexes?.Length ?? 0;

                if (stats.LastIndexingTime.HasValue)
                {
                    data["LastIndexingTime"] = stats.LastIndexingTime.Value;
                }

                if (_options.CheckStaleIndexes && stats.StaleIndexes != null && stats.StaleIndexes.Length > _options.MaxAllowedStaleIndexes)
                {
                    data["StaleIndexes"] = stats.StaleIndexes;
                    return HealthCheckResult.Degraded(
                        $"RavenDB storage has {stats.StaleIndexes.Length} stale index(es): {string.Join(", ", stats.StaleIndexes)}.",
                        null,
                        data);
                }

                return HealthCheckResult.Healthy("RavenDB storage is healthy.", data);
            }
            catch (Exception ex)
            {
                return new HealthCheckResult(
                    context?.Registration?.FailureStatus ?? HealthStatus.Unhealthy,
                    $"RavenDB health check failed: {ex.Message}",
                    ex,
                    data);
            }
        }

        private (IDocumentStore Store, string Database) ResolveStoreAndDatabase()
        {
            if (_documentStore != null)
            {
                return (_documentStore, _options.Database ?? _documentStore.Database);
            }

            if (_storage != null)
            {
                return (_storage.Repository?.DocumentStore, _options.Database ?? _storage.Repository?.DatabaseName);
            }

            if (_repository != null)
            {
                return (_repository.DocumentStore, _options.Database ?? _repository.DatabaseName);
            }

            if (_storageFactory != null && _serviceProvider != null)
            {
                var storageFromFactory = _storageFactory(_serviceProvider);
                if (storageFromFactory != null)
                {
                    return (storageFromFactory.Repository?.DocumentStore, _options.Database ?? storageFromFactory.Repository?.DatabaseName);
                }
            }

            if (_serviceProvider != null)
            {
                var storage = _serviceProvider.GetService<RavenStorage>()
                           ?? _serviceProvider.GetService<JobStorage>() as RavenStorage;

                if (storage != null)
                {
                    return (storage.Repository?.DocumentStore, _options.Database ?? storage.Repository?.DatabaseName);
                }

                var repo = _serviceProvider.GetService<IRepository>();
                if (repo != null)
                {
                    return (repo.DocumentStore, _options.Database ?? repo.DatabaseName);
                }

                var docStore = _serviceProvider.GetService<IDocumentStore>();
                if (docStore != null)
                {
                    return (docStore, _options.Database ?? docStore.Database);
                }
            }

            // Fallback to JobStorage.Current if configured
            try
            {
                if (JobStorage.Current is RavenStorage currentStorage)
                {
                    return (currentStorage.Repository?.DocumentStore, _options.Database ?? currentStorage.Repository?.DatabaseName);
                }
            }
            catch (InvalidOperationException)
            {
                // JobStorage.Current throws InvalidOperationException when not initialized
            }

            return (null, _options.Database);
        }
    }
}
