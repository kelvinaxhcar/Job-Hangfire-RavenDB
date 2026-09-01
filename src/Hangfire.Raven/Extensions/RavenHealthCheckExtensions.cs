using System;
using System.Collections.Generic;
using Hangfire.Raven.HealthChecks;
using Hangfire.Raven.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Raven.Client.Documents;

namespace Hangfire.Raven.Extensions
{
    public static class RavenHealthCheckExtensions
    {
        public const string DefaultName = "ravendb";

        /// <summary>
        /// Registers a RavenDB health check that resolves the RavenStorage or IDocumentStore from dependency injection.
        /// </summary>
        public static IHealthChecksBuilder AddRavenDb(
            this IHealthChecksBuilder builder,
            string name = DefaultName,
            HealthStatus? failureStatus = null,
            IEnumerable<string> tags = null,
            TimeSpan? timeout = null,
            Action<RavenDbHealthCheckOptions> configureOptions = null)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));

            var options = new RavenDbHealthCheckOptions();
            configureOptions?.Invoke(options);

            return builder.Add(new HealthCheckRegistration(
                name ?? DefaultName,
                sp => new RavenDbHealthCheck(sp, options),
                failureStatus,
                tags,
                timeout));
        }

        /// <summary>
        /// Registers a RavenDB health check using a specified RavenStorage instance.
        /// </summary>
        public static IHealthChecksBuilder AddRavenDb(
            this IHealthChecksBuilder builder,
            RavenStorage storage,
            string name = DefaultName,
            HealthStatus? failureStatus = null,
            IEnumerable<string> tags = null,
            TimeSpan? timeout = null,
            Action<RavenDbHealthCheckOptions> configureOptions = null)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            var options = new RavenDbHealthCheckOptions();
            configureOptions?.Invoke(options);

            return builder.Add(new HealthCheckRegistration(
                name ?? DefaultName,
                _ => new RavenDbHealthCheck(storage, options),
                failureStatus,
                tags,
                timeout));
        }

        /// <summary>
        /// Registers a RavenDB health check using a factory to resolve RavenStorage.
        /// </summary>
        public static IHealthChecksBuilder AddRavenDb(
            this IHealthChecksBuilder builder,
            Func<IServiceProvider, RavenStorage> storageFactory,
            string name = DefaultName,
            HealthStatus? failureStatus = null,
            IEnumerable<string> tags = null,
            TimeSpan? timeout = null,
            Action<RavenDbHealthCheckOptions> configureOptions = null)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            if (storageFactory == null) throw new ArgumentNullException(nameof(storageFactory));

            var options = new RavenDbHealthCheckOptions();
            configureOptions?.Invoke(options);

            return builder.Add(new HealthCheckRegistration(
                name ?? DefaultName,
                sp => new RavenDbHealthCheck(storageFactory, sp, options),
                failureStatus,
                tags,
                timeout));
        }

        /// <summary>
        /// Registers a RavenDB health check using a specified IDocumentStore instance.
        /// </summary>
        public static IHealthChecksBuilder AddRavenDb(
            this IHealthChecksBuilder builder,
            IDocumentStore documentStore,
            string database = null,
            string name = DefaultName,
            HealthStatus? failureStatus = null,
            IEnumerable<string> tags = null,
            TimeSpan? timeout = null,
            Action<RavenDbHealthCheckOptions> configureOptions = null)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            if (documentStore == null) throw new ArgumentNullException(nameof(documentStore));

            var options = new RavenDbHealthCheckOptions();
            configureOptions?.Invoke(options);

            return builder.Add(new HealthCheckRegistration(
                name ?? DefaultName,
                _ => new RavenDbHealthCheck(documentStore, database, options),
                failureStatus,
                tags,
                timeout));
        }

        /// <summary>
        /// Registers a RavenDB health check using a specified IRepository instance.
        /// </summary>
        public static IHealthChecksBuilder AddRavenDb(
            this IHealthChecksBuilder builder,
            IRepository repository,
            string name = DefaultName,
            HealthStatus? failureStatus = null,
            IEnumerable<string> tags = null,
            TimeSpan? timeout = null,
            Action<RavenDbHealthCheckOptions> configureOptions = null)
        {
            if (builder == null) throw new ArgumentNullException(nameof(builder));
            if (repository == null) throw new ArgumentNullException(nameof(repository));

            var options = new RavenDbHealthCheckOptions();
            configureOptions?.Invoke(options);

            return builder.Add(new HealthCheckRegistration(
                name ?? DefaultName,
                _ => new RavenDbHealthCheck(repository, options),
                failureStatus,
                tags,
                timeout));
        }
    }
}
