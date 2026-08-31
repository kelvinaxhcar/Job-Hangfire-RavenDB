using Hangfire.Raven.Extensions;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace Hangfire.Raven.Storage
{
    public static class RavenServerStorageExtensions
    {
        public static void AddExpire<T>(
          this IAsyncAdvancedSessionOperations advanced,
          T obj,
          DateTime dateTime)
        {
            advanced.GetMetadataFor<T>(obj)["@expires"] = (object)dateTime;
        }

        public static void RemoveExpire<T>(this IAsyncAdvancedSessionOperations advanced, T obj)
        {
            ((IDictionary<string, object>)advanced.GetMetadataFor<T>(obj)).Remove("Raven-Expiration-Date");
        }

        public static DateTime? GetExpire<T>(this IAsyncAdvancedSessionOperations advanced, T obj)
        {
            object obj1;
            return advanced.GetMetadataFor<T>(obj).TryGetValue("Raven-Expiration-Date", out obj1) ? new DateTime?((DateTime)obj1) : new DateTime?();
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          RavenStorage storage)
        {
            storage.ThrowIfNull(nameof(storage));
            return configuration.UseStorage<RavenStorage>(storage);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          string connectionUrl,
          string database)
        {
            return configuration.UseRavenStorage(connectionUrl, database, (X509Certificate2)null, new RavenStorageOptions());
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          string connectionUrl,
          string database,
          X509Certificate2 certificate)
        {
            return configuration.UseRavenStorage(connectionUrl, database, certificate, new RavenStorageOptions());
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          string connectionUrl,
          string database,
          RavenStorageOptions options)
        {
            return configuration.UseRavenStorage(connectionUrl, database, (X509Certificate2)null, options);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          string connectionUrl,
          string database,
          X509Certificate2 certificate,
          RavenStorageOptions options)
        {
            connectionUrl.ThrowIfNull(nameof(connectionUrl));
            return configuration.UseRavenStorage(new[] { connectionUrl }, database, certificate, options);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          IEnumerable<string> urls,
          string database)
        {
            return configuration.UseRavenStorage(urls, database, (X509Certificate2)null, new RavenStorageOptions());
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          IEnumerable<string> urls,
          string database,
          X509Certificate2 certificate)
        {
            return configuration.UseRavenStorage(urls, database, certificate, new RavenStorageOptions());
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          IEnumerable<string> urls,
          string database,
          RavenStorageOptions options)
        {
            return configuration.UseRavenStorage(urls, database, (X509Certificate2)null, options);
        }

        public static IGlobalConfiguration<RavenStorage> UseRavenStorage(
          this IGlobalConfiguration configuration,
          IEnumerable<string> urls,
          string database,
          X509Certificate2 certificate,
          RavenStorageOptions options)
        {
            configuration.ThrowIfNull(nameof(configuration));
            database.ThrowIfNull(nameof(database));
            options.ThrowIfNull(nameof(options));
            var validatedUrls = ValidateAndNormalizeUrls(urls);

            RavenStorage storage = new RavenStorage(new RepositoryConfig
            {
                Urls = validatedUrls,
                Database = database,
                Certificate = certificate
            }, options);

            return configuration.UseStorage<RavenStorage>(storage);
        }

        private static string[] ValidateAndNormalizeUrls(IEnumerable<string> urls)
        {
            urls.ThrowIfNull(nameof(urls));
            var urlArray = urls as string[] ?? urls.ToArray();
            if (urlArray.Length == 0)
                throw new ArgumentException("At least one RavenDB URL must be provided.", nameof(urls));

            foreach (var url in urlArray)
            {
                if (string.IsNullOrWhiteSpace(url) || !url.StartsWith("http", StringComparison.OrdinalIgnoreCase))
                    throw new ArgumentException($"Connection Url '{url}' must begin with http or https!", nameof(urls));
            }

            return urlArray;
        }
    }
}
