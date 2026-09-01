using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Hangfire.Raven.Storage;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenUseStorageOverloadsFacts : TesteBase
    {
        public RavenUseStorageOverloadsFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void UseRavenStorage_WithCertificateAndOptions_SingleUrl_ConfiguresProperly()
        {
            var options = new RavenStorageOptions
            {
                InvisibilityTimeout = TimeSpan.FromMinutes(15)
            };

            var result = GlobalConfiguration.Configuration.UseRavenStorage(_store.Urls[0], _store.Database, (X509Certificate2)null, options);

            Assert.NotNull(result);
            var storage = (RavenStorage)result.Entry;
            Assert.Equal(TimeSpan.FromMinutes(15), storage.Options.InvisibilityTimeout);
            Assert.Equal(_store.Database, storage.Repository.DatabaseName);
        }

        [Fact]
        public void UseRavenStorage_WithCertificateAndOptions_ArrayUrls_ConfiguresProperly()
        {
            var options = new RavenStorageOptions
            {
                QueuePollInterval = TimeSpan.FromSeconds(5)
            };

            var result = GlobalConfiguration.Configuration.UseRavenStorage(_store.Urls, _store.Database, (X509Certificate2)null, options);

            Assert.NotNull(result);
            var storage = (RavenStorage)result.Entry;
            Assert.Equal(TimeSpan.FromSeconds(5), storage.Options.QueuePollInterval);
            Assert.Equal(_store.Database, storage.Repository.DatabaseName);
        }

        [Fact]
        public void UseRavenStorage_WithCertificateAndOptions_IEnumerableUrls_ConfiguresProperly()
        {
            IEnumerable<string> urls = new List<string>(_store.Urls);
            var options = new RavenStorageOptions
            {
                DistributedLockLifetime = TimeSpan.FromMinutes(2)
            };

            var result = GlobalConfiguration.Configuration.UseRavenStorage(urls, _store.Database, (X509Certificate2)null, options);

            Assert.NotNull(result);
            var storage = (RavenStorage)result.Entry;
            Assert.Equal(TimeSpan.FromMinutes(2), storage.Options.DistributedLockLifetime);
            Assert.Equal(_store.Database, storage.Repository.DatabaseName);
        }

        [Fact]
        public void UseRavenStorage_WithDocumentStore_ConfiguresProperly()
        {
            var options = new RavenStorageOptions
            {
                InvisibilityTimeout = TimeSpan.FromMinutes(10)
            };

            var result = GlobalConfiguration.Configuration.UseRavenStorage(_store, options);
            Assert.NotNull(result);
            var storage = (RavenStorage)result.Entry;
            Assert.Equal(TimeSpan.FromMinutes(10), storage.Options.InvisibilityTimeout);

            var resultDefault = GlobalConfiguration.Configuration.UseRavenStorage(_store);
            Assert.NotNull(resultDefault);
        }

        [Fact]
        public void UseRavenStorage_WithRepositoryConfig_ConfiguresProperly()
        {
            var repoConfig = new RepositoryConfig
            {
                Urls = _store.Urls,
                Database = _store.Database
            };
            var options = new RavenStorageOptions
            {
                QueuePollInterval = TimeSpan.FromSeconds(7)
            };

            var result = GlobalConfiguration.Configuration.UseRavenStorage(repoConfig, options);
            Assert.NotNull(result);
            var storage = (RavenStorage)result.Entry;
            Assert.Equal(TimeSpan.FromSeconds(7), storage.Options.QueuePollInterval);

            var resultDefault = GlobalConfiguration.Configuration.UseRavenStorage(repoConfig);
            Assert.NotNull(resultDefault);
        }
    }
}
