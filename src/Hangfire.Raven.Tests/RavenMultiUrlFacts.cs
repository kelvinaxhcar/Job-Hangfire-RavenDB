using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using Hangfire.Raven.Storage;
using Moq;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenMultiUrlFacts
    {
        [Fact]
        public void RepositoryConfig_SettingConnectionUrl_SetsUrlsArray()
        {
            var config = new RepositoryConfig
            {
                ConnectionUrl = "http://localhost:8080"
            };

            Assert.Equal("http://localhost:8080", config.ConnectionUrl);
            Assert.Single(config.Urls);
            Assert.Equal("http://localhost:8080", config.Urls[0]);
        }

        [Fact]
        public void RepositoryConfig_SettingUrls_SetsConnectionUrlToFirstItem()
        {
            var urls = new[] { "http://node1:8080", "http://node2:8080", "http://node3:8080" };
            var config = new RepositoryConfig
            {
                Urls = urls
            };

            Assert.Equal("http://node1:8080", config.ConnectionUrl);
            Assert.Equal(3, config.Urls.Length);
            Assert.Equal(urls, config.Urls);
        }

        [Fact]
        public void RepositoryConfig_Defaults_AreSafe()
        {
            var config = new RepositoryConfig();

            Assert.Null(config.ConnectionUrl);
            Assert.NotNull(config.Urls);
            Assert.Empty(config.Urls);
        }

        [Fact]
        public void Repository_ThrowsArgumentNullException_WhenConfigIsNull()
        {
            Assert.Throws<ArgumentNullException>(() => new Repository(null));
        }

        [Fact]
        public void Repository_ThrowsArgumentException_WhenNoUrlsConfigured()
        {
            var config = new RepositoryConfig { Database = "TestDB" };

            Assert.Throws<ArgumentException>(() => new Repository(config));
        }

        [Fact]
        public void Repository_InitializesDocumentStoreUrls_WithMultipleUrls()
        {
            var urls = new[] { "http://node1:8080", "http://node2:8080" };
            var config = new RepositoryConfig
            {
                Urls = urls,
                Database = "ClusterDB"
            };

            using var repo = new Repository(config);

            Assert.NotNull(repo.DocumentStore);
            Assert.Equal(urls, repo.DocumentStore.Urls);
            Assert.Equal("ClusterDB", repo.DatabaseName);
        }

        [Fact]
        public void Repository_InitializesDocumentStoreUrls_WithSingleConnectionUrl()
        {
            var config = new RepositoryConfig
            {
                ConnectionUrl = "http://localhost:8080",
                Database = "SingleDB"
            };

            using var repo = new Repository(config);

            Assert.NotNull(repo.DocumentStore);
            Assert.Single(repo.DocumentStore.Urls);
            Assert.Equal("http://localhost:8080", repo.DocumentStore.Urls[0]);
        }

        [Fact]
        public void UseRavenStorage_ThrowsArgumentException_WhenAnyUrlDoesNotStartWithHttp()
        {
            var configurationMock = new Mock<IGlobalConfiguration>();
            var invalidUrls = new[] { "http://node1:8080", "ftp://invalid-node:8080" };

            Assert.Throws<ArgumentException>(() =>
                configurationMock.Object.UseRavenStorage(invalidUrls, "TestDB"));
        }

        [Fact]
        public void UseRavenStorage_ThrowsArgumentException_WhenUrlsEmpty()
        {
            var configurationMock = new Mock<IGlobalConfiguration>();

            Assert.Throws<ArgumentException>(() =>
                configurationMock.Object.UseRavenStorage(Array.Empty<string>(), "TestDB"));
        }
    }
}
