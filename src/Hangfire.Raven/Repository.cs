using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Raven.Client.Documents;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using System;
using System.Collections.Generic;

namespace Hangfire.Raven
{
    public class Repository : IRepository, IDisposable
    {
        private DocumentStore _documentStore;
        private readonly string _database;

        public Repository(RepositoryConfig config)
        {
            DocumentStore documentStore = new DocumentStore();
            documentStore.Urls = new string[1]
            {
        config.ConnectionUrl
            };
            documentStore.Database = config.Database;
            documentStore.Certificate = config.Certificate;
            _documentStore = documentStore;
            _documentStore.Initialize();
            _database = _documentStore.Database;
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes)
        {
            _documentStore.ExecuteIndexes(indexes);
        }

        public void Destroy()
        {
            if (_database == null || !_documentStore.DatabaseExists(_database))
                return;
            _documentStore.Maintenance.Server.Send(new DeleteDatabasesOperation(_database, true));
        }

        public void Create()
        {
            if (_database == null || _documentStore.DatabaseExists(_database))
                return;
            try
            {
                _documentStore.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(_database)));
            }
            catch (Exception)
            {
            }
            ConfigureExpiration();
        }

        private void ConfigureExpiration()
        {
            _documentStore.Maintenance.Send(new ConfigureExpirationOperation(new ExpirationConfiguration()
            {
                Disabled = false,
                DeleteFrequencyInSec = new long?(60L)
            }));
        }

        public void Dispose() => _documentStore.Dispose();

        public string DatabaseName => _database;

        public IDocumentSession OpenSession(SessionOptions options = null) =>
            options != null ? _documentStore.OpenSession(options) : _documentStore.OpenSession();

        public IAsyncDocumentSession OpenAsyncSession(SessionOptions options = null) =>
            options != null ? _documentStore.OpenAsyncSession(options) : _documentStore.OpenAsyncSession();

        public DatabaseStatistics GetDatabaseStatistics()
        {
            if (_database == null || !_documentStore.DatabaseExists(_database))
                return null;
            return _documentStore.Maintenance.Send(new GetStatisticsOperation());
        }

        public void EnsureRevisionsConfigured(RavenStorageOptions options)
        {
            if (options == null || !options.EnableJobRevisions) return;

            try
            {
                if (_database == null || !_documentStore.DatabaseExists(_database))
                    return;

                var config = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["RavenJobs"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false,
                            PurgeOnDelete = options.PurgeJobRevisionsOnDelete,
                            MinimumRevisionsToKeep = options.MinimumJobRevisionsToKeep,
                            MinimumRevisionAgeToKeep = options.MinimumJobRevisionAgeToKeep
                        }
                    }
                };

                _documentStore.Maintenance.Send(new ConfigureRevisionsOperation(config));
            }
            catch
            {
                // Silently handle if database does not yet exist or user has restricted permissions
            }
        }

        public BulkInsertOperation BulkInsert(string database = null)
        {
            return _documentStore.BulkInsert(database ?? _database);
        }

        public IDocumentStore DocumentStore => _documentStore;

        public string GetId(Type type, params string[] id)
        {
            return type.ToString() + "/" + string.Join("/", id);
        }
    }
}
