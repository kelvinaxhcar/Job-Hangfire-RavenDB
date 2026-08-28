using Hangfire.Raven.Extensions;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Embedded;
using System;
using System.Collections.Generic;

namespace Hangfire.Raven.Tests
{
    public class TestRepository : IRepository
    {

        private readonly IDocumentStore _documentStore;
        private readonly IDocumentSession _documentSession;

        public TestRepository(IDocumentSession documentSession)
        {
            if (documentSession != null)
            {
                _documentSession = documentSession;
                _documentStore = _documentSession.Advanced.DocumentStore;

                var urls = _documentStore.Urls;       // Array de URLs
                var database = _documentStore.Database; // Nome do banco de dados

                _documentStore = new DocumentStore
                {
                    Urls = urls,
                    Database = database
                };
                _documentStore.Initialize();
            }
        }

        public void Create()
        {
        }

        public void Destroy()
        {
        }

        public void Dispose()
        {
            _documentStore.Dispose();
        }

        public void ExecuteIndexes(List<AbstractIndexCreationTask> indexes)
        {
            _documentStore.ExecuteIndexes(indexes, null);
        }

        public string DatabaseName => _documentStore?.Database;

        public DatabaseStatistics GetDatabaseStatistics()
        {
            return _documentStore?.Maintenance?.Send(new GetStatisticsOperation());
        }

        public void EnsureRevisionsConfigured(Hangfire.Raven.Storage.RavenStorageOptions options)
        {
        }

        public string GetId(Type type, params string[] id)
        {
            return type.ToString() + "/" + string.Join("/", id);
        }

        public IDocumentSession OpenSession(SessionOptions options = null)
        {
            return options != null ? _documentStore.OpenSession(options) : _documentStore.OpenSession();
        }
    }
}
