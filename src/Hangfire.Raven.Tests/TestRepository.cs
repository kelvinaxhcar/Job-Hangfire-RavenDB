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

        public TestRepository()
        {
            EmbeddedServer.Instance.StartServer();

            _documentStore = EmbeddedServer.Instance.GetDocumentStore("Embedded");
            _documentStore.Operations.Send(new DeleteByQueryOperation(new IndexQuery
            {
                Query = "from @all_docs"
            }));
            _documentStore.Initialize();
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

        public string GetId(Type type, params string[] id)
        {
            return type.ToString() + "/" + string.Join("/", id);
        }

        public IDocumentSession OpenSession()
        {
            return _documentStore.OpenSession();
        }

    }
}
