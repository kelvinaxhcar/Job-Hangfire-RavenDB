using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using System;
using System.Collections.Generic;

namespace Hangfire.Raven
{
    public interface IRepository : IDisposable
    {
        string DatabaseName { get; }

        void Create();

        void Destroy();

        void ExecuteIndexes(List<AbstractIndexCreationTask> indexes);

        DatabaseStatistics GetDatabaseStatistics();

        string GetId(Type type, params string[] id);

        IDocumentSession OpenSession();
    }
}
