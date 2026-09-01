using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;

namespace Hangfire.Raven.Extensions
{
    public static class DatabaseExtensions
    {
        public static bool DatabaseExists(this IDocumentStore documentStore, string database)
        {
            if (documentStore == null || string.IsNullOrEmpty(database))
                return false;

            try
            {
                GetDatabaseRecordOperation operation = new GetDatabaseRecordOperation(database);
                return documentStore.Maintenance.Server.Send<DatabaseRecordWithEtag>((IServerOperation<DatabaseRecordWithEtag>)operation) != null;
            }
            catch
            {
                return false;
            }
        }
    }
}
