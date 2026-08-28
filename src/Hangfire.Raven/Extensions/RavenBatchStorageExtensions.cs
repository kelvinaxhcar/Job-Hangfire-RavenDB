using Hangfire.Common;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Raven.Extensions
{
    public static class RavenBatchStorageExtensions
    {
        public static List<string> BulkEnqueue(this JobStorage storage, IEnumerable<Expression<Action>> jobExpressions, string queue = "default")
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (jobExpressions == null) throw new ArgumentNullException(nameof(jobExpressions));

            var batchItems = jobExpressions.Select(expr => new BatchJobItem
            {
                Job = Job.FromExpression(expr)
            }).ToList();

            using var connection = storage.GetConnection();
            if (connection is IJobStorageBatchConnection batchConnection)
            {
                return batchConnection.BatchEnqueue(batchItems, queue);
            }

            using var transaction = connection.CreateWriteTransaction();
            var jobIds = new List<string>();
            foreach (var item in batchItems)
            {
                var id = connection.CreateExpiredJob(item.Job, item.Parameters, DateTime.UtcNow, TimeSpan.FromDays(1));
                transaction.SetJobState(id, new States.EnqueuedState(queue));
                transaction.Commit();
                jobIds.Add(id);
            }
            return jobIds;
        }

        public static List<string> BulkEnqueue(this JobStorage storage, IEnumerable<Job> jobs, string queue = "default")
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (jobs == null) throw new ArgumentNullException(nameof(jobs));

            var batchItems = jobs.Select(j => new BatchJobItem { Job = j }).ToList();

            using var connection = storage.GetConnection();
            if (connection is IJobStorageBatchConnection batchConnection)
            {
                return batchConnection.BatchEnqueue(batchItems, queue);
            }

            using var transaction = connection.CreateWriteTransaction();
            var jobIds = new List<string>();
            foreach (var item in batchItems)
            {
                var id = connection.CreateExpiredJob(item.Job, item.Parameters, DateTime.UtcNow, TimeSpan.FromDays(1));
                transaction.SetJobState(id, new States.EnqueuedState(queue));
                transaction.Commit();
                jobIds.Add(id);
            }
            return jobIds;
        }
    }
}
