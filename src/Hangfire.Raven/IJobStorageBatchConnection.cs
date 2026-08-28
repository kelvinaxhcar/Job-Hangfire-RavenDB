using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Raven
{
    public class BatchJobItem
    {
        public Job Job { get; set; }

        public IDictionary<string, string> Parameters { get; set; } = new Dictionary<string, string>();

        public IState InitialState { get; set; }

        public TimeSpan? ExpireIn { get; set; }

        public string JobId { get; set; }
    }

    public interface IJobStorageBatchConnection : IStorageConnection
    {
        List<string> BatchEnqueue(IEnumerable<BatchJobItem> jobs, string queue = "default");

        Task<List<string>> BatchEnqueueAsync(IEnumerable<BatchJobItem> jobs, string queue = "default", CancellationToken cancellationToken = default);
    }
}
