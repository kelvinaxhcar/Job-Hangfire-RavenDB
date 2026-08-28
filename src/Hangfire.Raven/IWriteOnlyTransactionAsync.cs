using Hangfire.States;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Raven
{
    public interface IWriteOnlyTransactionAsync : IWriteOnlyTransaction
    {
        Task CommitAsync(CancellationToken cancellationToken = default);

        Task ExpireJobAsync(string jobId, TimeSpan expireIn, CancellationToken cancellationToken = default);

        Task PersistJobAsync(string jobId, CancellationToken cancellationToken = default);

        Task SetJobStateAsync(string jobId, IState state, CancellationToken cancellationToken = default);

        Task AddJobStateAsync(string jobId, IState state, CancellationToken cancellationToken = default);

        Task AddToQueueAsync(string queue, string jobId, CancellationToken cancellationToken = default);

        Task IncrementCounterAsync(string key, CancellationToken cancellationToken = default);

        Task IncrementCounterAsync(string key, TimeSpan expireIn, CancellationToken cancellationToken = default);

        Task DecrementCounterAsync(string key, CancellationToken cancellationToken = default);

        Task DecrementCounterAsync(string key, TimeSpan expireIn, CancellationToken cancellationToken = default);

        Task AddToSetAsync(string key, string value, CancellationToken cancellationToken = default);

        Task AddToSetAsync(string key, string value, double score, CancellationToken cancellationToken = default);

        Task RemoveFromSetAsync(string key, string value, CancellationToken cancellationToken = default);

        Task InsertToListAsync(string key, string value, CancellationToken cancellationToken = default);

        Task RemoveFromListAsync(string key, string value, CancellationToken cancellationToken = default);

        Task TrimListAsync(string key, int keepStartingFrom, int keepEndingAt, CancellationToken cancellationToken = default);

        Task SetRangeInHashAsync(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs, CancellationToken cancellationToken = default);

        Task RemoveHashAsync(string key, CancellationToken cancellationToken = default);
    }
}
