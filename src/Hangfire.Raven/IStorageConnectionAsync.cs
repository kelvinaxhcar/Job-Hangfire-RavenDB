using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Raven
{
    public interface IStorageConnectionAsync : IStorageConnection
    {
        Task<string> CreateExpiredJobAsync(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn, CancellationToken cancellationToken = default);

        Task<JobData> GetJobDataAsync(string key, CancellationToken cancellationToken = default);

        Task<StateData> GetStateDataAsync(string jobId, CancellationToken cancellationToken = default);

        Task SetJobParameterAsync(string jobId, string name, string value, CancellationToken cancellationToken = default);

        Task<string> GetJobParameterAsync(string jobId, string name, CancellationToken cancellationToken = default);

        Task<HashSet<string>> GetAllItemsFromSetAsync(string key, CancellationToken cancellationToken = default);

        Task<string> GetFirstByLowestScoreFromSetAsync(string key, double fromScore, double toScore, CancellationToken cancellationToken = default);

        Task SetRangeInHashAsync(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs, CancellationToken cancellationToken = default);

        Task<Dictionary<string, string>> GetAllEntriesFromHashAsync(string key, CancellationToken cancellationToken = default);

        Task AnnounceServerAsync(string serverId, ServerContext context, CancellationToken cancellationToken = default);

        Task RemoveServerAsync(string serverId, CancellationToken cancellationToken = default);

        Task HeartbeatAsync(string serverId, CancellationToken cancellationToken = default);

        Task<int> RemoveTimedOutServersAsync(TimeSpan timeOut, CancellationToken cancellationToken = default);

        Task<long> GetSetCountAsync(string key, CancellationToken cancellationToken = default);

        Task<List<string>> GetRangeFromSetAsync(string key, int startingFrom, int endingAt, CancellationToken cancellationToken = default);

        Task<TimeSpan> GetSetTtlAsync(string key, CancellationToken cancellationToken = default);

        Task<long> GetCounterAsync(string key, CancellationToken cancellationToken = default);

        Task<long> GetHashCountAsync(string key, CancellationToken cancellationToken = default);

        Task<TimeSpan> GetHashTtlAsync(string key, CancellationToken cancellationToken = default);

        Task<string> GetValueFromHashAsync(string key, string name, CancellationToken cancellationToken = default);

        Task<long> GetListCountAsync(string key, CancellationToken cancellationToken = default);

        Task<TimeSpan> GetListTtlAsync(string key, CancellationToken cancellationToken = default);

        Task<List<string>> GetRangeFromListAsync(string key, int startingFrom, int endingAt, CancellationToken cancellationToken = default);

        Task<List<string>> GetAllItemsFromListAsync(string key, CancellationToken cancellationToken = default);

        Task<IWriteOnlyTransactionAsync> CreateWriteTransactionAsync(CancellationToken cancellationToken = default);
    }
}
