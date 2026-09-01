using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Storage;

namespace Hangfire.Raven.Extensions
{
    public static class RavenBatchCancellationExtensions
    {
        public static long DeleteJobsByState(this JobStorage storage, string stateName)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (string.IsNullOrEmpty(stateName)) throw new ArgumentNullException(nameof(stateName));

            using var connection = storage.GetConnection();
            if (connection is IBatchJobCancellation batchCancellation)
            {
                return batchCancellation.DeleteByState(stateName);
            }

            throw new NotSupportedException($"Storage connection '{connection.GetType().Name}' does not support IBatchJobCancellation.");
        }

        public static async Task<long> DeleteJobsByStateAsync(this JobStorage storage, string stateName, CancellationToken cancellationToken = default)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (string.IsNullOrEmpty(stateName)) throw new ArgumentNullException(nameof(stateName));

            using var connection = storage.GetConnection();
            if (connection is IBatchJobCancellation batchCancellation)
            {
                return await batchCancellation.DeleteByStateAsync(stateName, cancellationToken);
            }

            throw new NotSupportedException($"Storage connection '{connection.GetType().Name}' does not support IBatchJobCancellation.");
        }

        public static long DeleteJobsByQueue(this JobStorage storage, string queueName)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (string.IsNullOrEmpty(queueName)) throw new ArgumentNullException(nameof(queueName));

            using var connection = storage.GetConnection();
            if (connection is IBatchJobCancellation batchCancellation)
            {
                return batchCancellation.DeleteByQueue(queueName);
            }

            throw new NotSupportedException($"Storage connection '{connection.GetType().Name}' does not support IBatchJobCancellation.");
        }

        public static async Task<long> DeleteJobsByQueueAsync(this JobStorage storage, string queueName, CancellationToken cancellationToken = default)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (string.IsNullOrEmpty(queueName)) throw new ArgumentNullException(nameof(queueName));

            using var connection = storage.GetConnection();
            if (connection is IBatchJobCancellation batchCancellation)
            {
                return await batchCancellation.DeleteByQueueAsync(queueName, cancellationToken);
            }

            throw new NotSupportedException($"Storage connection '{connection.GetType().Name}' does not support IBatchJobCancellation.");
        }

        public static long DeleteJobs(this JobStorage storage, IEnumerable<string> jobIds)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (jobIds == null) throw new ArgumentNullException(nameof(jobIds));

            using var connection = storage.GetConnection();
            if (connection is IBatchJobCancellation batchCancellation)
            {
                return batchCancellation.DeleteJobs(jobIds);
            }

            throw new NotSupportedException($"Storage connection '{connection.GetType().Name}' does not support IBatchJobCancellation.");
        }

        public static async Task<long> DeleteJobsAsync(this JobStorage storage, IEnumerable<string> jobIds, CancellationToken cancellationToken = default)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (jobIds == null) throw new ArgumentNullException(nameof(jobIds));

            using var connection = storage.GetConnection();
            if (connection is IBatchJobCancellation batchCancellation)
            {
                return await batchCancellation.DeleteJobsAsync(jobIds, cancellationToken);
            }

            throw new NotSupportedException($"Storage connection '{connection.GetType().Name}' does not support IBatchJobCancellation.");
        }
    }
}
