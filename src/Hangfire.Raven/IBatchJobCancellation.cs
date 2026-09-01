using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Raven
{
    /// <summary>
    /// Provides high-performance batch job cancellation and deletion operations using RavenDB bulk operations.
    /// </summary>
    public interface IBatchJobCancellation
    {
        /// <summary>
        /// Deletes all jobs in the specified state (e.g. Failed, Succeeded, Deleted, Enqueued, Processing, Scheduled).
        /// Returns the number of deleted job documents.
        /// </summary>
        /// <param name="stateName">The state name (case-sensitive or standard Hangfire state name).</param>
        /// <returns>The total number of deleted jobs.</returns>
        long DeleteByState(string stateName);

        /// <summary>
        /// Asynchronously deletes all jobs in the specified state.
        /// </summary>
        /// <param name="stateName">The state name.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The total number of deleted jobs.</returns>
        Task<long> DeleteByStateAsync(string stateName, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes all jobs belonging to the specified queue and purges pending queue items.
        /// </summary>
        /// <param name="queueName">The name of the queue (e.g. "default").</param>
        /// <returns>The total number of deleted items.</returns>
        long DeleteByQueue(string queueName);

        /// <summary>
        /// Asynchronously deletes all jobs belonging to the specified queue and purges pending queue items.
        /// </summary>
        /// <param name="queueName">The name of the queue.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The total number of deleted items.</returns>
        Task<long> DeleteByQueueAsync(string queueName, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes a collection of specific job IDs in a single batch.
        /// </summary>
        /// <param name="jobIds">Collection of job identifiers.</param>
        /// <returns>The number of deleted jobs.</returns>
        long DeleteJobs(IEnumerable<string> jobIds);

        /// <summary>
        /// Asynchronously deletes a collection of specific job IDs in a single batch.
        /// </summary>
        /// <param name="jobIds">Collection of job identifiers.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The number of deleted jobs.</returns>
        Task<long> DeleteJobsAsync(IEnumerable<string> jobIds, CancellationToken cancellationToken = default);
    }
}
