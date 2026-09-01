using System;

namespace Hangfire.Raven.HealthChecks
{
    public class RavenDbHealthCheckOptions
    {
        /// <summary>
        /// Optional database name override. If not specified, the database configured in RavenStorage or IDocumentStore will be used.
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// Gets or sets whether to inspect stale indexes and report degraded status if any exist. Defaults to true.
        /// </summary>
        public bool CheckStaleIndexes { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum number of stale indexes allowed before marking the health check as degraded. Defaults to 0.
        /// </summary>
        public int MaxAllowedStaleIndexes { get; set; } = 0;
    }
}
