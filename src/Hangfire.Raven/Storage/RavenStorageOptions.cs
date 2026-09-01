using System;
using System.Collections.Generic;
using Microsoft.Extensions.Caching.Memory;
using Polly;
using Polly.Retry;

namespace Hangfire.Raven.Storage
{
    public class RavenStorageOptions
    {
        private readonly string _clientId = null;
        private TimeSpan _queuePollInterval;
        private TimeSpan _distributedLockLifetime;
        private TimeSpan _cacheSlidingExpiration;
        private ResiliencePipeline _retryPolicy;

        public RavenStorageOptions()
        {
            QueuePollInterval = TimeSpan.FromSeconds(15.0);
            InvisibilityTimeout = TimeSpan.FromMinutes(30.0);
            JobExpirationCheckInterval = TimeSpan.FromHours(1.0);
            CountersAggregateInterval = TimeSpan.FromMinutes(5.0);
            TransactionTimeout = TimeSpan.FromMinutes(1.0);
            DistributedLockLifetime = TimeSpan.FromSeconds(30.0);
            CacheSlidingExpiration = TimeSpan.FromSeconds(3.0);
            _clientId = Guid.NewGuid().ToString().Replace("-", string.Empty);
        }

        public TimeSpan QueuePollInterval
        {
            get => _queuePollInterval;
            set
            {
                var message = string.Format("The QueuePollInterval property value should be positive. Given: {0}.", value);
                if (value == TimeSpan.Zero)
                    throw new ArgumentException(message, nameof(value));
                _queuePollInterval = !(value != value.Duration()) ? value : throw new ArgumentException(message, nameof(value));
            }
        }

        public TimeSpan InvisibilityTimeout { get; set; }

        public TimeSpan JobExpirationCheckInterval { get; set; }

        public TimeSpan CountersAggregateInterval { get; set; }

        public TimeSpan TransactionTimeout { get; set; }

        public TimeSpan DistributedLockLifetime
        {
            get => _distributedLockLifetime;
            set
            {
                var message = string.Format("The DistributedLockLifetime property value should be positive. Given: {0}.", value);
                if (value == TimeSpan.Zero)
                    throw new ArgumentException(message, nameof(value));
                _distributedLockLifetime = !(value != value.Duration()) ? value : throw new ArgumentException(message, nameof(value));
            }
        }

        public IEnumerable<string> QueueNames { get; set; }

        public bool EnableJobRevisions { get; set; } = true;

        public int MinimumJobRevisionsToKeep { get; set; } = 50;

        public TimeSpan MinimumJobRevisionAgeToKeep { get; set; } = TimeSpan.FromDays(14);

        public bool PurgeJobRevisionsOnDelete { get; set; } = false;

        public bool EnableChangesApiQueueEvents { get; set; } = true;

        public bool EnableCache { get; set; } = true;

        public TimeSpan CacheSlidingExpiration
        {
            get => _cacheSlidingExpiration;
            set
            {
                var message = string.Format("The CacheSlidingExpiration property value should be positive. Given: {0}.", value);
                if (value == TimeSpan.Zero)
                    throw new ArgumentException(message, nameof(value));
                _cacheSlidingExpiration = !(value != value.Duration()) ? value : throw new ArgumentException(message, nameof(value));
            }
        }

        public IMemoryCache MemoryCache { get; set; }

        /// <summary>
        /// Gets or sets whether automatic Polly retry policy is enabled for transient errors. Defaults to true.
        /// </summary>
        public bool EnableRetryPolicy { get; set; } = true;

        /// <summary>
        /// Gets or sets the maximum number of retry attempts. Defaults to 3.
        /// </summary>
        public int MaxRetryAttempts { get; set; } = 3;

        /// <summary>
        /// Gets or sets the initial retry delay. Defaults to 100ms.
        /// </summary>
        public TimeSpan RetryInitialDelay { get; set; } = TimeSpan.FromMilliseconds(100);

        /// <summary>
        /// Gets or sets the maximum retry delay during exponential backoff. Defaults to 2s.
        /// </summary>
        public TimeSpan RetryMaxDelay { get; set; } = TimeSpan.FromSeconds(2);

        /// <summary>
        /// Gets or sets the custom or default Polly ResiliencePipeline for I/O and SaveChanges operations.
        /// </summary>
        public ResiliencePipeline RetryPolicy
        {
            get
            {
                if (_retryPolicy == null && EnableRetryPolicy)
                {
                    _retryPolicy = CreateDefaultRetryPolicy(MaxRetryAttempts, RetryInitialDelay, RetryMaxDelay);
                }
                return _retryPolicy;
            }
            set => _retryPolicy = value;
        }

        /// <summary>
        /// Factory method to create a default ResiliencePipeline with exponential backoff, jitter, and transient exception handling.
        /// </summary>
        public static ResiliencePipeline CreateDefaultRetryPolicy(
            int maxRetryAttempts = 3,
            TimeSpan? initialDelay = null,
            TimeSpan? maxDelay = null)
        {
            var baseDelay = initialDelay ?? TimeSpan.FromMilliseconds(100);
            var maxBackoff = maxDelay ?? TimeSpan.FromSeconds(2);

            return new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = maxRetryAttempts,
                    Delay = baseDelay,
                    MaxDelay = maxBackoff,
                    BackoffType = DelayBackoffType.Exponential,
                    UseJitter = true,
                    ShouldHandle = new PredicateBuilder()
                        .Handle<global::Raven.Client.Exceptions.ConcurrencyException>()
                        .Handle<global::Raven.Client.Exceptions.RavenException>()
                        .Handle<System.TimeoutException>()
                        .Handle<System.Net.Http.HttpRequestException>()
                        .Handle<System.IO.IOException>()
                })
                .Build();
        }

        public string ClientId => _clientId;
    }
}
