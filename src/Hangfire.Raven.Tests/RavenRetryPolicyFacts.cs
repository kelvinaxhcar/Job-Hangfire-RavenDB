using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Hangfire.Raven.Storage;
using Polly;
using Polly.Retry;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenRetryPolicyFacts : TesteBase
    {
        public RavenRetryPolicyFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void RavenStorageOptions_DefaultRetryPolicy_IsEnabledByDefault()
        {
            var options = new RavenStorageOptions();

            Assert.True(options.EnableRetryPolicy);
            Assert.Equal(3, options.MaxRetryAttempts);
            Assert.Equal(TimeSpan.FromMilliseconds(100), options.RetryInitialDelay);
            Assert.Equal(TimeSpan.FromSeconds(2), options.RetryMaxDelay);
            Assert.NotNull(options.RetryPolicy);
        }

        [Fact]
        public void RavenStorageOptions_WhenEnableRetryPolicyIsFalse_RetryPolicyIsNull()
        {
            var options = new RavenStorageOptions
            {
                EnableRetryPolicy = false
            };

            Assert.Null(options.RetryPolicy);
        }

        [Fact]
        public void RavenStorageOptions_CustomRetryPolicy_CanBeAssigned()
        {
            var customPolicy = new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = 5
                })
                .Build();

            var options = new RavenStorageOptions
            {
                RetryPolicy = customPolicy
            };

            Assert.Same(customPolicy, options.RetryPolicy);
        }

        [Fact]
        public void RetryPolicy_RetriesOnTransientExceptions()
        {
            var attempts = 0;
            var policy = RavenStorageOptions.CreateDefaultRetryPolicy(maxRetryAttempts: 3, initialDelay: TimeSpan.FromMilliseconds(10));

            policy.Execute(() =>
            {
                attempts++;
                if (attempts < 3)
                {
                    throw new HttpRequestException("Transient network issue");
                }
            });

            Assert.Equal(3, attempts);
        }

        [Fact]
        public void RetryPolicy_RetriesOnRavenExceptions()
        {
            var attempts = 0;
            var policy = RavenStorageOptions.CreateDefaultRetryPolicy(maxRetryAttempts: 3, initialDelay: TimeSpan.FromMilliseconds(10));

            policy.Execute(() =>
            {
                attempts++;
                if (attempts < 2)
                {
                    throw new RavenException("Transient RavenDB error");
                }
            });

            Assert.Equal(2, attempts);
        }

        [Fact]
        public async Task RetryPolicy_Async_RetriesOnTransientExceptions()
        {
            var attempts = 0;
            var policy = RavenStorageOptions.CreateDefaultRetryPolicy(maxRetryAttempts: 3, initialDelay: TimeSpan.FromMilliseconds(10));

            await policy.ExecuteAsync(async ct =>
            {
                attempts++;
                await Task.Yield();
                if (attempts < 3)
                {
                    throw new IOException("Transient IO error");
                }
            });

            Assert.Equal(3, attempts);
        }
    }
}
