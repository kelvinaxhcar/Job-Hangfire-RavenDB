using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Raven.DistributedLocks;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenDistributedLockEventWaitHandleFacts : TesteBase
    {
        public RavenDistributedLockEventWaitHandleFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void AcquireAndRelease_WorksProperly_WithCompareExchange()
        {
            UseStorage(storage =>
            {
                using (var connection = storage.GetConnection())
                {
                    using (var dLock = connection.AcquireDistributedLock("res-test-1", TimeSpan.FromSeconds(5)))
                    {
                        Assert.NotNull(dLock);
                    }

                    // After release, should be immediately acquirable again
                    using (var dLock2 = connection.AcquireDistributedLock("res-test-1", TimeSpan.FromSeconds(5)))
                    {
                        Assert.NotNull(dLock2);
                    }
                }
            });
        }

        [Fact]
        public void Acquire_WhenLockedByAnotherThread_TimesOutCleanlyWithoutLeak()
        {
            UseStorage(storage =>
            {
                var options1 = new RavenStorageOptions { ClientId = "client-A" };
                var options2 = new RavenStorageOptions { ClientId = "client-B" };

                using (new RavenDistributedLock(storage, "contested-res", TimeSpan.FromSeconds(5), options1))
                {
                    var task = Task.Run(() =>
                    {
                        Assert.Throws<DistributedLockTimeoutException>(() =>
                            new RavenDistributedLock(storage, "contested-res", TimeSpan.FromMilliseconds(400), options2));
                    });

                    task.Wait(TimeSpan.FromSeconds(5));
                }
            });
        }

        [Fact]
        public void Acquire_WhenLockReleased_AcquiresSuccessfullyViaSignal()
        {
            UseStorage(storage =>
            {
                var options1 = new RavenStorageOptions { ClientId = "client-1" };
                var options2 = new RavenStorageOptions { ClientId = "client-2" };

                var releaseEvent = new ManualResetEventSlim(false);

                var holderTask = Task.Run(() =>
                {
                    using (new RavenDistributedLock(storage, "signal-res", TimeSpan.FromSeconds(5), options1))
                    {
                        releaseEvent.Set();
                        Thread.Sleep(TimeSpan.FromMilliseconds(500));
                    }
                });

                releaseEvent.Wait(TimeSpan.FromSeconds(3));

                // Client 2 attempts to acquire with enough timeout to wait for release
                using (var lock2 = new RavenDistributedLock(storage, "signal-res", TimeSpan.FromSeconds(5), options2))
                {
                    Assert.NotNull(lock2);
                }

                holderTask.Wait(TimeSpan.FromSeconds(5));
            });
        }
    }
}
