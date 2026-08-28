using Hangfire.Logging;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Extensions;
using Hangfire.Raven.Storage;
using Hangfire.Storage;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Hangfire.Raven.DistributedLocks
{
    public class RavenDistributedLock : IDisposable
    {
        private static readonly ILog Logger = LogProvider.For<RavenDistributedLock>();
        private static readonly ThreadLocal<Dictionary<string, int>> AcquiredLocks = new ThreadLocal<Dictionary<string, int>>(() => new Dictionary<string, int>());
        private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromMinutes(1.0);
        private readonly RavenStorage _storage;
        private readonly string _resource;
        private readonly RavenStorageOptions _options;
        private DistributedLock _distributedLock;
        private Timer _heartbeatTimer;
        private bool _completed;
        private readonly object _lockObject = new object();

        private string LockKey => _storage.Repository.GetId(typeof(DistributedLock), _resource);
        private string EventWaitHandleName => GetType().FullName + "." + _resource;

        public RavenDistributedLock(
          RavenStorage storage,
          string resource,
          TimeSpan timeout,
          RavenStorageOptions options)
        {
            storage.ThrowIfNull(nameof(storage));
            if (string.IsNullOrEmpty(resource))
                throw new ArgumentNullException(nameof(resource));
            if (timeout.TotalSeconds > (double)int.MaxValue)
                throw new ArgumentException(string.Format("The timeout specified is too large. Please supply a timeout equal to or less than {0} seconds", int.MaxValue), nameof(timeout));
            options.ThrowIfNull(nameof(options));
            _storage = storage;
            _resource = resource;
            _options = options;
            if (!AcquiredLocks.Value.ContainsKey(_resource) || AcquiredLocks.Value[_resource] == 0)
            {
                Acquire(timeout);
                AcquiredLocks.Value[_resource] = 1;
                StartHeartBeat();
            }
            else
            {
                AcquiredLocks.Value[_resource]++;
            }
        }

        public void Dispose()
        {
            if (_completed)
                return;
            _completed = true;
            if (!AcquiredLocks.Value.ContainsKey(_resource))
                return;
            AcquiredLocks.Value[_resource]--;
            if (AcquiredLocks.Value[_resource] > 0)
                return;
            lock (_lockObject)
            {
                AcquiredLocks.Value.Remove(_resource);
                if (_heartbeatTimer != null)
                {
                    _heartbeatTimer.Dispose();
                    _heartbeatTimer = null;
                }
                Release();
            }
        }

        private void Acquire(TimeSpan timeout)
        {
            try
            {
                DateTime deadline = DateTime.UtcNow.Add(timeout);
                int millisecondsTimeout = timeout.TotalMilliseconds > 10000.0 ? 2000 : Math.Max(50, (int)(timeout.TotalMilliseconds / 5.0));

                while (true)
                {
                    using (IDocumentSession session = _storage.Repository.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var existingLock = session.Advanced.ClusterTransaction.GetCompareExchangeValue<DistributedLock>(LockKey);

                        if (existingLock == null || existingLock.Value == null)
                        {
                            var lockValue = new DistributedLock
                            {
                                ClientId = _storage.Options.ClientId,
                                Resource = _resource,
                                AcquiredAt = DateTime.UtcNow,
                                ExpiresAt = DateTime.UtcNow.Add(_options.DistributedLockLifetime)
                            };

                            session.Advanced.ClusterTransaction.CreateCompareExchangeValue(LockKey, lockValue);

                            try
                            {
                                session.SaveChanges();
                                _distributedLock = lockValue;
                                return;
                            }
                            catch (ConcurrencyException)
                            {
                                // Another node/thread claimed the compare exchange value concurrently
                            }
                        }
                        else if (existingLock.Value.ExpiresAt.HasValue && existingLock.Value.ExpiresAt.Value < DateTime.UtcNow)
                        {
                            // Lock has expired, atomically take ownership
                            existingLock.Value.ClientId = _storage.Options.ClientId;
                            existingLock.Value.AcquiredAt = DateTime.UtcNow;
                            existingLock.Value.ExpiresAt = DateTime.UtcNow.Add(_options.DistributedLockLifetime);

                            try
                            {
                                session.SaveChanges();
                                _distributedLock = existingLock.Value;
                                return;
                            }
                            catch (ConcurrencyException)
                            {
                                // Another node claimed the expired compare exchange value concurrently
                            }
                        }

                        if (DateTime.UtcNow >= deadline)
                        {
                            break;
                        }

                        try
                        {
                            new EventWaitHandle(false, EventResetMode.AutoReset, EventWaitHandleName).WaitOne(millisecondsTimeout);
                        }
                        catch (PlatformNotSupportedException)
                        {
                            Thread.Sleep(millisecondsTimeout);
                        }
                    }
                }

                throw new DistributedLockTimeoutException(_resource);
            }
            catch (DistributedLockTimeoutException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new RavenDistributedLockException("Could not place a lock on the resource '" + _resource + "': Check inner exception for details.", ex);
            }
        }

        private void Release()
        {
            try
            {
                if (_distributedLock != null)
                {
                    using (IDocumentSession documentSession = _storage.Repository.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        var cmpXchg = documentSession.Advanced.ClusterTransaction.GetCompareExchangeValue<DistributedLock>(LockKey);
                        if (cmpXchg != null && cmpXchg.Value != null && cmpXchg.Value.ClientId == _storage.Options.ClientId)
                        {
                            documentSession.Advanced.ClusterTransaction.DeleteCompareExchangeValue(cmpXchg.Key, cmpXchg.Index);
                            documentSession.SaveChanges();
                        }
                        _distributedLock = null;
                    }
                }

                try
                {
                    if (EventWaitHandle.TryOpenExisting(EventWaitHandleName, out EventWaitHandle result))
                    {
                        result.Set();
                    }
                }
                catch (PlatformNotSupportedException)
                {
                }
            }
            catch (Exception ex)
            {
                _distributedLock = null;
                throw new RavenDistributedLockException("Could not release a lock on the resource '" + _resource + "': Check inner exception for details.", ex);
            }
        }

        private void StartHeartBeat()
        {
            Logger.InfoFormat(".Starting heartbeat for resource: {0}", _resource);
            _heartbeatTimer = new Timer(state =>
            {
                lock (_lockObject)
                {
                    if (_completed)
                        return;

                    try
                    {
                        Logger.InfoFormat("..Heartbeat for resource {0}", _resource);
                        using var session = _storage.Repository.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide });
                        var cmpXchg = session.Advanced.ClusterTransaction.GetCompareExchangeValue<DistributedLock>(LockKey);
                        if (cmpXchg != null && cmpXchg.Value != null && cmpXchg.Value.ClientId == _storage.Options.ClientId)
                        {
                            cmpXchg.Value.ExpiresAt = DateTime.UtcNow.Add(_options.DistributedLockLifetime);
                            session.SaveChanges();
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.ErrorFormat("...Unable to update heartbeat on the resource '{0}'. {1}", _resource, ex);
                        Release();
                    }
                }
            }, null, KeepAliveInterval, KeepAliveInterval);
        }
    }
}
