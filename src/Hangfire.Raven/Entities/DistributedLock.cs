using System;

namespace Hangfire.Raven.Entities
{
    public class DistributedLock
    {
        public string Id => "DistributedLocks/" + this.Resource;

        public string Resource { get; set; }

        public string ClientId { get; set; }

        public DateTime? AcquiredAt { get; set; }

        public DateTime? ExpiresAt { get; set; }
    }
}
