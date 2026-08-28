using System;
using System.Collections.Generic;

namespace Hangfire.Raven.Dashboard
{
    public class RavenJobRevisionDto
    {
        public string Id { get; set; }

        public string StateName { get; set; }

        public string Reason { get; set; }

        public DateTime? Timestamp { get; set; }

        public Dictionary<string, string> StateData { get; set; } = new Dictionary<string, string>();

        public string ChangeVector { get; set; }
    }
}
