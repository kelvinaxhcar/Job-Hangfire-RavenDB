using System.Collections.Generic;

namespace Hangfire.Raven.Dashboard
{
    public class RavenStorageMetricsDto
    {
        public string DatabaseName { get; set; }
        public string DatabaseId { get; set; }
        public long DocumentsCount { get; set; }
        public int IndexesCount { get; set; }
        public int StaleIndexesCount { get; set; }
        public string[] StaleIndexes { get; set; }
        public string SizeOnDisk { get; set; }
        public long CompareExchangeCount { get; set; }
        public List<RavenIndexMetricsDto> Indexes { get; set; } = new List<RavenIndexMetricsDto>();
    }

    public class RavenIndexMetricsDto
    {
        public string Name { get; set; }
        public bool IsStale { get; set; }
        public string State { get; set; }
        public string Type { get; set; }
    }
}
