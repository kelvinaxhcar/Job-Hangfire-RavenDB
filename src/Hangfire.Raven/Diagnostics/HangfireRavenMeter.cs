using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.Diagnostics
{
    /// <summary>
    /// OpenTelemetry and Prometheus metrics instrumentation for Hangfire.Raven.
    /// Provides standard System.Diagnostics.Metrics instruments and Prometheus text format export.
    /// </summary>
    public static class HangfireRavenMeter
    {
        public const string MeterName = "Hangfire.Raven";
        public const string MeterVersion = "1.0.7";

        public static readonly Meter Meter = new Meter(MeterName, MeterVersion);

        private static readonly object SyncLock = new object();
        private static readonly List<WeakReference<RavenStorage>> RegisteredStorages = new List<WeakReference<RavenStorage>>();

        public static readonly Counter<long> OperationsCounter = Meter.CreateCounter<long>(
            "hangfire.ravendb.operations_total",
            unit: "{operation}",
            description: "Total number of RavenDB storage operations executed.");

        public static readonly Histogram<double> OperationDuration = Meter.CreateHistogram<double>(
            "hangfire.ravendb.operation_duration_seconds",
            unit: "s",
            description: "Duration of RavenDB storage operations in seconds.");

        public static readonly Counter<long> RetryAttemptsCounter = Meter.CreateCounter<long>(
            "hangfire.ravendb.retry_attempts_total",
            unit: "{retry}",
            description: "Total retry attempts performed by Polly resilience policy.");

        static HangfireRavenMeter()
        {
            Meter.CreateObservableGauge(
                "hangfire.ravendb.documents_count",
                GetDocumentCountMeasurements,
                unit: "{document}",
                description: "Total number of documents in RavenDB database.");

            Meter.CreateObservableGauge(
                "hangfire.ravendb.indexes_stale",
                GetStaleIndexMeasurements,
                unit: "{index}",
                description: "Count of stale indexes in RavenDB database.");

            Meter.CreateObservableGauge(
                "hangfire.ravendb.indexes_total",
                GetTotalIndexMeasurements,
                unit: "{index}",
                description: "Total number of indexes in RavenDB database.");

            Meter.CreateObservableGauge(
                "hangfire.ravendb.jobs_count",
                GetJobCountMeasurements,
                unit: "{job}",
                description: "Current number of Hangfire jobs by state.");

            Meter.CreateObservableGauge(
                "hangfire.ravendb.servers_count",
                GetServerCountMeasurements,
                unit: "{server}",
                description: "Number of active Hangfire servers.");

            Meter.CreateObservableGauge(
                "hangfire.ravendb.queues_count",
                GetQueueCountMeasurements,
                unit: "{queue}",
                description: "Number of Hangfire queues.");

            Meter.CreateObservableGauge(
                "hangfire.ravendb.recurring_count",
                GetRecurringCountMeasurements,
                unit: "{job}",
                description: "Number of recurring jobs in Hangfire.");
        }

        public static void RegisterStorage(RavenStorage storage)
        {
            if (storage == null) return;
            lock (SyncLock)
            {
                RegisteredStorages.RemoveAll(wr => !wr.TryGetTarget(out _));
                if (!RegisteredStorages.Any(wr => wr.TryGetTarget(out var target) && ReferenceEquals(target, storage)))
                {
                    RegisteredStorages.Add(new WeakReference<RavenStorage>(storage));
                }
            }
        }

        public static void UnregisterStorage(RavenStorage storage)
        {
            if (storage == null) return;
            lock (SyncLock)
            {
                RegisteredStorages.RemoveAll(wr => !wr.TryGetTarget(out var target) || ReferenceEquals(target, storage));
            }
        }

        public static List<RavenStorage> GetActiveStorages()
        {
            lock (SyncLock)
            {
                var list = new List<RavenStorage>();
                RegisteredStorages.RemoveAll(wr => !wr.TryGetTarget(out _));
                foreach (var wr in RegisteredStorages)
                {
                    if (wr.TryGetTarget(out var storage))
                    {
                        list.Add(storage);
                    }
                }

                if (list.Count == 0)
                {
                    try
                    {
                        if (JobStorage.Current is RavenStorage currentStorage)
                        {
                            list.Add(currentStorage);
                        }
                    }
                    catch (InvalidOperationException)
                    {
                    }
                }

                return list;
            }
        }

        private static IEnumerable<Measurement<long>> GetDocumentCountMeasurements()
        {
            var measurements = new List<Measurement<long>>();
            foreach (var storage in GetActiveStorages())
            {
                try
                {
                    var dbName = storage.Repository?.DatabaseName ?? "default";
                    var stats = storage.Repository?.GetDatabaseStatistics();
                    long count = stats?.CountOfDocuments ?? 0;
                    measurements.Add(new Measurement<long>(count, new KeyValuePair<string, object>("database", dbName)));
                }
                catch
                {
                }
            }
            return measurements;
        }

        private static IEnumerable<Measurement<long>> GetStaleIndexMeasurements()
        {
            var measurements = new List<Measurement<long>>();
            foreach (var storage in GetActiveStorages())
            {
                try
                {
                    var dbName = storage.Repository?.DatabaseName ?? "default";
                    var stats = storage.Repository?.GetDatabaseStatistics();
                    long count = stats?.StaleIndexes?.Length ?? (stats?.Indexes?.Count(i => i.IsStale) ?? 0);
                    measurements.Add(new Measurement<long>(count, new KeyValuePair<string, object>("database", dbName)));
                }
                catch
                {
                }
            }
            return measurements;
        }

        private static IEnumerable<Measurement<long>> GetTotalIndexMeasurements()
        {
            var measurements = new List<Measurement<long>>();
            foreach (var storage in GetActiveStorages())
            {
                try
                {
                    var dbName = storage.Repository?.DatabaseName ?? "default";
                    var stats = storage.Repository?.GetDatabaseStatistics();
                    long count = stats?.CountOfIndexes ?? 0;
                    measurements.Add(new Measurement<long>(count, new KeyValuePair<string, object>("database", dbName)));
                }
                catch
                {
                }
            }
            return measurements;
        }

        private static IEnumerable<Measurement<long>> GetJobCountMeasurements()
        {
            var measurements = new List<Measurement<long>>();
            foreach (var storage in GetActiveStorages())
            {
                try
                {
                    var dbName = storage.Repository?.DatabaseName ?? "default";
                    var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                    var stats = monitoringApi?.GetStatistics();
                    if (stats != null)
                    {
                        measurements.Add(new Measurement<long>(stats.Enqueued, new KeyValuePair<string, object>("database", dbName), new KeyValuePair<string, object>("state", "enqueued")));
                        measurements.Add(new Measurement<long>(stats.Processing, new KeyValuePair<string, object>("database", dbName), new KeyValuePair<string, object>("state", "processing")));
                        measurements.Add(new Measurement<long>(stats.Succeeded, new KeyValuePair<string, object>("database", dbName), new KeyValuePair<string, object>("state", "succeeded")));
                        measurements.Add(new Measurement<long>(stats.Failed, new KeyValuePair<string, object>("database", dbName), new KeyValuePair<string, object>("state", "failed")));
                        measurements.Add(new Measurement<long>(stats.Scheduled, new KeyValuePair<string, object>("database", dbName), new KeyValuePair<string, object>("state", "scheduled")));
                        measurements.Add(new Measurement<long>(stats.Deleted, new KeyValuePair<string, object>("database", dbName), new KeyValuePair<string, object>("state", "deleted")));
                        measurements.Add(new Measurement<long>(stats.Awaiting ?? 0, new KeyValuePair<string, object>("database", dbName), new KeyValuePair<string, object>("state", "awaiting")));
                    }
                }
                catch
                {
                }
            }
            return measurements;
        }

        private static IEnumerable<Measurement<long>> GetServerCountMeasurements()
        {
            var measurements = new List<Measurement<long>>();
            foreach (var storage in GetActiveStorages())
            {
                try
                {
                    var dbName = storage.Repository?.DatabaseName ?? "default";
                    var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                    var servers = monitoringApi?.Servers();
                    var count = servers?.Count ?? 0;
                    measurements.Add(new Measurement<long>(count, new KeyValuePair<string, object>("database", dbName)));
                }
                catch
                {
                }
            }
            return measurements;
        }

        private static IEnumerable<Measurement<long>> GetQueueCountMeasurements()
        {
            var measurements = new List<Measurement<long>>();
            foreach (var storage in GetActiveStorages())
            {
                try
                {
                    var dbName = storage.Repository?.DatabaseName ?? "default";
                    var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                    var queues = monitoringApi?.Queues();
                    var count = queues?.Count ?? 0;
                    measurements.Add(new Measurement<long>(count, new KeyValuePair<string, object>("database", dbName)));
                }
                catch
                {
                }
            }
            return measurements;
        }

        private static IEnumerable<Measurement<long>> GetRecurringCountMeasurements()
        {
            var measurements = new List<Measurement<long>>();
            foreach (var storage in GetActiveStorages())
            {
                try
                {
                    var dbName = storage.Repository?.DatabaseName ?? "default";
                    var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                    var stats = monitoringApi?.GetStatistics();
                    var count = stats?.Recurring ?? 0;
                    measurements.Add(new Measurement<long>(count, new KeyValuePair<string, object>("database", dbName)));
                }
                catch
                {
                }
            }
            return measurements;
        }

        /// <summary>
        /// Generates Prometheus exposition format (text/plain; version=0.0.4) metrics string.
        /// </summary>
        public static string GeneratePrometheusMetricsText(RavenStorage storage = null)
        {
            var storages = storage != null ? new List<RavenStorage> { storage } : GetActiveStorages();
            var sb = new StringBuilder();

            sb.AppendLine("# HELP hangfire_ravendb_documents_count Total number of documents in RavenDB database.");
            sb.AppendLine("# TYPE hangfire_ravendb_documents_count gauge");
            foreach (var s in storages)
            {
                var db = s.Repository?.DatabaseName ?? "default";
                long count = 0;
                try { count = s.Repository?.GetDatabaseStatistics()?.CountOfDocuments ?? 0; } catch { }
                sb.AppendLine($"hangfire_ravendb_documents_count{{database=\"{EscapePrometheus(db)}\"}} {count}");
            }
            sb.AppendLine();

            sb.AppendLine("# HELP hangfire_ravendb_indexes_stale Count of stale indexes in RavenDB database.");
            sb.AppendLine("# TYPE hangfire_ravendb_indexes_stale gauge");
            foreach (var s in storages)
            {
                var db = s.Repository?.DatabaseName ?? "default";
                long stale = 0;
                try
                {
                    var stats = s.Repository?.GetDatabaseStatistics();
                    stale = stats?.StaleIndexes?.Length ?? (stats?.Indexes?.Count(i => i.IsStale) ?? 0);
                }
                catch { }
                sb.AppendLine($"hangfire_ravendb_indexes_stale{{database=\"{EscapePrometheus(db)}\"}} {stale}");
            }
            sb.AppendLine();

            sb.AppendLine("# HELP hangfire_ravendb_indexes_total Total number of indexes in RavenDB database.");
            sb.AppendLine("# TYPE hangfire_ravendb_indexes_total gauge");
            foreach (var s in storages)
            {
                var db = s.Repository?.DatabaseName ?? "default";
                long totalIndexes = 0;
                try { totalIndexes = s.Repository?.GetDatabaseStatistics()?.CountOfIndexes ?? 0; } catch { }
                sb.AppendLine($"hangfire_ravendb_indexes_total{{database=\"{EscapePrometheus(db)}\"}} {totalIndexes}");
            }
            sb.AppendLine();

            sb.AppendLine("# HELP hangfire_ravendb_jobs_count Current number of Hangfire jobs by state.");
            sb.AppendLine("# TYPE hangfire_ravendb_jobs_count gauge");
            foreach (var s in storages)
            {
                var db = s.Repository?.DatabaseName ?? "default";
                var monitoringApi = s.GetMonitoringApi() as RavenStorageMonitoringApi;
                var stats = monitoringApi?.GetStatistics();
                if (stats != null)
                {
                    sb.AppendLine($"hangfire_ravendb_jobs_count{{database=\"{EscapePrometheus(db)}\",state=\"enqueued\"}} {stats.Enqueued}");
                    sb.AppendLine($"hangfire_ravendb_jobs_count{{database=\"{EscapePrometheus(db)}\",state=\"processing\"}} {stats.Processing}");
                    sb.AppendLine($"hangfire_ravendb_jobs_count{{database=\"{EscapePrometheus(db)}\",state=\"succeeded\"}} {stats.Succeeded}");
                    sb.AppendLine($"hangfire_ravendb_jobs_count{{database=\"{EscapePrometheus(db)}\",state=\"failed\"}} {stats.Failed}");
                    sb.AppendLine($"hangfire_ravendb_jobs_count{{database=\"{EscapePrometheus(db)}\",state=\"scheduled\"}} {stats.Scheduled}");
                    sb.AppendLine($"hangfire_ravendb_jobs_count{{database=\"{EscapePrometheus(db)}\",state=\"deleted\"}} {stats.Deleted}");
                    sb.AppendLine($"hangfire_ravendb_jobs_count{{database=\"{EscapePrometheus(db)}\",state=\"awaiting\"}} {stats.Awaiting ?? 0}");
                }
            }
            sb.AppendLine();

            sb.AppendLine("# HELP hangfire_ravendb_servers_count Number of active Hangfire servers.");
            sb.AppendLine("# TYPE hangfire_ravendb_servers_count gauge");
            foreach (var s in storages)
            {
                var db = s.Repository?.DatabaseName ?? "default";
                var monitoringApi = s.GetMonitoringApi() as RavenStorageMonitoringApi;
                var count = monitoringApi?.Servers()?.Count ?? 0;
                sb.AppendLine($"hangfire_ravendb_servers_count{{database=\"{EscapePrometheus(db)}\"}} {count}");
            }
            sb.AppendLine();

            sb.AppendLine("# HELP hangfire_ravendb_recurring_count Number of recurring jobs.");
            sb.AppendLine("# TYPE hangfire_ravendb_recurring_count gauge");
            foreach (var s in storages)
            {
                var db = s.Repository?.DatabaseName ?? "default";
                var monitoringApi = s.GetMonitoringApi() as RavenStorageMonitoringApi;
                var stats = monitoringApi?.GetStatistics();
                var count = stats?.Recurring ?? 0;
                sb.AppendLine($"hangfire_ravendb_recurring_count{{database=\"{EscapePrometheus(db)}\"}} {count}");
            }
            sb.AppendLine();

            sb.AppendLine("# HELP hangfire_ravendb_queues_count Number of Hangfire queues.");
            sb.AppendLine("# TYPE hangfire_ravendb_queues_count gauge");
            foreach (var s in storages)
            {
                var db = s.Repository?.DatabaseName ?? "default";
                var monitoringApi = s.GetMonitoringApi() as RavenStorageMonitoringApi;
                var count = monitoringApi?.Queues()?.Count ?? 0;
                sb.AppendLine($"hangfire_ravendb_queues_count{{database=\"{EscapePrometheus(db)}\"}} {count}");
            }

            return sb.ToString();
        }

        private static string EscapePrometheus(string value)
        {
            if (string.IsNullOrEmpty(value)) return string.Empty;
            return value.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n");
        }
    }
}
