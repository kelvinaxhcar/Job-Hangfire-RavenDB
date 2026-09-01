using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Hangfire.Raven.Diagnostics;
using Hangfire.Raven.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenOpenTelemetryMetricsFacts : TesteBase
    {
        public RavenOpenTelemetryMetricsFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void HangfireRavenMeter_HasCorrectMeterNameAndVersion()
        {
            Assert.Equal("Hangfire.Raven", HangfireRavenMeter.MeterName);
            Assert.Equal("1.0.7", HangfireRavenMeter.MeterVersion);
            Assert.NotNull(HangfireRavenMeter.Meter);
            Assert.Equal("Hangfire.Raven", HangfireRavenMeter.Meter.Name);
        }

        [Fact]
        public void HangfireRavenMeter_Instruments_AreDefined()
        {
            Assert.NotNull(HangfireRavenMeter.OperationsCounter);
            Assert.NotNull(HangfireRavenMeter.OperationDuration);
            Assert.NotNull(HangfireRavenMeter.RetryAttemptsCounter);

            HangfireRavenMeter.OperationsCounter.Add(1);
            HangfireRavenMeter.RetryAttemptsCounter.Add(1);
            HangfireRavenMeter.OperationDuration.Record(0.045);
        }

        [Fact]
        public void HangfireRavenMeter_RegisterAndUnregisterStorage_WorksProperly()
        {
            var options = new RavenStorageOptions();
            var storage = new RavenStorage(_store, options);

            var activeStorages = HangfireRavenMeter.GetActiveStorages();
            Assert.Contains(storage, activeStorages);

            storage.Dispose();
            activeStorages = HangfireRavenMeter.GetActiveStorages();
            Assert.DoesNotContain(storage, activeStorages);
        }

        [Fact]
        public void GeneratePrometheusMetricsText_ReturnsValidPrometheusFormat()
        {
            var storage = new RavenStorage(_store);

            var metricsText = HangfireRavenMeter.GeneratePrometheusMetricsText(storage);

            Assert.NotNull(metricsText);
            Assert.Contains("# HELP hangfire_ravendb_documents_count", metricsText);
            Assert.Contains("# TYPE hangfire_ravendb_documents_count gauge", metricsText);
            Assert.Contains("hangfire_ravendb_documents_count{database=", metricsText);

            Assert.Contains("# HELP hangfire_ravendb_indexes_stale", metricsText);
            Assert.Contains("# TYPE hangfire_ravendb_indexes_stale gauge", metricsText);

            Assert.Contains("# HELP hangfire_ravendb_jobs_count", metricsText);
            Assert.Contains("# TYPE hangfire_ravendb_jobs_count gauge", metricsText);
            Assert.Contains("state=\"enqueued\"", metricsText);
            Assert.Contains("state=\"processing\"", metricsText);
            Assert.Contains("state=\"succeeded\"", metricsText);
            Assert.Contains("state=\"failed\"", metricsText);

            Assert.Contains("# HELP hangfire_ravendb_servers_count", metricsText);
            Assert.Contains("# HELP hangfire_ravendb_queues_count", metricsText);
            Assert.Contains("# HELP hangfire_ravendb_recurring_count", metricsText);

            storage.Dispose();
        }

        [Fact]
        public void MeterListener_CanRecordObservableGauges()
        {
            var storage = new RavenStorage(_store);
            var recordedValues = 0;

            using var listener = new MeterListener();
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == HangfireRavenMeter.MeterName)
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
            {
                recordedValues++;
            });
            listener.Start();

            listener.RecordObservableInstruments();

            Assert.True(recordedValues > 0);

            storage.Dispose();
        }
    }
}
