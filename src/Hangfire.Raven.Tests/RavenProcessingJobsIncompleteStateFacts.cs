using System;
using System.Collections.Generic;
using Hangfire.Common;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenProcessingJobsIncompleteStateFacts : TesteBase
    {
        public RavenProcessingJobsIncompleteStateFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void ProcessingJobs_WhenStateDataIsNull_DoesNotThrowAndReturnsUnknownServer()
        {
            UseStorage(storage =>
            {
                using (var session = storage.Repository.OpenSession())
                {
                    var job = new RavenJob
                    {
                        Id = storage.Repository.GetId(typeof(RavenJob), "job-incomplete-1"),
                        CreatedAt = DateTime.UtcNow,
                        InvocationData = InvocationData.SerializeJob(Job.FromExpression(() => SampleMethod())),
                        StateData = new StateData
                        {
                            Name = ProcessingState.StateName,
                            Data = null
                        }
                    };
                    session.Store(job);
                    session.SaveChanges();
                }

                var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                Assert.NotNull(monitoringApi);

                var processingJobs = monitoringApi.ProcessingJobs(0, 10);
                Assert.NotNull(processingJobs);
                Assert.NotEmpty(processingJobs);

                var first = processingJobs[0].Value;
                Assert.Equal("Unknown", first.ServerId);
                Assert.Null(first.StartedAt);
            });
        }

        [Fact]
        public void ProcessingJobs_WhenStateDataHasNoServerIdOrServerName_ReturnsUnknownServer()
        {
            UseStorage(storage =>
            {
                using (var session = storage.Repository.OpenSession())
                {
                    var job = new RavenJob
                    {
                        Id = storage.Repository.GetId(typeof(RavenJob), "job-incomplete-2"),
                        CreatedAt = DateTime.UtcNow,
                        InvocationData = InvocationData.SerializeJob(Job.FromExpression(() => SampleMethod())),
                        StateData = new StateData
                        {
                            Name = ProcessingState.StateName,
                            Data = new Dictionary<string, string>
                            {
                                ["StartedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow)
                            }
                        }
                    };
                    session.Store(job);
                    session.SaveChanges();
                }

                var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                Assert.NotNull(monitoringApi);

                var processingJobs = monitoringApi.ProcessingJobs(0, 10);
                Assert.NotNull(processingJobs);
                Assert.NotEmpty(processingJobs);

                var first = processingJobs[0].Value;
                Assert.Equal("Unknown", first.ServerId);
                Assert.NotNull(first.StartedAt);
            });
        }

        [Fact]
        public void ProcessingJobs_WhenStateDataHasServerName_UsesServerNameAsFallback()
        {
            UseStorage(storage =>
            {
                using (var session = storage.Repository.OpenSession())
                {
                    var job = new RavenJob
                    {
                        Id = storage.Repository.GetId(typeof(RavenJob), "job-incomplete-3"),
                        CreatedAt = DateTime.UtcNow,
                        InvocationData = InvocationData.SerializeJob(Job.FromExpression(() => SampleMethod())),
                        StateData = new StateData
                        {
                            Name = ProcessingState.StateName,
                            Data = new Dictionary<string, string>
                            {
                                ["ServerName"] = "WorkerServer-99"
                            }
                        }
                    };
                    session.Store(job);
                    session.SaveChanges();
                }

                var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                Assert.NotNull(monitoringApi);

                var processingJobs = monitoringApi.ProcessingJobs(0, 10);
                Assert.NotNull(processingJobs);
                Assert.NotEmpty(processingJobs);

                var first = processingJobs[0].Value;
                Assert.Equal("WorkerServer-99", first.ServerId);
            });
        }

        [Fact]
        public void SucceededScheduledFailedDeletedJobs_WhenStateDataIncomplete_DoNotThrow()
        {
            UseStorage(storage =>
            {
                using (var session = storage.Repository.OpenSession())
                {
                    session.Store(new RavenJob
                    {
                        Id = storage.Repository.GetId(typeof(RavenJob), "job-sched-1"),
                        CreatedAt = DateTime.UtcNow,
                        InvocationData = InvocationData.SerializeJob(Job.FromExpression(() => SampleMethod())),
                        StateData = new StateData { Name = ScheduledState.StateName, Data = new Dictionary<string, string>() }
                    });

                    session.Store(new RavenJob
                    {
                        Id = storage.Repository.GetId(typeof(RavenJob), "job-succ-1"),
                        CreatedAt = DateTime.UtcNow,
                        InvocationData = InvocationData.SerializeJob(Job.FromExpression(() => SampleMethod())),
                        StateData = new StateData { Name = SucceededState.StateName, Data = new Dictionary<string, string>() }
                    });

                    session.Store(new RavenJob
                    {
                        Id = storage.Repository.GetId(typeof(RavenJob), "job-fail-1"),
                        CreatedAt = DateTime.UtcNow,
                        InvocationData = InvocationData.SerializeJob(Job.FromExpression(() => SampleMethod())),
                        StateData = new StateData { Name = FailedState.StateName, Data = new Dictionary<string, string>() }
                    });

                    session.Store(new RavenJob
                    {
                        Id = storage.Repository.GetId(typeof(RavenJob), "job-del-1"),
                        CreatedAt = DateTime.UtcNow,
                        InvocationData = InvocationData.SerializeJob(Job.FromExpression(() => SampleMethod())),
                        StateData = new StateData { Name = DeletedState.StateName, Data = new Dictionary<string, string>() }
                    });

                    session.SaveChanges();
                }

                var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                Assert.NotNull(monitoringApi);

                var scheduled = monitoringApi.ScheduledJobs(0, 10);
                Assert.NotEmpty(scheduled);

                var succeeded = monitoringApi.SucceededJobs(0, 10);
                Assert.NotEmpty(succeeded);

                var failed = monitoringApi.FailedJobs(0, 10);
                Assert.NotEmpty(failed);

                var deleted = monitoringApi.DeletedJobs(0, 10);
                Assert.NotEmpty(deleted);
            });
        }

        [Fact]
        public void SampleMethod()
        {
        }
    }
}
