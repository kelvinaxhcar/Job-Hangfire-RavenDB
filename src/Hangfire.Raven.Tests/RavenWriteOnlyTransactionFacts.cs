using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.States;
using Moq;
using Xunit;
using Hangfire.Raven.JobQueues;
using Hangfire.Raven.Storage;
using Hangfire.Raven.Entities;
using Xunit.Abstractions;
using Raven.Client.Documents.Session;
using Raven.Client;
using Hangfire.Common;

namespace Hangfire.Raven.Tests
{
    public class RavenWriteOnlyTransactionFacts : TesteBase
    {
        private readonly PersistentJobQueueProviderCollection _queueProviders;

        public RavenWriteOnlyTransactionFacts(ITestOutputHelper helper) : base(helper)
        {
            Mock<IPersistentJobQueueProvider> defaultProvider = new Mock<IPersistentJobQueueProvider>();
            defaultProvider.Setup(x => x.GetJobQueue())
                .Returns(new Mock<IPersistentJobQueue>().Object);

            _queueProviders = new PersistentJobQueueProviderCollection(defaultProvider.Object);
        }

        [Fact]
        public void Ctor_ThrowsAnException_IfStorageIsNull()
        {
            ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new RavenWriteOnlyTransaction(null));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void ExpireJob_SetsJobExpirationData()
        {
            UseConnection(repository =>
            {
                var jobId = Guid.NewGuid().ToString();
                RavenJob job = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                var anotherJobId = Guid.NewGuid().ToString();
                RavenJob anotherJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), anotherJobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                _session.Store(job);
                _session.Store(anotherJob);
                _session.SaveChanges();

                Commit(repository, x => x.ExpireJob(jobId, TimeSpan.FromDays(1)));
                _session.SaveChanges();
                _session.Advanced.Refresh(job);

                // Verificação do job expirado
                var testJob = GetTestJob(_session, job.Id);
                var expirationDateTime = GetJobExpirationDate(_session, testJob);

                Assert.True(DateTime.UtcNow.AddMinutes(-1) < expirationDateTime
                            && expirationDateTime <= DateTime.UtcNow.AddDays(1));

                // Verificação do job sem expiração
                var anotherTestJob = GetTestJob(_session, anotherJob.Id);
                _session.Advanced.Refresh(anotherTestJob);

                expirationDateTime = GetJobExpirationDate(_session, anotherTestJob);
                Assert.Null(expirationDateTime);
            });
        }

        private static DateTime? GetJobExpirationDate(IDocumentSession session, Object job)
        {
            var metadata = session.Advanced.GetMetadataFor(job);
            metadata.TryGetValue(Constants.Documents.Metadata.Expires, out var expireAt);
            if (expireAt == null)
            {
                return null;
            }
            DateTime.TryParse(expireAt.ToString(), out var expirationDateTime);
            return expirationDateTime;
        }


        [Fact]
        public void PersistJob_ClearsTheJobExpirationData()
        {
            UseConnection(repository =>
            {
                var jobId = Guid.NewGuid().ToString();
                RavenJob job = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                var anotherJobId = Guid.NewGuid().ToString();
                RavenJob anotherJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), anotherJobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                _session.Store(job);
                _session.Advanced.GetMetadataFor(job)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.Store(anotherJob);
                _session.Advanced.GetMetadataFor(anotherJob)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.SaveChanges();

                Commit(repository, x => x.PersistJob(jobId.ToString()));

                _session.Advanced.Refresh(job);
                var testJob = GetTestJob(_session, job.Id);
                var expireAt = GetJobExpirationDate(_session, testJob);
                Assert.Null(expireAt);

                var anotherTestJob = GetTestJob(_session, anotherJob.Id);
                expireAt = GetJobExpirationDate(_session, anotherTestJob);
                Assert.NotNull(expireAt);
            });
        }

        [Fact]
        public void SetJobState_AppendsAStateAndSetItToTheJob()
        {
            UseConnection(repository =>
            {
                var jobId = Guid.NewGuid().ToString();
                RavenJob job = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), jobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                var anotherJobId = Guid.NewGuid().ToString();
                RavenJob anotherJob = new RavenJob
                {
                    Id = repository.GetId(typeof(RavenJob), anotherJobId),
                    InvocationData = null,
                    CreatedAt = DateTime.UtcNow
                };

                _session.Store(job);
                _session.Store(anotherJob);
                _session.SaveChanges();

                var serializedData = new Dictionary<string, string> { { "Name", "Value" } };

                var state = new Mock<IState>();
                state.Setup(x => x.Name).Returns("State");
                state.Setup(x => x.Reason).Returns("Reason");
                state.Setup(x => x.SerializeData()).Returns(serializedData);

                Commit(repository, x => x.SetJobState(jobId, state.Object));

                _session.Advanced.Refresh(job);
                var testJob = GetTestJob(_session, job.Id);
                Assert.NotNull(testJob.StateData);

                var anotherTestJob = GetTestJob(_session, anotherJob.Id);
                Assert.Null(anotherTestJob.StateData);

                var jobState = testJob.StateData;
                Assert.Equal("State", jobState.Name);
                Assert.Equal("Reason", jobState.Reason);
                Assert.Equal(serializedData, jobState.Data);
            });
        }

        [Fact]
        public void AddToQueue_CallsEnqueue_OnTargetPersistentQueue()
        {
            UseConnection(repository =>
            {
                var correctJobQueue = new Mock<IPersistentJobQueue>();
                var correctProvider = new Mock<IPersistentJobQueueProvider>();
                correctProvider.Setup(x => x.GetJobQueue())
                    .Returns(correctJobQueue.Object);

                _queueProviders.Add(correctProvider.Object, new[] { "default" });

                Commit(repository, x => x.AddToQueue("default", "1"));

                correctJobQueue.Verify(x => x.Enqueue("default", "1"));
            });
        }

        [Fact]
        public void IncrementCounter_AddsRecordToCounterTable_WithPositiveValue()
        {
            UseConnection(repository =>
            {
                Commit(repository, x => x.IncrementCounter("my-key"));

                Counter record = _session.Query<Counter>().Single();

                var id = repository.GetId(typeof(Counter), "my-key");
                Assert.Equal(id, record.Id);
                Assert.Equal(1, record.Value);
                var expireAt = GetJobExpirationDate(_session, record);
                Assert.Equal(null, expireAt);
            });
        }

        [Fact]
        public void IncrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(repository =>
            {
                Commit(repository, x => x.IncrementCounter("my-key", TimeSpan.FromDays(1)));

                Counter record = _session.Query<Counter>().Single();

                var id = repository.GetId(typeof(Counter), "my-key");
                Assert.Equal(id, record.Id);
                Assert.Equal(1, record.Value);
                var expireAt = GetJobExpirationDate(_session, record);
                Assert.NotNull(expireAt);

                Assert.True(DateTime.UtcNow.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.UtcNow.AddHours(25));
            });
        }

        [Fact]
        public void IncrementCounter_WithExistingKey_IncrementsValue()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.IncrementCounter("my-key");
                    x.IncrementCounter("my-key");
                });

                Counter record = _session.Query<Counter>().Single();

                var id = repository.GetId(typeof(Counter), "my-key");
                Assert.Equal(id, record.Id);
                Assert.Equal(2, record.Value);
            });
        }

        [Fact]
        public void DecrementCounter_AddsRecordToCounterTable_WithNegativeValue()
        {
            UseConnection(repository =>
            {
                Commit(repository, x => x.DecrementCounter("my-key"));

                Counter record = _session.Query<Counter>().Single();

                var id = repository.GetId(typeof(Counter), "my-key");
                Assert.Equal(id, record.Id);
                Assert.Equal(-1, record.Value);
                var expireAt = GetJobExpirationDate(_session, record);
                Assert.Equal(null, expireAt);
            });
        }

        [Fact]
        public void DecrementCounter_WithExpiry_AddsARecord_WithExpirationTimeSet()
        {
            UseConnection(repository =>
            {
                Commit(repository, x => x.DecrementCounter("my-key", TimeSpan.FromDays(1)));

                Counter record = _session.Query<Counter>().Single();

                var id = repository.GetId(typeof(Counter), "my-key");
                _session.Advanced.Refresh(record);
                Assert.Equal(id, record.Id);
                Assert.Equal(-1, record.Value);
                var expireAt = GetJobExpirationDate(_session, record);
                Assert.NotNull(expireAt);

                Assert.True(DateTime.Now.AddHours(23) < expireAt);
                Assert.True(expireAt < DateTime.Now.AddHours(25));
            });
        }


        [Fact]
        public void AddToSet_AddsARecord_WhenKeyIsExists_ButValuesAreDifferent()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "another-value");
                });

                RavenSet record = _session.Query<RavenSet>().Single();
                var recordCount = record.Scores.Count;

                Assert.Equal(2, recordCount);
            });
        }

        [Fact]
        public void AddToSet_DoesNotAddARecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value");
                });

                RavenSet record = _session.Query<RavenSet>().Single();
                var recordCount = record.Scores.Count;

                Assert.Equal(1, recordCount);
            });
        }

        [Fact]
        public void AddToSet_WithScore_AddsARecordWithScore_WhenBothKeyAndValueAreNotExist()
        {
            UseConnection(repository =>
            {
                Commit(repository, x => x.AddToSet("my-key", "my-value", 3.2));

                RavenSet record = _session.Query<RavenSet>().Single();

                var id = repository.GetId(typeof(RavenSet), "my-key");
                Assert.Equal(id, record.Id);
                Assert.Equal("my-value", record.Scores.Keys.Single());
                Assert.Equal(3.2, record.Scores["my-value"], 3);
            });
        }

        [Fact]
        public void AddToSet_WithScore_UpdatesAScore_WhenBothKeyAndValueAreExist()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.AddToSet("my-key", "my-value", 3.2);
                });

                RavenSet record = _session.Query<RavenSet>().Single();

                Assert.Equal(3.2, record.Scores["my-value"], 3);
            });
        }

        [Fact]
        public void RemoveFromSet_RemovesARecord_WithGivenKeyAndValue()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "my-value");
                });

                var recordCount = _session.Query<RavenSet>().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameKey_AndDifferentValue()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("my-key", "different-value");
                });

                var recordCount = _session.Query<RavenSet>().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact]
        public void RemoveFromSet_DoesNotRemoveRecord_WithSameValue_AndDifferentKey()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.AddToSet("my-key", "my-value");
                    x.RemoveFromSet("different-key", "my-value");
                });

                var recordCount = _session.Query<RavenSet>().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact]
        public void InsertToList_AddsARecord_WithGivenValues()
        {
            UseConnection(repository =>
            {
                Commit(repository, x => x.InsertToList("my-key", "my-value"));

                RavenList record = _session.Query<RavenList>().Single();

                var id = repository.GetId(typeof(RavenList), "my-key");
                Assert.Equal(id, record.Id);
                Assert.Equal("my-value", record.Values.Single());
            });
        }

        [Fact]
        public void InsertToList_AddsAnotherRecord_WhenBothKeyAndValueAreExist()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                });

                RavenList record = _session.Query<RavenList>().Single();
                var recordCount = record.Values.Count;

                Assert.Equal(2, recordCount);
            });
        }

        [Fact]
        public void RemoveFromList_RemovesAllRecords_WithGivenKeyAndValue()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "my-value");
                });

                var recordCount = _session.Query<RavenList>().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameKey_ButDifferentValue()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("my-key", "different-value");
                });

                var recordCount = _session.Query<RavenList>().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact]
        public void RemoveFromList_DoesNotRemoveRecords_WithSameValue_ButDifferentKey()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "my-value");
                    x.RemoveFromList("different-key", "my-value");
                });

                var recordCount = _session.Query<RavenList>().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact]
        public void TrimList_TrimsAList_ToASpecifiedRange()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.InsertToList("my-key", "3");
                    x.TrimList("my-key", 1, 2);
                });

                var records = _session.Query<RavenList>().Single().Values;

                Assert.Equal(2, records.Count);
                Assert.Equal("1", records[0]);
                Assert.Equal("2", records[1]);
            });
        }

        [Fact]
        public void TrimList_RemovesRecordsToEnd_IfKeepEndingAt_GreaterThanMaxElementIndex()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.InsertToList("my-key", "1");
                    x.InsertToList("my-key", "2");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = _session.Query<RavenList>().Single().Values.Count;

                Assert.Equal(2, recordCount);
            });
        }

        [Fact]
        public void TrimList_RemovesAllRecords_WhenStartingFromValue_GreaterThanMaxElementIndex()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 100);
                });

                var recordCount = _session.Query<RavenList>().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact]
        public void TrimList_RemovesAllRecords_IfStartFromGreaterThanEndingAt()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("my-key", 1, 0);
                });

                var recordCount = _session.Query<RavenList>().Count();

                Assert.Equal(0, recordCount);
            });
        }

        [Fact]
        public void TrimList_RemovesRecords_OnlyOfAGivenKey()
        {
            UseConnection(repository =>
            {
                Commit(repository, x =>
                {
                    x.InsertToList("my-key", "0");
                    x.TrimList("another-key", 1, 0);
                });

                var recordCount = _session.Query<RavenList>().Count();

                Assert.Equal(1, recordCount);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.SetRangeInHash(null, new Dictionary<string, string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.SetRangeInHash("some-hash", null)));

                Assert.Equal("keyValuePairs", exception.ParamName);
            });
        }

        [Fact]
        public void SetRangeInHash_MergesAllRecords()
        {
            UseConnection(repository =>
            {
                Commit(repository, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                var id = repository.GetId(typeof(RavenHash), "some-hash");
                var result = _session.Load<RavenHash>(id).Fields;

                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }

        [Fact]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.RemoveHash(null)));
            });
        }

        [Fact]
        public void RemoveHash_RemovesAllHashRecords()
        {
            UseConnection(repository =>
            {
                // Arrange
                Commit(repository, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                // Act
                Commit(repository, x => x.RemoveHash("some-hash"));

                // Assert
                var count = _session.Query<RavenHash>().Count();
                Assert.Equal(0, count);
            });
        }

        [Fact]
        public void ExpireSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.ExpireSet(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void ExpireSet_SetsSetExpirationData()
        {
            UseConnection(repository =>
            {
                var set1 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set1"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value1", 0.0 }
                    }
                };

                var set2 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set2"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value2", 0.0 }
                    }
                };

                _session.Store(set1);
                _session.Store(set2);
                _session.SaveChanges();

                Commit(repository, x => x.ExpireSet("Set1", TimeSpan.FromDays(1)));
                _session.Advanced.Refresh(set1);
                _session.Advanced.Refresh(set2);

                var testSet1 = GetTestSet(_session, set1.Id);
                var expireAt = GetJobExpirationDate(_session, testSet1);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < expireAt && expireAt <= DateTime.UtcNow.AddDays(1));

                var testSet2 = GetTestSet(_session, set2.Id);
                Assert.NotNull(testSet2);
                expireAt = GetJobExpirationDate(_session, testSet2);
                Assert.Null(expireAt);
            });
        }

        [Fact]
        public void ExpireList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.ExpireList(null, TimeSpan.FromSeconds(45))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void ExpireList_SetsListExpirationData()
        {
            UseConnection(repository =>
            {
                var list1 = new RavenList
                {
                    Id = repository.GetId(typeof(RavenList), "List1"),
                    Values = new List<string>
                    {
                        "value1"
                    }
                };

                var list2 = new RavenList
                {
                    Id = repository.GetId(typeof(RavenList), "List2"),
                    Values = new List<string>
                    {
                        "value2"
                    }
                };

                _session.Store(list1);
                _session.Store(list2);
                _session.SaveChanges();

                Commit(repository, x => x.ExpireList("List1", TimeSpan.FromDays(1)));
                _session.Advanced.Refresh(list1);
                _session.Advanced.Refresh(list2);

                var testList1 = GetTestList(_session, list1.Id);
                var expireAt = GetJobExpirationDate(_session, testList1);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < expireAt && expireAt <= DateTime.UtcNow.AddDays(1));

                var testList2 = GetTestList(_session, list2.Id);
                expireAt = GetJobExpirationDate(_session, testList2);
                Assert.Null(expireAt);
            });
        }

        [Fact]
        public void ExpireHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.ExpireHash(null, TimeSpan.FromMinutes(5))));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void ExpireHash_SetsHashExpirationData()
        {
            UseConnection(repository =>
            {
                var hash1 = new RavenHash
                {
                    Id = repository.GetId(typeof(RavenHash), "Hash1"),
                    Fields = new Dictionary<string, string>
                    {
                        { "field1", "value1" }
                    }
                };

                var hash2 = new RavenHash
                {
                    Id = repository.GetId(typeof(RavenHash), "Hash2"),
                    Fields = new Dictionary<string, string>
                    {
                        { "field2", "value2" }
                    }
                };

                _session.Store(hash1);
                _session.Store(hash2);
                _session.SaveChanges();

                Commit(repository, x => x.ExpireHash("Hash1", TimeSpan.FromDays(1)));
                _session.Advanced.Refresh(hash1);
                _session.Advanced.Refresh(hash2);

                var testHash1 = GetTestHash(_session, hash1.Id);
                var expireAt = GetJobExpirationDate(_session, testHash1);
                Assert.True(DateTime.UtcNow.AddMinutes(-1) < expireAt && expireAt <= DateTime.UtcNow.AddDays(1));

                var testHash2 = GetTestHash(_session, hash2.Id);
                expireAt = GetJobExpirationDate(_session, testHash2);
                Assert.Null(expireAt);
            });
        }

        [Fact]
        public void PersistSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.PersistSet(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void PersistSet_ClearsTheSetExpirationData()
        {
            UseConnection(repository =>
            {
                var set1 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set1"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value1", 0.0 }
                    }
                };

                var set2 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set2"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value2", 0.0 }
                    }
                };

                _session.Store(set1);
                _session.Advanced.GetMetadataFor(set1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.Store(set2);
                _session.Advanced.GetMetadataFor(set2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.SaveChanges();

                Commit(repository, x => x.PersistSet("Set1"));

                _session.Advanced.Refresh(set1);
                var testSet1 = GetTestSet(_session, set1.Id);
                var expireAt = GetJobExpirationDate(_session, testSet1);
                Assert.Null(expireAt);

                _session.Advanced.Refresh(set2);
                var testSet2 = GetTestSet(_session, set2.Id);
                expireAt = GetJobExpirationDate(_session, testSet2);
                Assert.NotNull(expireAt);
            });
        }

        [Fact]
        public void PersistList_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.PersistList(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void PersistList_ClearsTheListExpirationData()
        {
            UseConnection(repository =>
            {
                var list1 = new RavenList
                {
                    Id = repository.GetId(typeof(RavenList), "List1"),
                    Values = new List<string>
                    {
                        "value1"
                    }
                };

                var list2 = new RavenList
                {
                    Id = repository.GetId(typeof(RavenList), "List2"),
                    Values = new List<string>
                    {
                        "value2"
                    }
                };

                _session.Store(list1);
                _session.Advanced.GetMetadataFor(list1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.Store(list2);
                _session.Advanced.GetMetadataFor(list2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.SaveChanges();

                Commit(repository, x => x.PersistList("List1"));

                _session.Advanced.Refresh(list1);
                var testList1 = GetTestList(_session, list1.Id);
                var expireAt = GetJobExpirationDate(_session, testList1);
                Assert.Null(expireAt);

                _session.Advanced.Refresh(list2);
                var testList2 = GetTestList(_session, list2.Id);
                expireAt = GetJobExpirationDate(_session, testList2);
                Assert.NotNull(expireAt);
            });
        }

        [Fact]
        public void PersistHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.PersistHash(null)));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void PersistHash_ClearsTheHashExpirationData()
        {
            UseConnection(repository =>
            {
                var hash1 = new RavenHash
                {
                    Id = repository.GetId(typeof(RavenHash), "Hash1"),
                    Fields = new Dictionary<string, string>
                    {
                        { "field1", "value1" }
                    }
                };

                var hash2 = new RavenHash
                {
                    Id = repository.GetId(typeof(RavenHash), "Hash2"),
                    Fields = new Dictionary<string, string>
                    {
                        { "field2", "value2" }
                    }
                };

                _session.Store(hash1);
                _session.Advanced.GetMetadataFor(hash1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.Store(hash2);
                _session.Advanced.GetMetadataFor(hash2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.SaveChanges();

                Commit(repository, x => x.PersistHash("Hash1"));

                var testHash1 = GetTestHash(_session, hash1.Id);
                var expireAt = GetJobExpirationDate(_session, testHash1);
                Assert.Null(expireAt);

                var testHash2 = GetTestHash(_session, hash2.Id);
                expireAt = GetJobExpirationDate(_session, testHash2);
                Assert.NotNull(expireAt);
            });
        }

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.AddRangeToSet(null, new List<string>())));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void AddRangeToSet_ThrowsAnException_WhenItemsValueIsNull()
        {
            UseConnection(repository =>
            {
                var exception = Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.AddRangeToSet("my-set", null)));

                Assert.Equal("items", exception.ParamName);
            });
        }

        [Fact]
        public void AddRangeToSet_AddToExistingSetData()
        {
            UseConnection(repository =>
            {
                var set1 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set1"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value1", 0.0 },
                        { "value2", 0.0 }
                    }
                };

                var set2 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set2"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value2", 0.0 }
                    }
                };

                _session.Store(set1);
                _session.Advanced.GetMetadataFor(set1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.Store(set2);
                _session.Advanced.GetMetadataFor(set2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.SaveChanges();

                var values = new[] { "test1", "test2", "test3" };
                Commit(repository, x => x.AddRangeToSet("Set1", values));
                _session.Advanced.Refresh(set1);

                var testSet1 = GetTestSet(_session, set1.Id);
                Assert.NotNull(testSet1);
                Assert.Equal(5, testSet1.Scores.Count);

                _session.Advanced.Refresh(set2);
                var testSet2 = GetTestSet(_session, set2.Id);
                Assert.NotNull(testSet2);
                Assert.Equal(1, testSet2.Scores.Count);
            });
        }

        [Fact]
        public void RemoveSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(repository =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(repository, x => x.RemoveSet(null)));
            });
        }

        [Fact]
        public void RemoveSet_ClearsTheSetData()
        {
            UseConnection(repository =>
            {
                var set1 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set1"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value1", 0.0 },
                        { "value2", 0.0 }
                    }
                };

                var set2 = new RavenSet
                {
                    Id = repository.GetId(typeof(RavenSet), "Set2"),
                    Scores = new Dictionary<string, double>
                    {
                        { "value2", 0.0 }
                    }
                };

                _session.Store(set1);
                _session.Advanced.GetMetadataFor(set1)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.Store(set2);
                _session.Advanced.GetMetadataFor(set2)[Constants.Documents.Metadata.Expires] = DateTime.UtcNow;
                _session.SaveChanges();

                Commit(repository, x => x.RemoveSet("Set1"));
                _session.Advanced.Refresh(set1);

                var testSet1 = GetTestSet(_session, set1.Id);
                Assert.Null(testSet1);

                _session.Advanced.Refresh(set2);
                var testSet2 = GetTestSet(_session, set2.Id);
                Assert.Equal(1, testSet2.Scores.Count);
            });
        }


        private static RavenJob GetTestJob(IDocumentSession _session, string jobId)
        {
            return _session.Load<RavenJob>(jobId);
        }

        private static RavenSet GetTestSet(IDocumentSession _session, string setId)
        {
            return _session.Load<RavenSet>(setId);
        }

        private static RavenList GetTestList(IDocumentSession _session, string listId)
        {
            return _session.Load<RavenList>(listId);
        }

        private static RavenHash GetTestHash(IDocumentSession _session, string hashId)
        {
            return _session.Load<RavenHash>(hashId);
        }

        private void UseConnection(Action<IRepository> action)
        {
            using (var repository = new TestRepository(_session))
            {
                action(repository);
            }
        }

        private void Commit(IRepository repository, Action<RavenWriteOnlyTransaction> action)
        {
            var storage = new RavenStorage(repository);

            using (RavenWriteOnlyTransaction transaction = new RavenWriteOnlyTransaction(storage))
            {
                action(transaction);
                transaction.Commit();
            }
        }
    }
}
