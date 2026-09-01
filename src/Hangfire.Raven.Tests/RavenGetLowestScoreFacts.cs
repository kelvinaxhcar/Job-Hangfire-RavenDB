using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Raven.Entities;
using Hangfire.Raven.Storage;
using Xunit;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class RavenGetLowestScoreFacts : TesteBase
    {
        public RavenGetLowestScoreFacts(ITestOutputHelper helper) : base(helper)
        {
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseStorage(storage =>
            {
                using var connection = new RavenConnection(storage);
                var exception = Assert.Throws<ArgumentNullException>(() =>
                    connection.GetFirstByLowestScoreFromSet(null, 0, 1));

                Assert.Equal("key", exception.ParamName);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ThrowsAnException_WhenToScoreIsLowerThanFromScore()
        {
            UseStorage(storage =>
            {
                using var connection = new RavenConnection(storage);
                Assert.Throws<ArgumentException>(() =>
                    connection.GetFirstByLowestScoreFromSet("key", 10, 5));
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenKeyDoesNotExist()
        {
            UseStorage(storage =>
            {
                using var connection = new RavenConnection(storage);
                var result = connection.GetFirstByLowestScoreFromSet("non-existent-key", 0, 100);

                Assert.Null(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenSetIsEmpty()
        {
            UseStorage(storage =>
            {
                using (var session = _store.OpenSession())
                {
                    session.Store(new RavenSet
                    {
                        Id = storage.Repository.GetId(typeof(RavenSet), "empty-set"),
                        Scores = new Dictionary<string, double>()
                    });
                    session.SaveChanges();
                }

                using var connection = new RavenConnection(storage);
                var result = connection.GetFirstByLowestScoreFromSet("empty-set", 0, 100);

                Assert.Null(result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsTheValueWithTheLowestScore()
        {
            UseStorage(storage =>
            {
                using (var session = _store.OpenSession())
                {
                    session.Store(new RavenSet
                    {
                        Id = storage.Repository.GetId(typeof(RavenSet), "schedule-set"),
                        Scores = new Dictionary<string, double>
                        {
                            { "job-high", 100.0 },
                            { "job-mid", 50.0 },
                            { "job-low", 10.0 },
                            { "job-out-of-range-low", 2.0 },
                            { "job-out-of-range-high", 200.0 }
                        }
                    });
                    session.SaveChanges();
                }

                using var connection = new RavenConnection(storage);
                var result = connection.GetFirstByLowestScoreFromSet("schedule-set", 5.0, 150.0);

                Assert.Equal("job-low", result);
            });
        }

        [Fact]
        public void GetFirstByLowestScoreFromSet_ReturnsNull_WhenNoScoresInRange()
        {
            UseStorage(storage =>
            {
                using (var session = _store.OpenSession())
                {
                    session.Store(new RavenSet
                    {
                        Id = storage.Repository.GetId(typeof(RavenSet), "schedule-set-2"),
                        Scores = new Dictionary<string, double>
                        {
                            { "job-1", 10.0 },
                            { "job-2", 20.0 }
                        }
                    });
                    session.SaveChanges();
                }

                using var connection = new RavenConnection(storage);
                var result = connection.GetFirstByLowestScoreFromSet("schedule-set-2", 30.0, 50.0);

                Assert.Null(result);
            });
        }

        [Fact]
        public async Task GetFirstByLowestScoreFromSetAsync_ThrowsAnException_WhenKeyIsNull()
        {
            using var repository = new TestRepository(_session);
            var storage = new RavenStorage(repository);
            using var connection = new RavenConnection(storage);
            var exception = await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await connection.GetFirstByLowestScoreFromSetAsync(null, 0, 1));

            Assert.Equal("key", exception.ParamName);
        }

        [Fact]
        public async Task GetFirstByLowestScoreFromSetAsync_ThrowsAnException_WhenToScoreIsLowerThanFromScore()
        {
            using var repository = new TestRepository(_session);
            var storage = new RavenStorage(repository);
            using var connection = new RavenConnection(storage);
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await connection.GetFirstByLowestScoreFromSetAsync("key", 10, 5));
        }

        [Fact]
        public async Task GetFirstByLowestScoreFromSetAsync_ReturnsTheValueWithLowestScore()
        {
            using (var session = _store.OpenSession())
            {
                using var repository = new TestRepository(session);
                var storage = new RavenStorage(repository);
                session.Store(new RavenSet
                {
                    Id = storage.Repository.GetId(typeof(RavenSet), "async-schedule-set"),
                    Scores = new Dictionary<string, double>
                    {
                        { "async-job-1", 50.0 },
                        { "async-job-2", 25.0 },
                        { "async-job-3", 75.0 }
                    }
                });
                session.SaveChanges();
            }

            using (var session = _store.OpenSession())
            {
                using var repository = new TestRepository(session);
                var storage = new RavenStorage(repository);
                using var connection = new RavenConnection(storage);
                var result = await connection.GetFirstByLowestScoreFromSetAsync("async-schedule-set", 20.0, 60.0);

                Assert.Equal("async-job-2", result);
            }
        }

        [Fact]
        public async Task GetFirstByLowestScoreFromSetAsync_RespectsCancellationToken()
        {
            using var repository = new TestRepository(_session);
            var storage = new RavenStorage(repository);
            using var connection = new RavenConnection(storage);
            using var cts = new CancellationTokenSource();
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await connection.GetFirstByLowestScoreFromSetAsync("any-key", 0, 100, cts.Token));
        }
    }
}
