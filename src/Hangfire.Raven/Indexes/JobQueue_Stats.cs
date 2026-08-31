using System.Linq;
using Hangfire.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Hangfire.Raven.Indexes
{
    public class JobQueue_Stats : AbstractIndexCreationTask<JobQueue, JobQueue_Stats.Result>
    {
        public class Result
        {
            public string Queue { get; set; }
            public int Length { get; set; }
            public int Fetched { get; set; }
        }

        public JobQueue_Stats()
        {
            Map = jobs => from job in jobs
                          select new Result
                          {
                              Queue = job.Queue,
                              Length = job.FetchedAt == null ? 1 : 0,
                              Fetched = job.FetchedAt != null ? 1 : 0
                          };

            Reduce = results => from result in results
                                group result by result.Queue into g
                                select new Result
                                {
                                    Queue = g.Key,
                                    Length = g.Sum(x => x.Length),
                                    Fetched = g.Sum(x => x.Fetched)
                                };
        }
    }
}
