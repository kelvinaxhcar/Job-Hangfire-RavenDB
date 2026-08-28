using System.Linq;
using Hangfire.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Hangfire.Raven.Indexes
{
    public class JobQueue_ByQueueAndFetchedAt : AbstractIndexCreationTask<JobQueue>
    {
        public JobQueue_ByQueueAndFetchedAt()
        {
            Map = jobs => from job in jobs
                          select new
                          {
                              job.Queue,
                              job.FetchedAt,
                              job.JobId
                          };
        }
    }
}
