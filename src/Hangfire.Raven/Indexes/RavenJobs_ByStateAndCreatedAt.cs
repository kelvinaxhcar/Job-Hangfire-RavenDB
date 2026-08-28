using System.Linq;
using Hangfire.Raven.Entities;
using Raven.Client.Documents.Indexes;

namespace Hangfire.Raven.Indexes
{
    public class RavenJobs_ByStateAndCreatedAt : AbstractIndexCreationTask<RavenJob>
    {
        public RavenJobs_ByStateAndCreatedAt()
        {
            Map = jobs => from job in jobs
                          select new
                          {
                              StateData_Name = job.StateData.Name,
                              job.CreatedAt
                          };
        }
    }
}
