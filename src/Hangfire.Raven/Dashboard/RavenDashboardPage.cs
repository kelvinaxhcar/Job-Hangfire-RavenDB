using Hangfire.Dashboard;
using Hangfire.Dashboard.Pages;
using Hangfire.Raven.Storage;

namespace Hangfire.Raven.Dashboard
{
    public class RavenDashboardPage : RazorPage
    {
        public override void Execute()
        {
            Layout = new LayoutPage("RavenDB");

            RavenStorageMetricsDto metrics = null;
            if (Storage is RavenStorage storage)
            {
                var monitoringApi = storage.GetMonitoringApi() as RavenStorageMonitoringApi;
                metrics = monitoringApi?.GetRavenMetrics();
            }

            metrics ??= new RavenStorageMetricsDto();

            WriteLiteral("<div class=\"row\">\r\n");
            WriteLiteral("<div class=\"col-md-12\">\r\n");
            WriteLiteral("<h1 class=\"page-header\">RavenDB Storage Metrics</h1>\r\n");

            // Overview Cards Row
            WriteLiteral("<div class=\"row\">\r\n");

            // Database Info Card
            WriteLiteral("<div class=\"col-md-4 col-sm-6\">\r\n");
            WriteLiteral("<div class=\"panel panel-default\">\r\n");
            WriteLiteral("<div class=\"panel-heading\"><strong>Database</strong></div>\r\n");
            WriteLiteral("<div class=\"panel-body\">\r\n");
            WriteLiteral($"<h4>{metrics.DatabaseName ?? "N/A"}</h4>\r\n");
            WriteLiteral($"<p class=\"text-muted\" style=\"margin-bottom:0;\"><small>ID: {metrics.DatabaseId ?? "N/A"}</small></p>\r\n");
            WriteLiteral("</div></div></div>\r\n");

            // Documents Card
            WriteLiteral("<div class=\"col-md-4 col-sm-6\">\r\n");
            WriteLiteral("<div class=\"panel panel-default\">\r\n");
            WriteLiteral("<div class=\"panel-heading\"><strong>Total Documents</strong></div>\r\n");
            WriteLiteral("<div class=\"panel-body\">\r\n");
            WriteLiteral($"<h2 class=\"text-primary\" style=\"margin:0;\">{metrics.DocumentsCount:N0}</h2>\r\n");
            WriteLiteral("</div></div></div>\r\n");

            // Size on Disk Card
            WriteLiteral("<div class=\"col-md-4 col-sm-6\">\r\n");
            WriteLiteral("<div class=\"panel panel-default\">\r\n");
            WriteLiteral("<div class=\"panel-heading\"><strong>Size on Disk</strong></div>\r\n");
            WriteLiteral("<div class=\"panel-body\">\r\n");
            WriteLiteral($"<h2 class=\"text-success\" style=\"margin:0;\">{metrics.SizeOnDisk ?? "N/A"}</h2>\r\n");
            WriteLiteral("</div></div></div>\r\n");

            WriteLiteral("</div>\r\n");

            // Indexes Section
            WriteLiteral("<div class=\"panel panel-default\">\r\n");
            WriteLiteral("<div class=\"panel-heading\">\r\n");
            WriteLiteral($"<h3 class=\"panel-title\">Indexes ({metrics.IndexesCount} total) ");
            if (metrics.StaleIndexesCount > 0)
            {
                WriteLiteral($"<span class=\"label label-warning\">{metrics.StaleIndexesCount} Stale</span>");
            }
            else
            {
                WriteLiteral("<span class=\"label label-success\">Healthy</span>");
            }
            WriteLiteral("</h3></div>\r\n");

            WriteLiteral("<div class=\"table-responsive\">\r\n");
            WriteLiteral("<table class=\"table table-striped table-hover\">\r\n");
            WriteLiteral("<thead><tr><th>Index Name</th><th>Type</th><th>State</th><th>Status</th></tr></thead>\r\n");
            WriteLiteral("<tbody>\r\n");

            if (metrics.Indexes != null && metrics.Indexes.Count > 0)
            {
                foreach (var index in metrics.Indexes)
                {
                    WriteLiteral("<tr>\r\n");
                    WriteLiteral($"<td><strong>{index.Name}</strong></td>\r\n");
                    WriteLiteral($"<td>{index.Type}</td>\r\n");
                    WriteLiteral($"<td>{index.State}</td>\r\n");
                    if (index.IsStale)
                    {
                        WriteLiteral("<td><span class=\"label label-warning\">Stale</span></td>\r\n");
                    }
                    else
                    {
                        WriteLiteral("<td><span class=\"label label-success\">Up to date</span></td>\r\n");
                    }
                    WriteLiteral("</tr>\r\n");
                }
            }
            else
            {
                WriteLiteral("<tr><td colspan=\"4\" class=\"text-muted\">No indexes registered.</td></tr>\r\n");
            }

            WriteLiteral("</tbody></table></div></div>\r\n");

            WriteLiteral("</div></div>\r\n");
        }
    }
}
