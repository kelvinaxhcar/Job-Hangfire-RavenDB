using System;
using Hangfire.Dashboard;
using Hangfire.Raven.Dashboard.UI5;

namespace Hangfire.Raven.Dashboard
{
    public static class RavenDashboardExtensions
    {
        private static bool _initialized;
        private static readonly object _lock = new object();

        public static IGlobalConfiguration UseRavenDashboard(this IGlobalConfiguration configuration)
        {
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            if (_initialized)
                return configuration;

            lock (_lock)
            {
                if (_initialized)
                    return configuration;

                // 1. Classic RavenDB Metrics Razor Page
                DashboardRoutes.Routes.AddRazorPage("/ravendb", match => new RavenDashboardPage());

                // 2. OpenUI5 SPA Dashboard Dispatcher
                var ui5PageDispatcher = new RavenUI5PageDispatcher();
                DashboardRoutes.Routes.Add("/ui5", ui5PageDispatcher);

                // 3. OpenUI5 REST JSON API Dispatchers
                var ui5ApiDispatcher = new RavenUI5ApiDispatcher();
                DashboardRoutes.Routes.Add("/api/ui5/overview", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/api/ui5/jobs", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/api/ui5/recurring", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/api/ui5/servers", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/api/ui5/job-revisions", ui5ApiDispatcher);

                DashboardRoutes.Routes.Add("/ui5/api/overview", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/ui5/api/jobs", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/ui5/api/recurring", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/ui5/api/servers", ui5ApiDispatcher);
                DashboardRoutes.Routes.Add("/ui5/api/job-revisions", ui5ApiDispatcher);

                // 4. Navigation Menu Items
                NavigationMenu.Items.Add(page => new MenuItem("RavenDB", page.Url.To("/ravendb"))
                {
                    Active = page.RequestPath.StartsWith("/ravendb")
                });

                NavigationMenu.Items.Add(page => new MenuItem("UI5 / Fiori", page.Url.To("/ui5"))
                {
                    Active = page.RequestPath.StartsWith("/ui5")
                });

                _initialized = true;
            }

            return configuration;
        }

        public static IGlobalConfiguration UseRavenUI5Dashboard(this IGlobalConfiguration configuration)
        {
            return configuration.UseRavenDashboard();
        }
    }
}
