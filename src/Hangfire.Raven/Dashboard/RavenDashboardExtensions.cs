using System;
using Hangfire.Dashboard;

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

                DashboardRoutes.Routes.AddRazorPage("/ravendb", match => new RavenDashboardPage());

                NavigationMenu.Items.Add(page => new MenuItem("RavenDB", page.Url.To("/ravendb"))
                {
                    Active = page.RequestPath.StartsWith("/ravendb")
                });

                _initialized = true;
            }

            return configuration;
        }
    }
}
