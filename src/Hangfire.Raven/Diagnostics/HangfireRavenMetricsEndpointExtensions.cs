using System;
using System.Threading.Tasks;
using Hangfire.Raven.Diagnostics;
using Hangfire.Raven.Storage;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Hangfire.Raven
{
    public static class HangfireRavenMetricsEndpointExtensions
    {
        /// <summary>
        /// Maps a Prometheus-compatible metrics endpoint (text/plain; version=0.0.4) for Hangfire RavenDB storage.
        /// </summary>
        /// <param name="endpoints">The endpoint route builder.</param>
        /// <param name="pattern">The URL path pattern. Defaults to "/hangfire/metrics".</param>
        /// <returns>The route endpoint convention builder.</returns>
        public static IEndpointConventionBuilder MapRavenMetrics(
            this IEndpointRouteBuilder endpoints,
            string pattern = "/hangfire/metrics")
        {
            if (endpoints == null) throw new ArgumentNullException(nameof(endpoints));

            return endpoints.MapGet(pattern, async context =>
            {
                var storage = context.RequestServices.GetService<RavenStorage>()
                           ?? context.RequestServices.GetService<JobStorage>() as RavenStorage;

                var text = HangfireRavenMeter.GeneratePrometheusMetricsText(storage);
                context.Response.ContentType = "text/plain; version=0.0.4; charset=utf-8";
                await context.Response.WriteAsync(text).ConfigureAwait(false);
            });
        }
    }
}
