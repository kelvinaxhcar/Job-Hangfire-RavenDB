using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Hangfire.Dashboard;
using Hangfire.Raven.Dashboard;
using Hangfire.Raven.Storage;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace Hangfire.Raven.Tests
{
    public class RavenDashboardSecurityFacts
    {
        [Fact]
        public void RavenDashboardPage_EncodesPotentiallyMaliciousInputs_AgainstXSS()
        {
            var maliciousDbName = "<script>alert('xss-db')</script>";
            var maliciousDbId = "db\" onmouseover=\"alert(1)\"";
            var maliciousSize = "<img src=x onerror=alert('xss-size')>";
            var maliciousIndexName = "<svg/onload=alert('xss-index')>";
            var maliciousIndexType = "<b>MapReduce</b>";
            var maliciousIndexState = "<iframe src='javascript:alert(1)'>";

            var metrics = new RavenStorageMetricsDto
            {
                DatabaseName = maliciousDbName,
                DatabaseId = maliciousDbId,
                SizeOnDisk = maliciousSize,
                DocumentsCount = 100,
                IndexesCount = 1,
                StaleIndexesCount = 0,
                Indexes = new List<RavenIndexMetricsDto>
                {
                    new RavenIndexMetricsDto
                    {
                        Name = maliciousIndexName,
                        Type = maliciousIndexType,
                        State = maliciousIndexState,
                        IsStale = false
                    }
                }
            };

            var repositoryMock = new Mock<IRepository>();
            var storageMock = new Mock<RavenStorage>(repositoryMock.Object, new RavenStorageOptions { EnableCache = false });
            var monitoringApiMock = new Mock<RavenStorageMonitoringApi>(storageMock.Object);
            
            monitoringApiMock.Setup(m => m.GetRavenMetrics()).Returns(metrics);
            storageMock.Setup(s => s.GetMonitoringApi()).Returns(monitoringApiMock.Object);

            var services = new ServiceCollection();
            var httpContext = new DefaultHttpContext
            {
                RequestServices = services.BuildServiceProvider()
            };
            httpContext.Response.Body = new MemoryStream();

            var context = new AspNetCoreDashboardContext(storageMock.Object, new DashboardOptions(), httpContext);

            var page = new RavenDashboardPage();
            
            var contextProp = typeof(RazorPage).GetProperty("Context", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (contextProp != null && contextProp.CanWrite)
            {
                contextProp.SetValue(page, context);
            }
            else
            {
                var contextField = typeof(RazorPage).GetField("_context", BindingFlags.Instance | BindingFlags.NonPublic)
                                ?? typeof(RazorPage).GetField("<Context>k__BackingField", BindingFlags.Instance | BindingFlags.NonPublic);
                contextField?.SetValue(page, context);
            }

            page.Execute();

            string html = string.Empty;
            foreach (var field in typeof(RazorPage).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public))
            {
                var val = field.GetValue(page);
                if (val is StringBuilder sb && sb.Length > 0)
                {
                    html = sb.ToString();
                    break;
                }
                if (val is StringWriter sw)
                {
                    html = sw.ToString();
                    if (!string.IsNullOrEmpty(html)) break;
                }
            }

            Assert.NotEmpty(html);

            // Raw dangerous tags must NOT be present unescaped
            Assert.DoesNotContain("<script>", html);
            Assert.DoesNotContain("<img src=x", html);
            Assert.DoesNotContain("<svg/onload=", html);
            Assert.DoesNotContain("<iframe", html);
            Assert.DoesNotContain("<b>MapReduce</b>", html);

            // Escaped HTML entities MUST be present
            Assert.Contains("&lt;script&gt;alert(&#39;xss-db&#39;)&lt;/script&gt;", html);
            Assert.Contains("&lt;img src=x onerror=alert(&#39;xss-size&#39;)&gt;", html);
            Assert.Contains("&lt;svg/onload=alert(&#39;xss-index&#39;)&gt;", html);
            Assert.Contains("&lt;iframe src=&#39;javascript:alert(1)&#39;&gt;", html);
            Assert.Contains("&lt;b&gt;MapReduce&lt;/b&gt;", html);
        }
    }
}
