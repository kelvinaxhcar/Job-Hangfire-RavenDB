using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace Hangfire.Raven
{
    public class RepositoryConfig
    {
        private string[] _urls;

        public string ConnectionUrl
        {
            get => _urls != null && _urls.Length > 0 ? _urls[0] : null;
            set => _urls = !string.IsNullOrEmpty(value) ? new[] { value } : Array.Empty<string>();
        }

        public string[] Urls
        {
            get => _urls ?? Array.Empty<string>();
            set => _urls = value;
        }

        public string Database { get; set; }

        public X509Certificate2 Certificate { get; set; }
    }
}
