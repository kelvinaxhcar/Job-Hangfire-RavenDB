using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents;
using Raven.TestDriver;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Hangfire.Raven.Tests
{
    public class RavenTestesUnitarios : RavenTestDriver
    {
        private Dictionary<string, IDocumentStore> _storesDosBancos;

        static RavenTestesUnitarios()
        {
            try
            {
                var baseDir = System.AppContext.BaseDirectory;
                var serverDir = System.IO.Path.Combine(baseDir, "RavenDBServer");
                System.IO.Directory.CreateDirectory(serverDir);
                var content = @"{
  ""ServerUrl"": ""http://127.0.0.1:0"",
  ""Setup.Mode"": ""None"",
  ""License.ThrowOnInvalidOrMissingLicense"": false,
  ""Security.UnsecuredAccessAllowed"": ""PublicNetwork""
}";
                System.IO.File.WriteAllText(System.IO.Path.Combine(serverDir, "settings.json"), content);
                System.IO.File.WriteAllText(System.IO.Path.Combine(baseDir, "settings.json"), content);
            }
            catch { }

            System.Environment.SetEnvironmentVariable("RAVEN_License__ThrowOnInvalidOrMissingLicense", "false");
            System.Environment.SetEnvironmentVariable("RAVEN_License_ThrowOnInvalidOrMissingLicense", "false");
            System.Environment.SetEnvironmentVariable("RAVEN_Setup_Mode", "None");

            ConfigureServer(new TestServerOptions
            {
                FrameworkVersion = null,
                CommandLineArgs = new List<string>
                {
                    "--License.ThrowOnInvalidOrMissingLicense=false",
                    "--Setup.Mode=None"
                }
            });
        }

        public RavenTestesUnitarios(Dictionary<string, IDocumentStore> storesDosBancos)
        {
            _storesDosBancos = storesDosBancos;
        }

        public void Dispose()
        {
            DestruirBancoDeDadosTeste();
        }

        private void DestruirBancoDeDadosTeste()
        {
            foreach (var storeNomeado in _storesDosBancos)
            {
                storeNomeado.Value.Dispose();
            }
        }

        public void SalvarAlteracoes(IDocumentSession session)
        {
            session.SaveChanges();
            WaitForIndexing(session.Advanced.DocumentStore);
            Thread.Sleep(100);
        }

        public void SalvarAlteracoesAsync(IAsyncDocumentSession session)
        {
            session.SaveChangesAsync();
            WaitForIndexing(session.Advanced.DocumentStore);
            Thread.Sleep(100);
        }

        public IDocumentStore ObterNovoStore(string nomeDoBanco)
        {
            var store = GetDocumentStore(database: nomeDoBanco);
            _storesDosBancos.Add(nomeDoBanco, store);

            WaitForIndexing(store);
            Thread.Sleep(100);

            return store;
        }

        public void AdicionarListaComId<T>(IDocumentSession session, List<KeyValuePair<string, T>> lista) where T : new()
        {
            lista.ForEach(x => session.Store(x.Value, x.Key));
            SalvarAlteracoes(session);
        }

        protected void WaitForIndexing(IDocumentStore documentStore)
        {
            while (documentStore.Maintenance.Send(new GetStatisticsOperation()).StaleIndexes.Any())
            {
                Thread.Sleep(50);
            }
        }
        protected override void PreInitialize(IDocumentStore documentStore)
        {
            documentStore.Conventions.MaxNumberOfRequestsPerSession = 100;
            documentStore.Conventions.UseOptimisticConcurrency = false;
            documentStore.Conventions.IdentityPartsSeparator = '-';
            documentStore.Conventions.SaveEnumsAsIntegers = true;
        }
    }
}
