using Raven.Client.Documents.Session;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide;
using System;
using Xunit;
using Raven.Client.Documents.Operations;
using System.Collections.Generic;
using System.Threading;
using Raven.TestDriver;
using System.Linq;

namespace Hangfire.Raven.Tests
{
    public class TesteBase : IDisposable
    {
        public IDocumentStore _store;
        public IDocumentSession _session;
        private IAsyncDocumentSession _sessionAsync;
        private string NomeDoBancoDeDados;
        private Dictionary<string, IDocumentStore> StoresDosBancos { get; set; }
        private RavenTestesUnitarios _ravenTestesUnitarios;

        public TesteBase()
        {
            StoresDosBancos = new Dictionary<string, IDocumentStore>();
            _ravenTestesUnitarios = new RavenTestesUnitarios(StoresDosBancos);
            NomeDoBancoDeDados = ObterNomeDoTeste();
            _store = _ravenTestesUnitarios.ObterNovoStore(NomeDoBancoDeDados);
            CriarNovaSessao();
            CriarNovaSessaoAsync();
        }

        private IDocumentSession CriarNovaSessao()
        {
            if (_session != null)
            {
                _ravenTestesUnitarios.SalvarAlteracoes(_session);
                _session.Dispose();
            }

            _session = _store.OpenSession();
            return _session;
        }

        private IAsyncDocumentSession CriarNovaSessaoAsync()
        {
            if (_sessionAsync != null)
            {
                _ravenTestesUnitarios.SalvarAlteracoesAsync(_sessionAsync);
                _sessionAsync.Dispose();
            }

            _sessionAsync = _store.OpenAsyncSession();
            return _sessionAsync;
        }
        private string ObterNomeDoTeste()
        {
            var stackTrace = new System.Diagnostics.StackTrace();
            var frame = stackTrace.GetFrame(1);
            var method = frame.GetMethod();
            return method?.Name ?? "Teste_Unico";
        }

        protected void SalvarAlteracoes()
        {
            _ravenTestesUnitarios.SalvarAlteracoes(_session);
        }

        protected void SalvarAlteracoesAsync()
        {
            _ravenTestesUnitarios.SalvarAlteracoesAsync(_sessionAsync);
        }

        public void Dispose()
        {
            _session?.Dispose();
            _sessionAsync?.Dispose();
            _store?.Dispose();
            _ravenTestesUnitarios?.Dispose();
        }

        private class RavenTestesUnitarios : RavenTestDriver
        {
            private Dictionary<string, IDocumentStore> _storesDosBancos;

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
}
