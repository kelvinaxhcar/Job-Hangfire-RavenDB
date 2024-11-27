using Raven.Client.Documents.Session;
using Raven.Client.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Xunit.Abstractions;

namespace Hangfire.Raven.Tests
{
    public class TesteBase : IDisposable
    {
        public IDocumentStore _store;
        public IDocumentSession _session;
        private IAsyncDocumentSession _sessionAsync;
        private string NomeDoBancoDeDados;
        private readonly ITestOutputHelper _helper;
        private Dictionary<string, IDocumentStore> StoresDosBancos { get; set; }
        private RavenTestesUnitarios _ravenTestesUnitarios;

        public TesteBase(ITestOutputHelper helper)
        {
            _helper = helper;
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
        public string ObterNomeDoTeste()
        {
            var valueHelper = _helper
                .GetType()
                ?.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic)
                ?.GetValue(_helper);

            var nomeDoTeste = ((ITest)valueHelper)
                .DisplayName
                .Split(".")
                .LastOrDefault();

            return Regex.Replace(nomeDoTeste, "\\W", "_");
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
    }
}
