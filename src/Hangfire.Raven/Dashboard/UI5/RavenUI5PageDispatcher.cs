using System.Threading.Tasks;
using Hangfire.Dashboard;

namespace Hangfire.Raven.Dashboard.UI5
{
    public class RavenUI5PageDispatcher : IDashboardDispatcher
    {
        public async Task Dispatch(DashboardContext context)
        {
            context.Response.ContentType = "text/html; charset=utf-8";
            await context.Response.WriteAsync(GetHtml());
        }

        private static string GetHtml()
        {
            return @"<!DOCTYPE html>
<html lang=""en"">
<head>
    <meta charset=""UTF-8"">
    <meta name=""viewport"" content=""width=device-width, initial-scale=1.0"">
    <title>Hangfire Dashboard — SAP Fiori / OpenUI5</title>
    <script id=""sap-ui-bootstrap""
        src=""https://sdk.openui5.org/resources/sap-ui-core.js""
        data-sap-ui-theme=""sap_horizon""
        data-sap-ui-libs=""sap.m,sap.f,sap.ui.layout,sap.ui.core,sap.tnt""
        data-sap-ui-async=""true""
        data-sap-ui-compatVersion=""edge"">
    </script>
    <script src=""https://cdn.jsdelivr.net/npm/chart.js""></script>
    <style>
        html, body { height: 100%; margin: 0; padding: 0; }
        .kpi-container { display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 20px; }
        .kpi-tile { min-width: 170px; flex: 1; box-shadow: 0 4px 12px rgba(0,0,0,0.06); border-radius: 8px; }
        .sapMPageEnableScrolling { padding: 1.5rem !important; }
        .code-snippet { background: #f4f6f8; padding: 12px; border-radius: 6px; font-family: Consolas, monospace; font-size: 13px; max-height: 250px; overflow: auto; border: 1px solid #e2e8f0; }
        .chart-grid { display: flex; flex-wrap: wrap; gap: 20px; margin-bottom: 24px; }
        .chart-card { flex: 1; min-width: 320px; background: rgba(255,255,255,0.7); backdrop-filter: blur(8px); border-radius: 12px; padding: 18px; border: 1px solid rgba(0,0,0,0.08); box-shadow: 0 4px 16px rgba(0,0,0,0.04); }
        .chart-title { font-size: 15px; font-weight: 600; margin-bottom: 12px; display: flex; align-items: center; gap: 8px; color: #1d2d3e; }
        .chart-wrapper { position: relative; height: 240px; width: 100%; }
        .sapUiTheme-sap_horizon_dark .chart-card { background: rgba(30, 40, 55, 0.7); border-color: rgba(255,255,255,0.1); color: #fff; }
        .sapUiTheme-sap_horizon_dark .chart-title { color: #edf2f7; }
    </style>
</head>
<body class=""sapUiBody"">
    <div id=""content""></div>

    <script>
    sap.ui.getCore().attachInit(function () {
        sap.ui.require([
            ""sap/m/App"",
            ""sap/m/Page"",
            ""sap/tnt/ToolPage"",
            ""sap/tnt/ToolHeader"",
            ""sap/tnt/SideNavigation"",
            ""sap/tnt/NavigationList"",
            ""sap/tnt/NavigationListItem"",
            ""sap/m/Title"",
            ""sap/m/Text"",
            ""sap/m/Button"",
            ""sap/m/Select"",
            ""sap/ui/core/Item"",
            ""sap/m/ToolbarSpacer"",
            ""sap/m/GenericTile"",
            ""sap/m/TileContent"",
            ""sap/m/NumericContent"",
            ""sap/m/Table"",
            ""sap/m/Column"",
            ""sap/m/ColumnListItem"",
            ""sap/m/ObjectIdentifier"",
            ""sap/m/ObjectStatus"",
            ""sap/m/ObjectNumber"",
            ""sap/m/SegmentedButton"",
            ""sap/m/SegmentedButtonItem"",
            ""sap/m/SearchField"",
            ""sap/m/Panel"",
            ""sap/m/Dialog"",
            ""sap/m/FormattedText"",
            ""sap/m/MessageToast"",
            ""sap/ui/model/json/JSONModel"",
            ""sap/ui/layout/Grid"",
            ""sap/ui/layout/VerticalLayout""
        ], function (
            App, Page, ToolPage, ToolHeader, SideNavigation, NavigationList, NavigationListItem,
            Title, Text, Button, Select, Item, ToolbarSpacer, GenericTile, TileContent, NumericContent,
            Table, Column, ColumnListItem, ObjectIdentifier, ObjectStatus, ObjectNumber,
            SegmentedButton, SegmentedButtonItem, SearchField, Panel, Dialog, FormattedText, MessageToast,
            JSONModel, Grid, VerticalLayout
        ) {
            // Models
            var oOverviewModel = new JSONModel();
            var oJobsModel = new JSONModel({ items: [], state: 'succeeded' });
            var oRecurringModel = new JSONModel({ items: [] });
            var oServersModel = new JSONModel({ items: [] });

            // Charts instances
            var doughnutChartInstance = null;
            var timelineChartInstance = null;

            // API Base URL
            var sApiBase = window.location.pathname.replace(/\/ui5\/?$/, '') + '/api/ui5';

            function updateCharts(data) {
                if (!window.Chart) return;

                // 1. Update Doughnut Chart (Job States)
                var doughnutCtx = document.getElementById('jobsDoughnutChart');
                if (doughnutCtx && data && data.stats) {
                    var stats = data.stats;
                    var chartData = [
                        stats.succeeded || 0,
                        stats.failed || 0,
                        stats.processing || 0,
                        stats.enqueued || 0,
                        stats.scheduled || 0,
                        stats.deleted || 0
                    ];

                    if (!doughnutChartInstance) {
                        doughnutChartInstance = new Chart(doughnutCtx, {
                            type: 'doughnut',
                            data: {
                                labels: ['Sucesso', 'Falhas', 'Processando', 'Enfileirados', 'Agendados', 'Deletados'],
                                datasets: [{
                                    data: chartData,
                                    backgroundColor: ['#107e3e', '#bb0000', '#0070f2', '#e9730c', '#9254de', '#6c757d'],
                                    borderWidth: 2
                                }]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: {
                                    legend: { position: 'right' }
                                },
                                animation: { duration: 600 }
                            }
                        });
                    } else {
                        doughnutChartInstance.data.datasets[0].data = chartData;
                        doughnutChartInstance.update();
                    }
                }

                // 2. Update Timeline Chart (Hourly Throughput)
                var timelineCtx = document.getElementById('jobsTimelineChart');
                if (timelineCtx && data && data.timeline) {
                    var labels = (data.timeline.hourlySucceeded || []).map(function(x) { return x.time; });
                    var succData = (data.timeline.hourlySucceeded || []).map(function(x) { return x.count; });
                    var failData = (data.timeline.hourlyFailed || []).map(function(x) { return x.count; });

                    if (labels.length === 0) {
                        labels = ['Agora'];
                        succData = [data.stats ? (data.stats.succeeded || 0) : 0];
                        failData = [data.stats ? (data.stats.failed || 0) : 0];
                    }

                    if (!timelineChartInstance) {
                        timelineChartInstance = new Chart(timelineCtx, {
                            type: 'line',
                            data: {
                                labels: labels,
                                datasets: [
                                    {
                                        label: 'Sucessos',
                                        data: succData,
                                        borderColor: '#107e3e',
                                        backgroundColor: 'rgba(16, 126, 62, 0.15)',
                                        fill: true,
                                        tension: 0.35
                                    },
                                    {
                                        label: 'Falhas',
                                        data: failData,
                                        borderColor: '#bb0000',
                                        backgroundColor: 'rgba(187, 0, 0, 0.15)',
                                        fill: true,
                                        tension: 0.35
                                    }
                                ]
                            },
                            options: {
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: {
                                    legend: { position: 'top' }
                                },
                                scales: {
                                    y: { beginAtZero: true }
                                },
                                animation: { duration: 600 }
                            }
                        });
                    } else {
                        timelineChartInstance.data.labels = labels;
                        timelineChartInstance.data.datasets[0].data = succData;
                        timelineChartInstance.data.datasets[1].data = failData;
                        timelineChartInstance.update();
                    }
                }
            }

            function loadOverview() {
                oOverviewModel.loadData(sApiBase + '/overview', null, true, 'GET');
            }

            oOverviewModel.attachRequestCompleted(function() {
                var oData = oOverviewModel.getData();
                setTimeout(function() { updateCharts(oData); }, 100);
            });

            function loadJobs(sState) {
                sState = sState || oJobsModel.getProperty('/state') || 'succeeded';
                oJobsModel.setProperty('/state', sState);
                oJobsModel.loadData(sApiBase + '/jobs?state=' + encodeURIComponent(sState) + '&count=100', null, true, 'GET');
            }

            function loadRecurring() {
                oRecurringModel.loadData(sApiBase + '/recurring', null, true, 'GET');
            }

            function loadServers() {
                oServersModel.loadData(sApiBase + '/servers', null, true, 'GET');
            }

            function refreshAll() {
                loadOverview();
                loadJobs();
                loadRecurring();
                loadServers();
                MessageToast.show('Dados atualizados em tempo real');
            }

            // Auto refresh timer
            var nAutoRefreshInterval = null;
            function setAutoRefresh(nSeconds) {
                if (nAutoRefreshInterval) {
                    clearInterval(nAutoRefreshInterval);
                    nAutoRefreshInterval = null;
                }
                if (nSeconds > 0) {
                    nAutoRefreshInterval = setInterval(refreshAll, nSeconds * 1000);
                }
            }

            // 1. OVERVIEW VIEW
            function createOverviewView() {
                var oKpiContainer = new sap.ui.core.HTML({
                    content: '<div class=""kpi-container"">' +
                        '<div id=""kpi-enqueued""></div>' +
                        '<div id=""kpi-processing""></div>' +
                        '<div id=""kpi-succeeded""></div>' +
                        '<div id=""kpi-failed""></div>' +
                        '<div id=""kpi-recurring""></div>' +
                        '<div id=""kpi-docs""></div>' +
                        '<div id=""kpi-size""></div>' +
                    '</div>',
                    afterRendering: function() {
                        new GenericTile({
                            header: 'Jobs Enfileirados',
                            subheader: 'Na fila',
                            tileContent: new TileContent({
                                content: new NumericContent({
                                    value: '{/stats/enqueued}',
                                    valueColor: 'Neutral',
                                    indicator: 'None'
                                })
                            })
                        }).setModel(oOverviewModel).placeAt('kpi-enqueued');

                        new GenericTile({
                            header: 'Em Processamento',
                            subheader: 'Executando agora',
                            tileContent: new TileContent({
                                content: new NumericContent({
                                    value: '{/stats/processing}',
                                    valueColor: 'Neutral',
                                    icon: 'sap-icon://process'
                                })
                            })
                        }).setModel(oOverviewModel).placeAt('kpi-processing');

                        new GenericTile({
                            header: 'Concluídos',
                            subheader: 'Sucesso',
                            tileContent: new TileContent({
                                content: new NumericContent({
                                    value: '{/stats/succeeded}',
                                    valueColor: 'Good',
                                    icon: 'sap-icon://sys-enter-2'
                                })
                            })
                        }).setModel(oOverviewModel).placeAt('kpi-succeeded');

                        new GenericTile({
                            header: 'Falhas',
                            subheader: 'Com erro',
                            tileContent: new TileContent({
                                content: new NumericContent({
                                    value: '{/stats/failed}',
                                    valueColor: 'Critical',
                                    icon: 'sap-icon://error'
                                })
                            })
                        }).setModel(oOverviewModel).placeAt('kpi-failed');

                        new GenericTile({
                            header: 'Jobs Recorrentes',
                            subheader: 'Agendados no Cron',
                            tileContent: new TileContent({
                                content: new NumericContent({
                                    value: '{/stats/recurring}',
                                    valueColor: 'Neutral',
                                    icon: 'sap-icon://history'
                                })
                            })
                        }).setModel(oOverviewModel).placeAt('kpi-recurring');

                        new GenericTile({
                            header: 'RavenDB Documentos',
                            subheader: 'Total na base',
                            tileContent: new TileContent({
                                content: new NumericContent({
                                    value: '{/ravendb/documentsCount}',
                                    valueColor: 'Neutral',
                                    icon: 'sap-icon://documents'
                                })
                            })
                        }).setModel(oOverviewModel).placeAt('kpi-docs');

                        new GenericTile({
                            header: 'RavenDB Tamanho',
                            subheader: 'Espaço em disco',
                            tileContent: new TileContent({
                                content: new NumericContent({
                                    value: '{/ravendb/sizeOnDisk}',
                                    valueColor: 'Good',
                                    icon: 'sap-icon://database'
                                })
                            })
                        }).setModel(oOverviewModel).placeAt('kpi-size');
                    }
                });

                var oChartsContainer = new sap.ui.core.HTML({
                    content: '<div class=""chart-grid"">' +
                        '<div class=""chart-card"">' +
                            '<div class=""chart-title"">📊 Distribuição dos Jobs por Estado</div>' +
                            '<div class=""chart-wrapper""><canvas id=""jobsDoughnutChart""></canvas></div>' +
                        '</div>' +
                        '<div class=""chart-card"">' +
                            '<div class=""chart-title"">📈 Throughput em Tempo Real (Sucesso vs Falha)</div>' +
                            '<div class=""chart-wrapper""><canvas id=""jobsTimelineChart""></canvas></div>' +
                        '</div>' +
                    '</div>',
                    afterRendering: function() {
                        setTimeout(function() {
                            updateCharts(oOverviewModel.getData());
                        }, 200);
                    }
                });

                var oServersOverviewTable = new Table({
                    headerText: 'Servidores Ativos no Cluster',
                    columns: [
                        new Column({ header: new Text({ text: 'Servidor / Host' }) }),
                        new Column({ header: new Text({ text: 'Workers' }) }),
                        new Column({ header: new Text({ text: 'Filas Atendidas' }) }),
                        new Column({ header: new Text({ text: 'Último Heartbeat' }) }),
                        new Column({ header: new Text({ text: 'Status' }) })
                    ],
                    items: {
                        path: '/servers',
                        template: new ColumnListItem({
                            cells: [
                                new ObjectIdentifier({ title: '{name}' }),
                                new ObjectNumber({ number: '{workersCount}', state: 'Information' }),
                                new Text({ text: '{= ${queues} ? ${queues}.join("", "") : ""default"" }' }),
                                new Text({ text: '{heartbeat}' }),
                                new ObjectStatus({ text: 'Online', state: 'Success', icon: 'sap-icon://sys-enter-2' })
                            ]
                        })
                    }
                }).setModel(oOverviewModel);

                var oQueuesTable = new Table({
                    headerText: 'Filas Ativas (Queues)',
                    columns: [
                        new Column({ header: new Text({ text: 'Fila' }) }),
                        new Column({ header: new Text({ text: 'Jobs Pendentes' }) }),
                        new Column({ header: new Text({ text: 'Jobs Processados (Fetched)' }) })
                    ],
                    items: {
                        path: '/queues',
                        template: new ColumnListItem({
                            cells: [
                                new ObjectIdentifier({ title: '{name}' }),
                                new ObjectNumber({ number: '{length}', state: 'Information' }),
                                new ObjectNumber({ number: '{fetched}', state: 'None' })
                            ]
                        })
                    }
                }).setModel(oOverviewModel);

                var oIndexesTable = new Table({
                    headerText: 'Índices RavenDB & Saúde',
                    columns: [
                        new Column({ header: new Text({ text: 'Índice' }) }),
                        new Column({ header: new Text({ text: 'Tipo' }) }),
                        new Column({ header: new Text({ text: 'Estado' }) }),
                        new Column({ header: new Text({ text: 'Status' }) })
                    ],
                    items: {
                        path: '/ravendb/indexes',
                        template: new ColumnListItem({
                            cells: [
                                new ObjectIdentifier({ title: '{name}' }),
                                new Text({ text: '{type}' }),
                                new Text({ text: '{state}' }),
                                new ObjectStatus({
                                    text: '{= ${isStale} ? ""Stale"" : ""Up to date"" }',
                                    state: '{= ${isStale} ? ""Warning"" : ""Success"" }'
                                })
                            ]
                        })
                    }
                }).setModel(oOverviewModel);

                return new Page({
                    title: 'Visão Geral & Métricas do Cluster',
                    content: [
                        oKpiContainer,
                        oChartsContainer,
                        new Panel({ headerText: 'Servidores Ativos no Cluster', content: [oServersOverviewTable] }),
                        new Panel({ headerText: 'Resumo das Filas de Mensageria', content: [oQueuesTable] }),
                        new Panel({ headerText: 'Índices RavenDB e Desempenho', content: [oIndexesTable] })
                    ]
                });
            }

            // 2. JOBS EXPLORER VIEW
            function createJobsView() {
                var oStateButtons = new SegmentedButton({
                    selectedKey: 'succeeded',
                    selectionChange: function (oEvent) {
                        loadJobs(oEvent.getParameter('item').getKey());
                    },
                    items: [
                        new SegmentedButtonItem({ text: 'Sucesso', key: 'succeeded', icon: 'sap-icon://sys-enter-2' }),
                        new SegmentedButtonItem({ text: 'Falhas', key: 'failed', icon: 'sap-icon://error' }),
                        new SegmentedButtonItem({ text: 'Processando', key: 'processing', icon: 'sap-icon://process' }),
                        new SegmentedButtonItem({ text: 'Enfileirados', key: 'enqueued', icon: 'sap-icon://list' }),
                        new SegmentedButtonItem({ text: 'Agendados', key: 'scheduled', icon: 'sap-icon://future' }),
                        new SegmentedButtonItem({ text: 'Deletados', key: 'deleted', icon: 'sap-icon://delete' })
                    ]
                });

                var oJobsTable = new Table({
                    headerText: 'Histórico de Execuções',
                    columns: [
                        new Column({ header: new Text({ text: 'Job ID' }), width: '220px' }),
                        new Column({ header: new Text({ text: 'Classe / Método' }) }),
                        new Column({ header: new Text({ text: 'Argumentos' }) }),
                        new Column({ header: new Text({ text: 'Data / Duração' }) }),
                        new Column({ header: new Text({ text: 'Detalhes' }), width: '100px' })
                    ],
                    items: {
                        path: '/items',
                        template: new ColumnListItem({
                            cells: [
                                new ObjectIdentifier({ title: '{id}' }),
                                new Text({ text: '{= ${job/type} ? (${job/type} + ""."" + ${job/method}) : ""N/A"" }' }),
                                new Text({ text: '{job/arguments}' }),
                                new Text({ text: '{= ${succeededAt} || ${failedAt} || ${startedAt} || ${enqueuedAt} || ${deletedAt} || ""Recente"" }' }),
                                new Button({
                                    icon: 'sap-icon://inspect',
                                    type: 'Transparent',
                                    press: function (oEvent) {
                                        var oContext = oEvent.getSource().getBindingContext();
                                        var oData = oContext.getObject();

                                        var sContent = '<p><strong>ID:</strong> ' + oData.id + '</p>' +
                                                       '<p><strong>Método:</strong> ' + (oData.job ? (oData.job.type + '.' + oData.job.method) : 'N/A') + '</p>' +
                                                       '<p><strong>Argumentos:</strong> ' + (oData.job ? oData.job.arguments : '') + '</p>';
                                        
                                        if (oData.exceptionMessage) {
                                            sContent += '<p style=""color: #b00;""><strong>Erro:</strong> ' + oData.exceptionMessage + '</p>' +
                                                        '<div class=""code-snippet"">' + (oData.exceptionDetails || oData.exceptionType) + '</div>';
                                        }

                                        var oRevisionsModel = new JSONModel();
                                        oRevisionsModel.loadData(sApiBase + '/job-revisions?id=' + encodeURIComponent(oData.id));

                                        var oRevisionsTable = new Table({
                                            headerText: 'Trilha de Auditoria Imutável (RavenDB Document Revisions)',
                                            columns: [
                                                new Column({ header: new Text({ text: 'Estado' }), width: '130px' }),
                                                new Column({ header: new Text({ text: 'Data / Hora' }) }),
                                                new Column({ header: new Text({ text: 'Informações da Transição' }) })
                                            ],
                                            items: {
                                                path: '/items',
                                                template: new ColumnListItem({
                                                    cells: [
                                                        new ObjectStatus({
                                                            text: '{stateName}',
                                                            state: '{= ${stateName} === ""Succeeded"" ? ""Success"" : (${stateName} === ""Failed"" ? ""Error"" : ""None"") }'
                                                        }),
                                                        new Text({ text: '{timestamp}' }),
                                                        new Text({ text: '{= ${reason} || ""Estado persistido com sucesso no cluster RavenDB"" }' })
                                                    ]
                                                })
                                            }
                                        }).setModel(oRevisionsModel);

                                        var oDialog = new Dialog({
                                            title: 'Auditoria & Detalhes do Job: ' + oData.id,
                                            contentWidth: '700px',
                                            type: 'Message',
                                            content: [
                                                new FormattedText({ htmlText: sContent }),
                                                new Panel({ headerText: 'Histórico de Revisões (Compliance)', content: [oRevisionsTable] })
                                            ],
                                            beginButton: new Button({
                                                text: 'Fechar',
                                                press: function () { oDialog.close(); }
                                            }),
                                            afterClose: function () { oDialog.destroy(); }
                                        });
                                        oDialog.open();
                                    }
                                })
                            ]
                        })
                    }
                }).setModel(oJobsModel);

                return new Page({
                    title: 'Jobs Explorer',
                    content: [
                        new Panel({ content: [oStateButtons] }),
                        oJobsTable
                    ]
                });
            }

            // 3. RECURRING JOBS VIEW
            function createRecurringView() {
                var oTable = new Table({
                    headerText: 'Jobs Recorrentes Configurados',
                    columns: [
                        new Column({ header: new Text({ text: 'Identificador' }) }),
                        new Column({ header: new Text({ text: 'Expressão Cron' }) }),
                        new Column({ header: new Text({ text: 'Fila' }) }),
                        new Column({ header: new Text({ text: 'Próxima Execução' }) }),
                        new Column({ header: new Text({ text: 'Última Execução' }) }),
                        new Column({ header: new Text({ text: 'Último Status' }) })
                    ],
                    items: {
                        path: '/items',
                        template: new ColumnListItem({
                            cells: [
                                new ObjectIdentifier({ title: '{id}', text: '{= ${job/type} + ""."" + ${job/method} }' }),
                                new Text({ text: '{cron}' }),
                                new Text({ text: '{queue}' }),
                                new Text({ text: '{nextExecution}' }),
                                new Text({ text: '{lastExecution}' }),
                                new ObjectStatus({
                                    text: '{lastJobState}',
                                    state: '{= ${lastJobState} === ""Succeeded"" ? ""Success"" : (${lastJobState} === ""Failed"" ? ""Error"" : ""None"") }'
                                })
                            ]
                        })
                    }
                }).setModel(oRecurringModel);

                return new Page({
                    title: 'Jobs Recorrentes (Cron)',
                    content: [oTable]
                });
            }

            // 4. SERVERS VIEW
            function createServersView() {
                var oTable = new Table({
                    headerText: 'Servidores Hangfire Ativos no Cluster',
                    columns: [
                        new Column({ header: new Text({ text: 'Servidor / Host' }) }),
                        new Column({ header: new Text({ text: 'Trabalhadores (Workers)' }) }),
                        new Column({ header: new Text({ text: 'Filas Atendidas' }) }),
                        new Column({ header: new Text({ text: 'Último Heartbeat' }) }),
                        new Column({ header: new Text({ text: 'Status' }) })
                    ],
                    items: {
                        path: '/items',
                        template: new ColumnListItem({
                            cells: [
                                new ObjectIdentifier({ title: '{name}' }),
                                new ObjectNumber({ number: '{workersCount}', state: 'Information' }),
                                new Text({ text: '{= ${queues} ? ${queues}.join("", "") : ""default"" }' }),
                                new Text({ text: '{heartbeat}' }),
                                new ObjectStatus({ text: 'Online', state: 'Success', icon: 'sap-icon://sys-enter-2' })
                            ]
                        })
                    }
                }).setModel(oServersModel);

                return new Page({
                    title: 'Servidores & Nós de Execução',
                    content: [oTable]
                });
            }

            // 5. RAVENDB HEALTH VIEW
            function createRavenHealthView() {
                var oDbInfo = new Panel({
                    headerText: 'Informações do Banco RavenDB',
                    content: [
                        new VerticalLayout({
                            content: [
                                new Text({ text: 'Nome da Base: {/ravendb/databaseName}' }),
                                new Text({ text: 'ID do Cluster: {/ravendb/databaseId}' }),
                                new Text({ text: 'Tamanho em Disco: {/ravendb/sizeOnDisk}' }),
                                new Text({ text: 'Total de Documentos: {/ravendb/documentsCount}' })
                            ]
                        }).setModel(oOverviewModel)
                    ]
                });

                return new Page({
                    title: 'RavenDB Health & Métricas',
                    content: [oDbInfo]
                });
            }

            // Navigation and ToolPage Shell
            var oOverviewPage = createOverviewView();
            var oJobsPage = createJobsView();
            var oRecurringPage = createRecurringView();
            var oServersPage = createServersView();
            var oRavenHealthPage = createRavenHealthView();

            var oApp = new App({
                pages: [oOverviewPage, oJobsPage, oRecurringPage, oServersPage, oRavenHealthPage]
            });

            var oSideNav = new SideNavigation({
                item: new NavigationList({
                    selectedKey: 'overview',
                    itemSelect: function(oEvent) {
                        var sKey = oEvent.getParameter('item').getKey();
                        switch(sKey) {
                            case 'overview':
                                oApp.to(oOverviewPage);
                                loadOverview();
                                break;
                            case 'jobs':
                                oApp.to(oJobsPage);
                                loadJobs();
                                break;
                            case 'recurring':
                                oApp.to(oRecurringPage);
                                loadRecurring();
                                break;
                            case 'servers':
                                oApp.to(oServersPage);
                                loadServers();
                                break;
                            case 'ravendb':
                                oApp.to(oRavenHealthPage);
                                loadOverview();
                                break;
                        }
                    },
                    items: [
                        new NavigationListItem({ text: 'Visão Geral', icon: 'sap-icon://home', key: 'overview' }),
                        new NavigationListItem({ text: 'Jobs Explorer', icon: 'sap-icon://activity-items', key: 'jobs' }),
                        new NavigationListItem({ text: 'Jobs Recorrentes', icon: 'sap-icon://history', key: 'recurring' }),
                        new NavigationListItem({ text: 'Servidores', icon: 'sap-icon://server', key: 'servers' }),
                        new NavigationListItem({ text: 'RavenDB Health', icon: 'sap-icon://database', key: 'ravendb' })
                    ]
                })
            });

            var oHeader = new ToolHeader({
                content: [
                    new Button({
                        icon: 'sap-icon://menu2',
                        type: 'Transparent',
                        press: function () {
                            oSideNav.setExpanded(!oSideNav.getExpanded());
                        }
                    }),
                    new Title({ text: 'Hangfire — RavenDB' }),
                    new ObjectStatus({ text: 'SAP Fiori Horizon', state: 'Information' }),
                    new ToolbarSpacer(),
                    new Text({ text: 'Auto-Refresh:' }),
                    new Select({
                        selectedKey: '10',
                        change: function(oEvent) {
                            var nSec = parseInt(oEvent.getParameter('selectedItem').getKey(), 10);
                            setAutoRefresh(nSec);
                        },
                        items: [
                            new Item({ key: '0', text: 'Desligado' }),
                            new Item({ key: '5', text: '5 segundos' }),
                            new Item({ key: '10', text: '10 segundos' }),
                            new Item({ key: '30', text: '30 segundos' })
                        ]
                    }),
                    new Button({
                        icon: 'sap-icon://refresh',
                        text: 'Atualizar',
                        type: 'Emphasized',
                        press: refreshAll
                    }),
                    new Button({
                        icon: 'sap-icon://palette',
                        tooltip: 'Alternar Tema Claro / Escuro',
                        type: 'Transparent',
                        press: function () {
                            var sCurrent = sap.ui.getCore().getConfiguration().getTheme();
                            var sNext = sCurrent === 'sap_horizon' ? 'sap_horizon_dark' : 'sap_horizon';
                            sap.ui.getCore().applyTheme(sNext);
                            MessageToast.show('Tema alterado para: ' + sNext);
                        }
                    }),
                    new Button({
                        icon: 'sap-icon://action',
                        text: 'Dashboard Clássico',
                        type: 'Default',
                        press: function () {
                            var sClassic = window.location.pathname.replace(/\/ui5\/?$/, '');
                            window.location.href = sClassic || '/hangfire';
                        }
                    })
                ]
            });

            var oToolPage = new ToolPage({
                header: oHeader,
                sideContent: oSideNav,
                mainContents: [oApp]
            });

            oToolPage.placeAt('content');

            // Initial load & 10s default timer
            refreshAll();
            setAutoRefresh(10);
        });
    });
    </script>
</body>
</html>";
        }
    }
}
