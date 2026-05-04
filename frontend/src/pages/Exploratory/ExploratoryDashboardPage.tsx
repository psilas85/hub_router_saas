// hub_router_1.0.1/frontend/src/pages/Exploratory/ExploratoryDashboardPage.tsx

import { useState } from "react";
import {
    BarChart,
    Bar,
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
} from "recharts";
import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import toast from "react-hot-toast";
import {
    fetchResumo,
    fetchQualidade,
    fetchTemporal,
    fetchDistribuicao,
    fetchRankings,
    fetchGeografico,
    fetchCorrelacao,
    fetchConcentracao,
} from "@/services/exploratoryApi";
import type {
    EDAParams,
    ResumoDados,
    QualidadeDados,
    TemporalDados,
    DistribuicaoDados,
    RankingsDados,
    GeograficoDados,
    CorrelacaoDados,
    ConcentracaoDados,
} from "@/services/exploratoryApi";

// ─── helpers ─────────────────────────────────────────────────────────────────

function fmt(n: number | undefined | null, decimals = 0) {
    if (n == null) return "—";
    return n.toLocaleString("pt-BR", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function fmtBRL(n: number | undefined | null) {
    if (n == null) return "—";
    return n.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function corrColor(r: number): string {
    const abs = Math.abs(r);
    if (r > 0) return `rgba(220,38,38,${abs})`;  // red for positive
    return `rgba(37,99,235,${abs})`;              // blue for negative
}

// ─── tiny reusable components ────────────────────────────────────────────────

function KPICard({ label, value }: { label: string; value: string }) {
    return (
        <div className="bg-white border rounded-lg p-4 text-center shadow-sm">
            <p className="text-xs text-gray-500 uppercase tracking-wide">{label}</p>
            <p className="text-xl font-bold text-gray-800 mt-1">{value}</p>
        </div>
    );
}

function SectionTitle({ children }: { children: React.ReactNode }) {
    return <h3 className="text-base font-semibold text-gray-700 mb-2 mt-4">{children}</h3>;
}

function EmptyState({ msg }: { msg?: string }) {
    return <p className="text-gray-400 text-sm py-8 text-center">{msg ?? "Sem dados para exibir."}</p>;
}

function Spinner() {
    return (
        <div className="flex items-center justify-center py-16">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
        </div>
    );
}

// ─── tab panels ──────────────────────────────────────────────────────────────

function ResumoPanel({ d }: { d: ResumoDados }) {
    const t = d.totais;
    const c = d.cobertura_datas;
    const nulosData = Object.entries(d.nulos_pct).map(([k, v]) => ({ campo: k, pct: v }));

    return (
        <div>
            <SectionTitle>Totais do período</SectionTitle>
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
                <KPICard label="Entregas" value={fmt(t.total_entregas)} />
                <KPICard label="Peso (kg)" value={fmt(t.total_peso, 1)} />
                <KPICard label="Volumes" value={fmt(t.total_volumes)} />
                <KPICard label="Valor NF" value={fmtBRL(t.total_valor_nf)} />
                <KPICard label="Frete" value={fmtBRL(t.total_valor_frete)} />
            </div>

            <SectionTitle>Cobertura</SectionTitle>
            <div className="grid grid-cols-3 gap-3">
                <KPICard label="Data mínima" value={c.data_minima ?? "—"} />
                <KPICard label="Data máxima" value={c.data_maxima ?? "—"} />
                <KPICard label="Dias cobertos" value={fmt(c.dias_cobertos)} />
            </div>

            <SectionTitle>Nulos por campo (%)</SectionTitle>
            {nulosData.length ? (
                <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={nulosData} margin={{ top: 0, right: 10, bottom: 40, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="campo" tick={{ fontSize: 11 }} angle={-30} textAnchor="end" />
                        <YAxis unit="%" />
                        <Tooltip formatter={(v: number) => `${v}%`} />
                        <Bar dataKey="pct" fill="#3b82f6" name="Nulos %" radius={[3, 3, 0, 0]} />
                    </BarChart>
                </ResponsiveContainer>
            ) : <EmptyState />}
        </div>
    );
}

function QualidadePanel({ d }: { d: QualidadeDados }) {
    return (
        <div>
            <SectionTitle>Outliers por IQR</SectionTitle>
            {d.outliers_iqr.length ? (
                <div className="overflow-x-auto">
                    <table className="w-full text-sm border-collapse">
                        <thead>
                            <tr className="bg-gray-100 text-left">
                                <th className="px-3 py-2">Campo</th>
                                <th className="px-3 py-2 text-right">Total obs.</th>
                                <th className="px-3 py-2 text-right">Outliers</th>
                                <th className="px-3 py-2 text-right">%</th>
                                <th className="px-3 py-2 text-right">Lim. inf.</th>
                                <th className="px-3 py-2 text-right">Lim. sup.</th>
                            </tr>
                        </thead>
                        <tbody>
                            {d.outliers_iqr.map((row) => (
                                <tr key={row.coluna} className="border-t">
                                    <td className="px-3 py-1.5 font-mono text-xs">{row.coluna}</td>
                                    <td className="px-3 py-1.5 text-right">{fmt(row.total_observacoes)}</td>
                                    <td className="px-3 py-1.5 text-right">{fmt(row.outliers)}</td>
                                    <td className="px-3 py-1.5 text-right">{row.percentual}%</td>
                                    <td className="px-3 py-1.5 text-right">{fmt(row.lim_inf, 2)}</td>
                                    <td className="px-3 py-1.5 text-right">{fmt(row.lim_sup, 2)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : <EmptyState />}

            <SectionTitle>Zerados e nulos</SectionTitle>
            {d.zerados.length ? (
                <div className="overflow-x-auto">
                    <table className="w-full text-sm border-collapse">
                        <thead>
                            <tr className="bg-gray-100 text-left">
                                <th className="px-3 py-2">Campo</th>
                                <th className="px-3 py-2 text-right">Zerados</th>
                                <th className="px-3 py-2 text-right">% zero</th>
                                <th className="px-3 py-2 text-right">Nulos</th>
                                <th className="px-3 py-2 text-right">% nulo</th>
                            </tr>
                        </thead>
                        <tbody>
                            {d.zerados.map((row) => (
                                <tr key={row.coluna} className="border-t">
                                    <td className="px-3 py-1.5 font-mono text-xs">{row.coluna}</td>
                                    <td className="px-3 py-1.5 text-right">{fmt(row.zerados)}</td>
                                    <td className="px-3 py-1.5 text-right">{row.pct_zerados}%</td>
                                    <td className="px-3 py-1.5 text-right">{fmt(row.nulos)}</td>
                                    <td className="px-3 py-1.5 text-right">{row.pct_nulos}%</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : <EmptyState />}

            {d.campos_criticos_faltando.length > 0 && (
                <>
                    <SectionTitle>Campos críticos com dados faltando</SectionTitle>
                    <ul className="space-y-1">
                        {d.campos_criticos_faltando.map((item) => (
                            <li key={item.campo} className="text-sm text-red-600">
                                <span className="font-mono">{item.campo}</span>: {fmt(item.faltando)} registros ({item.pct}%)
                            </li>
                        ))}
                    </ul>
                </>
            )}
        </div>
    );
}

function TemporalPanel({ d }: { d: TemporalDados }) {
    if (!d.series.length) return <EmptyState />;
    return (
        <div>
            <SectionTitle>Entregas por período</SectionTitle>
            <ResponsiveContainer width="100%" height={250}>
                <BarChart data={d.series} margin={{ top: 0, right: 10, bottom: 40, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="periodo" tick={{ fontSize: 10 }} angle={-30} textAnchor="end" />
                    <YAxis />
                    <Tooltip />
                    <Bar dataKey="qtd_entregas" fill="#3b82f6" name="Entregas" radius={[3, 3, 0, 0]} />
                </BarChart>
            </ResponsiveContainer>

            <SectionTitle>Valor NF e Frete por período</SectionTitle>
            <ResponsiveContainer width="100%" height={250}>
                <LineChart data={d.series} margin={{ top: 0, right: 10, bottom: 40, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="periodo" tick={{ fontSize: 10 }} angle={-30} textAnchor="end" />
                    <YAxis />
                    <Tooltip formatter={(v: number) => fmtBRL(v)} />
                    <Legend />
                    <Line dataKey="total_valor_nf" stroke="#10b981" name="Valor NF" dot={false} />
                    <Line dataKey="total_valor_frete" stroke="#f59e0b" name="Frete" dot={false} />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

function MiniHistogram({ data, title }: { data: { bin_label: string; count: number }[]; title: string }) {
    if (!data.length) return <EmptyState msg={`Sem dados: ${title}`} />;
    return (
        <div>
            <p className="text-sm font-medium text-gray-600 mb-1">{title}</p>
            <ResponsiveContainer width="100%" height={160}>
                <BarChart data={data} margin={{ top: 0, right: 4, bottom: 24, left: 0 }}>
                    <XAxis dataKey="bin_label" tick={false} />
                    <YAxis tick={{ fontSize: 9 }} width={32} />
                    <Tooltip labelFormatter={(l) => l} />
                    <Bar dataKey="count" fill="#6366f1" name="Frequência" />
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}

function DistribuicaoPanel({ d }: { d: DistribuicaoDados }) {
    return (
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
            <MiniHistogram data={d.peso} title="Peso (kg)" />
            <MiniHistogram data={d.valor_nf} title="Valor NF (R$)" />
            <MiniHistogram data={d.valor_frete} title="Frete (R$)" />
            <MiniHistogram data={d.volumes} title="Volumes" />
            <MiniHistogram data={d.frete_sobre_nf} title="Razão Frete / NF" />
        </div>
    );
}

function RankingsPanel({ d }: { d: RankingsDados }) {
    const freqData = d.top_frequencia.slice(0, 10).map((r) => ({
        nome: `${r.destinatario_nome.substring(0, 20)} (${r.cte_uf})`,
        qtd: r.qtd_entregas,
    }));
    const nfData = d.top_valor_nf.slice(0, 10).map((r) => ({
        nome: `${r.destinatario_nome.substring(0, 20)} (${r.cte_uf})`,
        valor: r.valor_total_nf,
    }));

    return (
        <div>
            <SectionTitle>Top 10 por frequência de entregas</SectionTitle>
            {freqData.length ? (
                <ResponsiveContainer width="100%" height={300}>
                    <BarChart layout="vertical" data={freqData} margin={{ top: 0, right: 30, bottom: 0, left: 160 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" />
                        <YAxis type="category" dataKey="nome" tick={{ fontSize: 11 }} width={155} />
                        <Tooltip />
                        <Bar dataKey="qtd" fill="#3b82f6" name="Entregas" />
                    </BarChart>
                </ResponsiveContainer>
            ) : <EmptyState />}

            <SectionTitle>Top 10 por valor total de NF</SectionTitle>
            {nfData.length ? (
                <ResponsiveContainer width="100%" height={300}>
                    <BarChart layout="vertical" data={nfData} margin={{ top: 0, right: 30, bottom: 0, left: 160 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis type="number" tickFormatter={(v) => `R$${(v / 1000).toFixed(0)}k`} />
                        <YAxis type="category" dataKey="nome" tick={{ fontSize: 11 }} width={155} />
                        <Tooltip formatter={(v: number) => fmtBRL(v)} />
                        <Bar dataKey="valor" fill="#10b981" name="Valor NF" />
                    </BarChart>
                </ResponsiveContainer>
            ) : <EmptyState />}

            <SectionTitle>Top cidades</SectionTitle>
            {d.top_cidades.length ? (
                <div className="overflow-x-auto">
                    <table className="w-full text-sm border-collapse">
                        <thead>
                            <tr className="bg-gray-100 text-left">
                                <th className="px-3 py-2">Cidade</th>
                                <th className="px-3 py-2">UF</th>
                                <th className="px-3 py-2 text-right">Entregas</th>
                                <th className="px-3 py-2 text-right">Valor NF</th>
                            </tr>
                        </thead>
                        <tbody>
                            {d.top_cidades.slice(0, 15).map((row) => (
                                <tr key={`${row.cte_cidade}-${row.cte_uf}`} className="border-t">
                                    <td className="px-3 py-1.5">{row.cte_cidade}</td>
                                    <td className="px-3 py-1.5">{row.cte_uf}</td>
                                    <td className="px-3 py-1.5 text-right">{fmt(row.qtd_entregas)}</td>
                                    <td className="px-3 py-1.5 text-right">{fmtBRL(row.valor_total_nf)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : <EmptyState />}
        </div>
    );
}

function GeograficoPanel({ d }: { d: GeograficoDados }) {
    if (!d.pontos.length) return <EmptyState msg="Nenhum ponto com coordenadas." />;

    const center: [number, number] = [d.pontos[0].lat, d.pontos[0].lon];
    const maxNF = Math.max(...d.pontos.map((p) => p.valor_nf), 1);

    function pontoColor(valor: number): string {
        const ratio = valor / maxNF;
        if (ratio > 0.66) return "#dc2626";
        if (ratio > 0.33) return "#f59e0b";
        return "#16a34a";
    }

    return (
        <div>
            <div className="flex gap-6 text-sm text-gray-600 mb-3">
                <span>Com coordenadas: <b>{fmt(d.total_com_coordenadas)}</b></span>
                <span>Sem coordenadas: <b>{fmt(d.total_sem_coordenadas)}</b></span>
                {d.pontos.length < d.total_com_coordenadas && (
                    <span className="text-amber-600">Exibindo amostra de {fmt(d.pontos.length)} pontos</span>
                )}
            </div>
            <div style={{ height: 500 }} className="rounded border overflow-hidden">
                <MapContainer center={center} zoom={7} style={{ height: "100%", width: "100%" }}>
                    <TileLayer
                        attribution="OpenStreetMap"
                        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    {d.pontos.map((p, i) => (
                        <CircleMarker
                            key={i}
                            center={[p.lat, p.lon]}
                            radius={5}
                            pathOptions={{ color: pontoColor(p.valor_nf), fillOpacity: 0.75, weight: 1 }}
                        >
                            <Popup>
                                <b>{p.destinatario_nome}</b><br />
                                {p.cidade}<br />
                                {fmtBRL(p.valor_nf)}
                            </Popup>
                        </CircleMarker>
                    ))}
                </MapContainer>
            </div>
            <div className="flex gap-4 mt-2 text-xs text-gray-500">
                <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-green-600 inline-block" /> Baixo valor</span>
                <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-amber-500 inline-block" /> Médio</span>
                <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-red-600 inline-block" /> Alto valor</span>
            </div>
        </div>
    );
}

function CorrelacaoPanel({ d }: { d: CorrelacaoDados }) {
    if (!d.matriz.length) return <EmptyState />;
    const n = d.variaveis.length;
    return (
        <div>
            <SectionTitle>Matriz de correlação</SectionTitle>
            <div
                className="inline-grid gap-1"
                style={{ gridTemplateColumns: `auto repeat(${n}, 80px)` }}
            >
                <div />
                {d.variaveis.map((v) => (
                    <div key={v} className="text-center text-xs font-mono text-gray-600 truncate px-1">{v}</div>
                ))}
                {d.variaveis.map((vx) => (
                    <>
                        <div key={`lbl-${vx}`} className="text-xs font-mono text-gray-600 flex items-center pr-2 whitespace-nowrap">{vx}</div>
                        {d.variaveis.map((vy) => {
                            const cell = d.matriz.find((m) => m.var_x === vx && m.var_y === vy);
                            const r = cell?.r ?? 0;
                            return (
                                <div
                                    key={`${vx}-${vy}`}
                                    className="h-14 flex items-center justify-center text-xs font-semibold rounded"
                                    style={{ backgroundColor: corrColor(r), color: Math.abs(r) > 0.5 ? "white" : "inherit" }}
                                    title={`${vx} × ${vy}: r=${r}`}
                                >
                                    {r.toFixed(2)}
                                </div>
                            );
                        })}
                    </>
                ))}
            </div>
        </div>
    );
}

function ConcentracaoPanel({ d }: { d: ConcentracaoDados }) {
    const fimMesData = d.fim_mes.map((r) => ({
        periodo: r.periodo,
        "Resto do mês": r.entregas_resto,
        "Últ. 5 dias úteis": r.entregas_ultimos_5uteis,
        pct: r.pct_ultimos_5uteis,
    }));

    return (
        <div>
            <SectionTitle>Concentração fim de mês (últimos 5 dias úteis)</SectionTitle>
            {fimMesData.length ? (
                <ResponsiveContainer width="100%" height={260}>
                    <BarChart data={fimMesData} margin={{ top: 0, right: 10, bottom: 40, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="periodo" tick={{ fontSize: 10 }} angle={-30} textAnchor="end" />
                        <YAxis />
                        <Tooltip />
                        <Legend />
                        <Bar dataKey="Resto do mês" stackId="a" fill="#93c5fd" />
                        <Bar dataKey="Últ. 5 dias úteis" stackId="a" fill="#f97316" radius={[3, 3, 0, 0]} />
                    </BarChart>
                </ResponsiveContainer>
            ) : <EmptyState />}

            <SectionTitle>Entregas por dia da semana</SectionTitle>
            {d.dia_semana.length ? (
                <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={d.dia_semana} margin={{ top: 0, right: 10, bottom: 10, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="dia" tick={{ fontSize: 11 }} />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="qtd_entregas" fill="#6366f1" name="Entregas" radius={[3, 3, 0, 0]} />
                    </BarChart>
                </ResponsiveContainer>
            ) : <EmptyState />}

            <SectionTitle>Entregas por dia do mês</SectionTitle>
            {d.dia_mes.length ? (
                <ResponsiveContainer width="100%" height={200}>
                    <BarChart data={d.dia_mes} margin={{ top: 0, right: 10, bottom: 10, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="dia" tick={{ fontSize: 10 }} />
                        <YAxis />
                        <Tooltip />
                        <Bar dataKey="qtd_entregas" fill="#0ea5e9" name="Entregas" radius={[3, 3, 0, 0]} />
                    </BarChart>
                </ResponsiveContainer>
            ) : <EmptyState />}
        </div>
    );
}

// ─── tabs config ──────────────────────────────────────────────────────────────

const TABS = [
    { key: "resumo", label: "Resumo" },
    { key: "qualidade", label: "Qualidade" },
    { key: "temporal", label: "Temporal" },
    { key: "distribuicao", label: "Distribuição" },
    { key: "rankings", label: "Rankings" },
    { key: "geografico", label: "Geográfico" },
    { key: "correlacao", label: "Correlação" },
    { key: "concentracao", label: "Concentração" },
] as const;

type TabKey = typeof TABS[number]["key"];

// ─── main page ───────────────────────────────────────────────────────────────

export default function ExploratoryDashboardPage() {
    const today = new Date().toISOString().split("T")[0];
    const sixMonthsAgo = new Date(Date.now() - 180 * 86400000).toISOString().split("T")[0];

    const [dataInicial, setDataInicial] = useState(sixMonthsAgo);
    const [dataFinal, setDataFinal] = useState(today);
    const [granularidade, setGranularidade] = useState<EDAParams["granularidade"]>("mensal");
    const [activeTab, setActiveTab] = useState<TabKey>("resumo");
    const [loading, setLoading] = useState(false);

    const [resumo, setResumo] = useState<ResumoDados | null>(null);
    const [qualidade, setQualidade] = useState<QualidadeDados | null>(null);
    const [temporal, setTemporal] = useState<TemporalDados | null>(null);
    const [distribuicao, setDistribuicao] = useState<DistribuicaoDados | null>(null);
    const [rankings, setRankings] = useState<RankingsDados | null>(null);
    const [geografico, setGeografico] = useState<GeograficoDados | null>(null);
    const [correlacao, setCorrelacao] = useState<CorrelacaoDados | null>(null);
    const [concentracao, setConcentracao] = useState<ConcentracaoDados | null>(null);

    const params: EDAParams = { data_inicial: dataInicial, data_final: dataFinal, granularidade };

    async function handleAnalisar() {
        setLoading(true);
        try {
            const [r, q, t, dist, rank, geo, corr, conc] = await Promise.all([
                fetchResumo(params),
                fetchQualidade(params),
                fetchTemporal(params),
                fetchDistribuicao(params),
                fetchRankings(params),
                fetchGeografico(params),
                fetchCorrelacao(params),
                fetchConcentracao(params),
            ]);
            setResumo(r);
            setQualidade(q);
            setTemporal(t);
            setDistribuicao(dist);
            setRankings(rank);
            setGeografico(geo);
            setCorrelacao(corr);
            setConcentracao(conc);
            toast.success("Análise concluída");
        } catch {
            toast.error("Erro ao carregar análises. Verifique os filtros.");
        } finally {
            setLoading(false);
        }
    }

    const hasData = resumo !== null;

    return (
        <div className="p-4 max-w-6xl mx-auto">
            <h1 className="text-xl font-bold text-gray-800 mb-4">Análise Exploratória</h1>

            {/* Filter bar */}
            <div className="flex flex-wrap gap-3 items-end bg-white border rounded-lg p-4 mb-4 shadow-sm">
                <div>
                    <label className="block text-xs text-gray-500 mb-1">Data inicial</label>
                    <input
                        type="date"
                        value={dataInicial}
                        onChange={(e) => setDataInicial(e.target.value)}
                        className="border rounded px-2 py-1.5 text-sm"
                    />
                </div>
                <div>
                    <label className="block text-xs text-gray-500 mb-1">Data final</label>
                    <input
                        type="date"
                        value={dataFinal}
                        onChange={(e) => setDataFinal(e.target.value)}
                        className="border rounded px-2 py-1.5 text-sm"
                    />
                </div>
                <div>
                    <label className="block text-xs text-gray-500 mb-1">Granularidade</label>
                    <select
                        value={granularidade}
                        onChange={(e) => setGranularidade(e.target.value as EDAParams["granularidade"])}
                        className="border rounded px-2 py-1.5 text-sm"
                    >
                        <option value="diaria">Diária</option>
                        <option value="mensal">Mensal</option>
                        <option value="anual">Anual</option>
                    </select>
                </div>
                <button
                    onClick={handleAnalisar}
                    disabled={loading}
                    className="ml-auto px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded text-sm font-medium disabled:opacity-50"
                >
                    {loading ? "Analisando…" : "Analisar"}
                </button>
            </div>

            {loading && <Spinner />}

            {!loading && !hasData && (
                <div className="text-center py-16 text-gray-400">
                    Selecione o período e clique em <b>Analisar</b> para iniciar.
                </div>
            )}

            {!loading && hasData && (
                <>
                    {/* Tabs */}
                    <div className="flex flex-wrap gap-1 border-b mb-4">
                        {TABS.map((tab) => (
                            <button
                                key={tab.key}
                                onClick={() => setActiveTab(tab.key)}
                                className={`px-3 py-2 text-sm font-medium rounded-t transition-colors ${
                                    activeTab === tab.key
                                        ? "bg-white border border-b-white -mb-px text-blue-600"
                                        : "text-gray-500 hover:text-gray-700"
                                }`}
                            >
                                {tab.label}
                            </button>
                        ))}
                    </div>

                    {/* Tab content */}
                    <div className="bg-white border rounded-lg p-4 shadow-sm">
                        {activeTab === "resumo" && resumo && <ResumoPanel d={resumo} />}
                        {activeTab === "qualidade" && qualidade && <QualidadePanel d={qualidade} />}
                        {activeTab === "temporal" && temporal && <TemporalPanel d={temporal} />}
                        {activeTab === "distribuicao" && distribuicao && <DistribuicaoPanel d={distribuicao} />}
                        {activeTab === "rankings" && rankings && <RankingsPanel d={rankings} />}
                        {activeTab === "geografico" && geografico && <GeograficoPanel d={geografico} />}
                        {activeTab === "correlacao" && correlacao && <CorrelacaoPanel d={correlacao} />}
                        {activeTab === "concentracao" && concentracao && <ConcentracaoPanel d={concentracao} />}
                    </div>
                </>
            )}
        </div>
    );
}
