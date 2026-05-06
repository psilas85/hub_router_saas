// hub_router_1.0.1/frontend/src/pages/Exploratory/ExploratoryDashboardPage.tsx

import { useState, Fragment } from "react";
import {
    BarChart,
    Bar,
    LineChart,
    Line,
    ComposedChart,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
    LabelList,
    ReferenceLine,
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
import { fieldLabel, INFO_TIPS } from "./labels";
import { InfoTip } from "./InfoTip";

// ─── formatters ──────────────────────────────────────────────────────────────

function fmt(n: number | undefined | null, decimals = 0) {
    if (n == null) return "—";
    return n.toLocaleString("pt-BR", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
    });
}

function fmtBRL(n: number | undefined | null) {
    if (n == null) return "—";
    return n.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function fmtBRLShort(n: number): string {
    if (n >= 1_000_000) return `R$${(n / 1_000_000).toFixed(1)}M`;
    if (n >= 1_000) return `R$${(n / 1_000).toFixed(0)}k`;
    return fmtBRL(n);
}

function corrColor(r: number): string {
    const abs = Math.abs(r);
    if (r > 0) return `rgba(220,38,38,${0.15 + abs * 0.85})`;
    return `rgba(37,99,235,${0.15 + abs * 0.85})`;
}

function pctClass(pct: number, warn = 5, danger = 15): string {
    if (pct >= danger) return "bg-red-100 text-red-700 font-semibold";
    if (pct >= warn) return "bg-amber-100 text-amber-700 font-semibold";
    return "bg-green-100 text-green-700";
}

// ─── shared UI ───────────────────────────────────────────────────────────────

function KPICard({ label, value, sub }: { label: string; value: string; sub?: string }) {
    return (
        <div className="bg-white border rounded-xl p-4 text-center shadow-sm hover:shadow-md transition-shadow">
            <p className="text-xs text-gray-400 uppercase tracking-widest mb-1">{label}</p>
            <p className="text-2xl font-bold text-gray-800">{value}</p>
            {sub && <p className="text-xs text-gray-400 mt-1">{sub}</p>}
        </div>
    );
}

function SectionTitle({
    children,
    tipKey,
    align,
}: {
    children: React.ReactNode;
    tipKey?: string;
    align?: "left" | "right";
}) {
    const tip = tipKey ? INFO_TIPS[tipKey] : null;
    return (
        <h3 className="text-sm font-semibold text-gray-600 uppercase tracking-wide mb-2 mt-5 flex items-center gap-1">
            {children}
            {tip && <InfoTip {...tip} align={align} />}
        </h3>
    );
}

function TabDescription({ children }: { children: React.ReactNode }) {
    return <p className="text-sm text-gray-500 mb-4 leading-relaxed">{children}</p>;
}

function EmptyState({ msg }: { msg?: string }) {
    return (
        <div className="flex flex-col items-center justify-center py-12 text-gray-300">
            <span className="text-4xl mb-2">📭</span>
            <p className="text-sm">{msg ?? "Sem dados para o período selecionado."}</p>
        </div>
    );
}

function Spinner() {
    return (
        <div className="flex flex-col items-center justify-center py-16 gap-3">
            <div className="animate-spin rounded-full h-10 w-10 border-b-2 border-blue-600" />
            <p className="text-sm text-gray-400">Carregando análises…</p>
        </div>
    );
}

function PctBadge({ value, warn, danger }: { value: number; warn?: number; danger?: number }) {
    return (
        <span className={`px-2 py-0.5 rounded-full text-xs ${pctClass(value, warn, danger)}`}>
            {value}%
        </span>
    );
}

// ─── custom recharts tooltips ────────────────────────────────────────────────

function TooltipQtd({ active, payload, label }: any) {
    if (!active || !payload?.length) return null;
    return (
        <div className="bg-white border rounded-lg shadow-lg p-2 text-xs">
            <p className="font-semibold text-gray-700 mb-1">{label}</p>
            {payload.map((p: any) => (
                <p key={p.name} style={{ color: p.color }}>
                    {p.name}: <b>{fmt(p.value)}</b>
                </p>
            ))}
        </div>
    );
}

function TooltipBRL({ active, payload, label }: any) {
    if (!active || !payload?.length) return null;
    return (
        <div className="bg-white border rounded-lg shadow-lg p-2 text-xs">
            <p className="font-semibold text-gray-700 mb-1">{label}</p>
            {payload.map((p: any) => (
                <p key={p.name} style={{ color: p.color }}>
                    {p.name}: <b>{fmtBRL(p.value)}</b>
                </p>
            ))}
        </div>
    );
}

function TooltipHist({ active, payload, label }: any) {
    if (!active || !payload?.length) return null;
    const bar = payload.find((p: any) => p.dataKey === "count");
    const line = payload.find((p: any) => p.dataKey === "pct_acumulado");
    return (
        <div className="bg-white border rounded-lg shadow-lg p-2 text-xs">
            <p className="font-semibold text-gray-700 mb-1">Faixa: {label}</p>
            {bar && <p className="text-gray-600">Entregas nesta faixa: <b>{fmt(bar.value)}</b></p>}
            {line != null && (
                <p className="text-gray-500 mt-0.5">
                    Acumulado: <b>{line.value}%</b>
                    {line.value >= 80 && (
                        <span className="ml-1 text-red-500 font-semibold">← 80% atingido</span>
                    )}
                </p>
            )}
        </div>
    );
}

// ─── tab panels ──────────────────────────────────────────────────────────────

function ResumoPanel({ d }: { d: ResumoDados }) {
    const t = d.totais;
    const c = d.cobertura_datas;
    const nulosData = Object.entries(d.nulos_pct).map(([k, v]) => ({
        campo: fieldLabel(k),
        pct: v,
    }));

    return (
        <div>
            <TabDescription>
                Visão geral consolidada do período: total de entregas, valor movimentado e qualidade
                dos dados cadastrados.
            </TabDescription>

            <SectionTitle tipKey="resumo_totais">Totais do período</SectionTitle>
            <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
                <KPICard label="Entregas" value={fmt(t.total_entregas)} />
                <KPICard label="Peso total" value={`${fmt(t.total_peso, 1)} kg`} />
                <KPICard label="Volumes" value={fmt(t.total_volumes)} />
                <KPICard label="Valor das NFs" value={fmtBRL(t.total_valor_nf)} />
                <KPICard label="Receita de frete" value={fmtBRL(t.total_valor_frete)} />
            </div>

            <SectionTitle>Cobertura do período</SectionTitle>
            <div className="grid grid-cols-3 gap-3">
                <KPICard label="Primeiro envio" value={c.data_minima ?? "—"} />
                <KPICard label="Último envio" value={c.data_maxima ?? "—"} />
                <KPICard label="Dias cobertos" value={fmt(c.dias_cobertos)} />
            </div>

            <SectionTitle tipKey="resumo_nulos">Dados faltando por campo (%)</SectionTitle>
            {nulosData.length ? (
                <ResponsiveContainer width="100%" height={210}>
                    <BarChart data={nulosData} margin={{ top: 4, right: 16, bottom: 48, left: 0 }}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} />
                        <XAxis
                            dataKey="campo"
                            tick={{ fontSize: 11, fill: "#6b7280" }}
                            angle={-25}
                            textAnchor="end"
                            interval={0}
                        />
                        <YAxis unit="%" tick={{ fontSize: 11, fill: "#6b7280" }} width={36} />
                        <Tooltip
                            formatter={(v: number) => [`${v}%`, "Campos faltando"]}
                            labelFormatter={(l) => `Campo: ${l}`}
                        />
                        <Bar dataKey="pct" fill="#3b82f6" name="Faltando %" radius={[4, 4, 0, 0]}>
                            <LabelList
                                dataKey="pct"
                                position="top"
                                formatter={(v: number) => (v > 0 ? `${v}%` : "")}
                                style={{ fontSize: 10, fill: "#374151" }}
                            />
                        </Bar>
                    </BarChart>
                </ResponsiveContainer>
            ) : (
                <EmptyState />
            )}
        </div>
    );
}

function QualidadePanel({ d }: { d: QualidadeDados }) {
    return (
        <div>
            <TabDescription>
                Diagnóstico da qualidade dos dados: identifica valores extremos, campos zerados e
                informações essenciais faltando — base para confiar nos resultados das demais análises.
            </TabDescription>

            <SectionTitle tipKey="outliers_iqr">Valores extremos por campo</SectionTitle>
            {d.outliers_iqr.length ? (
                <div className="overflow-x-auto">
                    <table className="w-full text-sm border-collapse">
                        <thead>
                            <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wide">
                                <th className="px-3 py-2 rounded-tl">Campo</th>
                                <th className="px-3 py-2 text-right">Total</th>
                                <th className="px-3 py-2 text-right">Extremos</th>
                                <th className="px-3 py-2 text-center">% extremos</th>
                                <th className="px-3 py-2 text-right">Limite mín.</th>
                                <th className="px-3 py-2 text-right rounded-tr">Limite máx.</th>
                            </tr>
                        </thead>
                        <tbody>
                            {d.outliers_iqr.map((row, i) => (
                                <tr
                                    key={row.coluna}
                                    className={`border-t ${i % 2 === 0 ? "" : "bg-gray-50/50"}`}
                                >
                                    <td className="px-3 py-2 font-medium text-gray-700">
                                        {fieldLabel(row.coluna)}
                                    </td>
                                    <td className="px-3 py-2 text-right text-gray-500">
                                        {fmt(row.total_observacoes)}
                                    </td>
                                    <td className="px-3 py-2 text-right text-gray-700">
                                        {fmt(row.outliers)}
                                    </td>
                                    <td className="px-3 py-2 text-center">
                                        <PctBadge value={row.percentual} warn={3} danger={10} />
                                    </td>
                                    <td className="px-3 py-2 text-right text-gray-500 text-xs">
                                        {fmt(row.lim_inf, 2)}
                                    </td>
                                    <td className="px-3 py-2 text-right text-gray-500 text-xs">
                                        {fmt(row.lim_sup, 2)}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : (
                <EmptyState msg="Nenhum outlier identificado." />
            )}

            <SectionTitle tipKey="zerados">Zeros e dados em branco</SectionTitle>
            {d.zerados.length ? (
                <div className="overflow-x-auto">
                    <table className="w-full text-sm border-collapse">
                        <thead>
                            <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wide">
                                <th className="px-3 py-2 rounded-tl">Campo</th>
                                <th className="px-3 py-2 text-right">Zerados</th>
                                <th className="px-3 py-2 text-center">% zero</th>
                                <th className="px-3 py-2 text-right">Em branco</th>
                                <th className="px-3 py-2 text-center rounded-tr">% em branco</th>
                            </tr>
                        </thead>
                        <tbody>
                            {d.zerados.map((row, i) => (
                                <tr
                                    key={row.coluna}
                                    className={`border-t ${i % 2 === 0 ? "" : "bg-gray-50/50"}`}
                                >
                                    <td className="px-3 py-2 font-medium text-gray-700">
                                        {fieldLabel(row.coluna)}
                                    </td>
                                    <td className="px-3 py-2 text-right text-gray-500">
                                        {fmt(row.zerados)}
                                    </td>
                                    <td className="px-3 py-2 text-center">
                                        <PctBadge value={row.pct_zerados} warn={5} danger={20} />
                                    </td>
                                    <td className="px-3 py-2 text-right text-gray-500">
                                        {fmt(row.nulos)}
                                    </td>
                                    <td className="px-3 py-2 text-center">
                                        <PctBadge value={row.pct_nulos} warn={5} danger={20} />
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : (
                <EmptyState msg="Nenhum zero ou nulo identificado." />
            )}

            {d.campos_criticos_faltando.length > 0 && (
                <>
                    <SectionTitle tipKey="campos_criticos">Campos essenciais incompletos</SectionTitle>
                    <ul className="space-y-1.5">
                        {d.campos_criticos_faltando.map((item) => (
                            <li
                                key={item.campo}
                                className="flex items-center gap-2 text-sm bg-red-50 border border-red-100 rounded-lg px-3 py-2"
                            >
                                <span className="text-red-400">⚠</span>
                                <span className="font-medium text-gray-700">
                                    {fieldLabel(item.campo)}
                                </span>
                                <span className="text-gray-500 text-xs">
                                    — {fmt(item.faltando)} registros sem dado ({item.pct}%)
                                </span>
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

    const granLabel =
        d.granularidade === "mensal"
            ? "mês"
            : d.granularidade === "anual"
                ? "ano"
                : "dia";

    return (
        <div>
            <TabDescription>
                Evolução das entregas ao longo do tempo. Identifique sazonalidade, tendências de
                crescimento e períodos atípicos.
            </TabDescription>

            <SectionTitle tipKey="temporal">Entregas por {granLabel}</SectionTitle>
            <ResponsiveContainer width="100%" height={260}>
                <BarChart data={d.series} margin={{ top: 4, right: 16, bottom: 48, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <XAxis
                        dataKey="periodo"
                        tick={{ fontSize: 10, fill: "#6b7280" }}
                        angle={-30}
                        textAnchor="end"
                        interval={0}
                    />
                    <YAxis
                        tick={{ fontSize: 11, fill: "#6b7280" }}
                        width={50}
                        tickFormatter={(v) => fmt(v)}
                    />
                    <Tooltip content={<TooltipQtd />} />
                    <Bar
                        dataKey="qtd_entregas"
                        fill="#3b82f6"
                        name="Entregas"
                        radius={[4, 4, 0, 0]}
                    />
                </BarChart>
            </ResponsiveContainer>

            <SectionTitle>Valor da NF e receita de frete por {granLabel}</SectionTitle>
            <ResponsiveContainer width="100%" height={260}>
                <LineChart data={d.series} margin={{ top: 4, right: 16, bottom: 48, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <XAxis
                        dataKey="periodo"
                        tick={{ fontSize: 10, fill: "#6b7280" }}
                        angle={-30}
                        textAnchor="end"
                        interval={0}
                    />
                    <YAxis
                        tick={{ fontSize: 11, fill: "#6b7280" }}
                        width={70}
                        tickFormatter={fmtBRLShort}
                    />
                    <Tooltip content={<TooltipBRL />} />
                    <Legend />
                    <Line
                        dataKey="total_valor_nf"
                        stroke="#10b981"
                        name="Valor da NF"
                        dot={false}
                        strokeWidth={2}
                    />
                    <Line
                        dataKey="total_valor_frete"
                        stroke="#f59e0b"
                        name="Receita de frete"
                        dot={false}
                        strokeWidth={2}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}

function MiniHistogram({
    data,
    title,
    tipKey,
    color = "#6366f1",
}: {
    data: { bin_label: string; count: number }[];
    title: string;
    tipKey?: string;
    color?: string;
}) {
    if (!data.length) return <EmptyState msg={`Sem dados: ${title}`} />;

    const total = data.reduce((s, d) => s + d.count, 0);
    let running = 0;
    const paretoData = data.map((d) => {
        running += d.count;
        return {
            ...d,
            pct_acumulado: total > 0 ? Math.round((running / total) * 1000) / 10 : 0,
        };
    });

    return (
        <div>
            <p className="text-sm font-medium text-gray-600 mb-1 flex items-center">
                {title}
                {tipKey && INFO_TIPS[tipKey] && <InfoTip {...INFO_TIPS[tipKey]} />}
                <InfoTip {...INFO_TIPS.pareto} align="right" />
            </p>
            <ResponsiveContainer width="100%" height={180}>
                <ComposedChart data={paretoData} margin={{ top: 4, right: 36, bottom: 4, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} />
                    <XAxis dataKey="bin_label" tick={false} axisLine={false} />
                    <YAxis
                        yAxisId="left"
                        tick={{ fontSize: 9, fill: "#9ca3af" }}
                        width={36}
                        tickFormatter={(v) => fmt(v)}
                    />
                    <YAxis
                        yAxisId="right"
                        orientation="right"
                        domain={[0, 100]}
                        unit="%"
                        tick={{ fontSize: 9, fill: "#9ca3af" }}
                        width={32}
                    />
                    <Tooltip content={<TooltipHist />} />
                    <Bar
                        yAxisId="left"
                        dataKey="count"
                        fill={color}
                        name="Entregas"
                        radius={[2, 2, 0, 0]}
                        fillOpacity={0.85}
                    />
                    <Line
                        yAxisId="right"
                        dataKey="pct_acumulado"
                        stroke="#374151"
                        dot={false}
                        strokeWidth={1.5}
                        name="Acumulado %"
                    />
                    <ReferenceLine
                        yAxisId="right"
                        y={80}
                        stroke="#ef4444"
                        strokeDasharray="5 3"
                        label={{
                            value: "80%",
                            position: "insideTopRight",
                            fontSize: 9,
                            fill: "#ef4444",
                        }}
                    />
                </ComposedChart>
            </ResponsiveContainer>
        </div>
    );
}

function DistribuicaoPanel({ d }: { d: DistribuicaoDados }) {
    return (
        <div>
            <TabDescription>
                Como os valores se distribuem entre as entregas. Cada gráfico mostra a frequência de
                ocorrências em diferentes faixas — útil para entender o perfil típico da operação.
            </TabDescription>
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-6">
                <MiniHistogram data={d.peso} title="Peso por entrega (kg)" color="#3b82f6" />
                <MiniHistogram data={d.valor_nf} title="Valor da NF por entrega (R$)" color="#10b981" />
                <MiniHistogram data={d.valor_frete} title="Receita de frete por entrega (R$)" color="#f59e0b" />
                <MiniHistogram data={d.volumes} title="Volumes por entrega" color="#8b5cf6" />
                <MiniHistogram
                    data={d.frete_sobre_nf}
                    title="Frete como % da NF"
                    tipKey="frete_sobre_nf"
                    color="#ef4444"
                />
            </div>
        </div>
    );
}

function RankingsPanel({ d }: { d: RankingsDados }) {
    const freqData = d.top_frequencia.slice(0, 10).map((r) => ({
        nome: `${(r.destinatario_nome ?? "").substring(0, 22)}${(r.destinatario_nome ?? "").length > 22 ? "…" : ""} (${r.cte_uf})`,
        "Entregas": r.qtd_entregas ?? 0,
    }));

    const nfData = d.top_valor_nf.slice(0, 10).map((r) => ({
        nome: `${(r.destinatario_nome ?? "").substring(0, 22)}${(r.destinatario_nome ?? "").length > 22 ? "…" : ""} (${r.cte_uf})`,
        "Valor NF": r.valor_total_nf ?? 0,
    }));

    return (
        <div>
            <TabDescription>
                Os principais clientes e cidades do período — base para priorizar rotas, renegociar
                contratos e identificar oportunidades de crescimento.
            </TabDescription>

            <SectionTitle tipKey="rankings">Top 10 clientes por número de entregas</SectionTitle>
            {freqData.length ? (
                <ResponsiveContainer width="100%" height={320}>
                    <BarChart
                        layout="vertical"
                        data={freqData}
                        margin={{ top: 4, right: 60, bottom: 4, left: 175 }}
                    >
                        <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                        <XAxis
                            type="number"
                            tick={{ fontSize: 10, fill: "#6b7280" }}
                            tickFormatter={(v) => fmt(v)}
                        />
                        <YAxis
                            type="category"
                            dataKey="nome"
                            tick={{ fontSize: 11, fill: "#374151" }}
                            width={170}
                        />
                        <Tooltip content={<TooltipQtd />} />
                        <Bar dataKey="Entregas" fill="#3b82f6" radius={[0, 4, 4, 0]}>
                            <LabelList
                                dataKey="Entregas"
                                position="right"
                                formatter={(v: number) => fmt(v)}
                                style={{ fontSize: 10, fill: "#6b7280" }}
                            />
                        </Bar>
                    </BarChart>
                </ResponsiveContainer>
            ) : (
                <EmptyState />
            )}

            <SectionTitle>Top 10 clientes por valor total de NF</SectionTitle>
            {nfData.length ? (
                <ResponsiveContainer width="100%" height={320}>
                    <BarChart
                        layout="vertical"
                        data={nfData}
                        margin={{ top: 4, right: 90, bottom: 4, left: 175 }}
                    >
                        <CartesianGrid strokeDasharray="3 3" horizontal={false} />
                        <XAxis
                            type="number"
                            tick={{ fontSize: 10, fill: "#6b7280" }}
                            tickFormatter={fmtBRLShort}
                        />
                        <YAxis
                            type="category"
                            dataKey="nome"
                            tick={{ fontSize: 11, fill: "#374151" }}
                            width={170}
                        />
                        <Tooltip content={<TooltipBRL />} />
                        <Bar dataKey="Valor NF" fill="#10b981" radius={[0, 4, 4, 0]}>
                            <LabelList
                                dataKey="Valor NF"
                                position="right"
                                formatter={fmtBRLShort}
                                style={{ fontSize: 10, fill: "#6b7280" }}
                            />
                        </Bar>
                    </BarChart>
                </ResponsiveContainer>
            ) : (
                <EmptyState />
            )}

            <SectionTitle>Top 15 cidades por volume de entregas</SectionTitle>
            {d.top_cidades.length ? (
                <div className="overflow-x-auto">
                    <table className="w-full text-sm border-collapse">
                        <thead>
                            <tr className="bg-gray-50 text-left text-xs text-gray-500 uppercase tracking-wide">
                                <th className="px-3 py-2 rounded-tl">#</th>
                                <th className="px-3 py-2">Cidade</th>
                                <th className="px-3 py-2">UF</th>
                                <th className="px-3 py-2 text-right">Entregas</th>
                                <th className="px-3 py-2 text-right rounded-tr">Valor total NF</th>
                            </tr>
                        </thead>
                        <tbody>
                            {d.top_cidades.slice(0, 15).map((row, i) => (
                                <tr
                                    key={`${row.cte_cidade}-${row.cte_uf}`}
                                    className={`border-t ${i % 2 === 0 ? "" : "bg-gray-50/50"}`}
                                >
                                    <td className="px-3 py-1.5 text-gray-400 text-xs">{i + 1}</td>
                                    <td className="px-3 py-1.5 font-medium text-gray-700">
                                        {row.cte_cidade}
                                    </td>
                                    <td className="px-3 py-1.5 text-gray-500">{row.cte_uf}</td>
                                    <td className="px-3 py-1.5 text-right text-gray-700">
                                        {fmt(row.qtd_entregas)}
                                    </td>
                                    <td className="px-3 py-1.5 text-right text-gray-700">
                                        {fmtBRL(row.valor_total_nf)}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            ) : (
                <EmptyState />
            )}
        </div>
    );
}

function GeograficoPanel({ d }: { d: GeograficoDados }) {
    if (!d.pontos.length) return <EmptyState msg="Nenhuma entrega com coordenadas geográficas." />;

    const center: [number, number] = [d.pontos[0].lat, d.pontos[0].lon];
    const maxNF = Math.max(...d.pontos.map((p) => p.valor_nf), 1);

    function pontoColor(valor: number): string {
        const ratio = valor / maxNF;
        if (ratio > 0.66) return "#dc2626";
        if (ratio > 0.33) return "#f59e0b";
        return "#16a34a";
    }

    const pctComCoord =
        d.total_com_coordenadas + d.total_sem_coordenadas > 0
            ? Math.round(
                (d.total_com_coordenadas /
                    (d.total_com_coordenadas + d.total_sem_coordenadas)) *
                100
            )
            : 0;

    return (
        <div>
            <TabDescription>
                Onde estão concentradas as entregas. Útil para visualizar cobertura geográfica,
                identificar regiões desatendidas e planejar a abertura de novos hubs.
            </TabDescription>

            <div className="flex flex-wrap gap-4 text-sm mb-3">
                <span className="flex items-center gap-1.5">
                    <span className="w-2 h-2 rounded-full bg-blue-500 inline-block" />
                    Com coordenadas:{" "}
                    <b className="text-gray-700">{fmt(d.total_com_coordenadas)}</b>
                    <span className="text-gray-400">({pctComCoord}%)</span>
                </span>
                <span className="flex items-center gap-1.5">
                    <span className="w-2 h-2 rounded-full bg-gray-300 inline-block" />
                    Sem coordenadas:{" "}
                    <b className="text-gray-500">{fmt(d.total_sem_coordenadas)}</b>
                </span>
                {d.pontos.length < d.total_com_coordenadas && (
                    <span className="text-amber-600 text-xs flex items-center gap-1">
                        ⚠ Exibindo amostra de {fmt(d.pontos.length)} pontos
                        <InfoTip
                            comercial={`Volume total de ${fmt(d.total_com_coordenadas)} pontos — amostrado em 5.000 para desempenho do mapa.`}
                        />
                    </span>
                )}
            </div>

            <div style={{ height: 500 }} className="rounded-xl border overflow-hidden shadow-sm">
                <MapContainer center={center} zoom={7} style={{ height: "100%", width: "100%" }}>
                    <TileLayer
                        attribution="© OpenStreetMap"
                        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                    />
                    {d.pontos.map((p, i) => (
                        <CircleMarker
                            key={i}
                            center={[p.lat, p.lon]}
                            radius={5}
                            pathOptions={{
                                color: pontoColor(p.valor_nf),
                                fillOpacity: 0.75,
                                weight: 1,
                            }}
                        >
                            <Popup>
                                <div className="text-xs space-y-0.5">
                                    <p className="font-semibold">{p.destinatario_nome || "—"}</p>
                                    <p className="text-gray-500">{p.cidade}</p>
                                    <p className="text-gray-700">{fmtBRL(p.valor_nf)}</p>
                                </div>
                            </Popup>
                        </CircleMarker>
                    ))}
                </MapContainer>
            </div>

            <div className="flex gap-5 mt-2.5 text-xs text-gray-500">
                <span className="flex items-center gap-1.5">
                    <span className="w-3 h-3 rounded-full bg-green-600 inline-block" />
                    Baixo valor de NF
                </span>
                <span className="flex items-center gap-1.5">
                    <span className="w-3 h-3 rounded-full bg-amber-500 inline-block" />
                    Valor médio
                </span>
                <span className="flex items-center gap-1.5">
                    <span className="w-3 h-3 rounded-full bg-red-600 inline-block" />
                    Alto valor de NF
                </span>
            </div>
        </div>
    );
}

function CorrelacaoPanel({ d }: { d: CorrelacaoDados }) {
    if (!d.matriz.length) return <EmptyState />;
    const n = d.variaveis.length;

    return (
        <div>
            <TabDescription>
                O quanto as variáveis numéricas se relacionam entre si. Útil para entender padrões —
                por exemplo, se entregas de maior peso também geram maior receita de frete.
            </TabDescription>

            <SectionTitle tipKey="correlacao">Matriz de correlação</SectionTitle>

            <div
                className="inline-grid gap-1.5"
                style={{ gridTemplateColumns: `140px repeat(${n}, 90px)` }}
            >
                {/* header row */}
                <div />
                {d.variaveis.map((v) => (
                    <div
                        key={v}
                        className="text-center text-xs font-medium text-gray-500 px-1 leading-tight"
                        title={v}
                    >
                        {fieldLabel(v)}
                    </div>
                ))}

                {/* data rows */}
                {d.variaveis.map((vx) => (
                    <Fragment key={vx}>
                        <div className="text-xs font-medium text-gray-500 flex items-center pr-2 leading-tight">
                            {fieldLabel(vx)}
                        </div>
                        {d.variaveis.map((vy) => {
                            const cell = d.matriz.find(
                                (m) => m.var_x === vx && m.var_y === vy
                            );
                            const r = cell?.r ?? 0;
                            const isIdentity = vx === vy;
                            return (
                                <div
                                    key={`${vx}-${vy}`}
                                    className="h-16 flex flex-col items-center justify-center rounded-lg text-xs font-bold gap-0.5"
                                    style={{
                                        backgroundColor: isIdentity
                                            ? "#f3f4f6"
                                            : corrColor(r),
                                        color:
                                            !isIdentity && Math.abs(r) > 0.45
                                                ? "white"
                                                : "#374151",
                                    }}
                                    title={`${fieldLabel(vx)} × ${fieldLabel(vy)}: r = ${r}`}
                                >
                                    <span>{r.toFixed(2)}</span>
                                    {!isIdentity && (
                                        <span className="text-[9px] font-normal opacity-80">
                                            {Math.abs(r) >= 0.7
                                                ? "forte"
                                                : Math.abs(r) >= 0.3
                                                    ? "moderada"
                                                    : "fraca"}
                                        </span>
                                    )}
                                </div>
                            );
                        })}
                    </Fragment>
                ))}
            </div>

            <p className="text-xs text-gray-400 mt-3">
                Passe o cursor sobre cada célula para ver o par de variáveis e o valor exato.
            </p>
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
            <TabDescription>
                Distribuição das entregas ao longo do calendário — identifica picos operacionais e
                ajuda a planejar capacidade de frota e equipe.
            </TabDescription>

            <SectionTitle tipKey="concentracao_fim_mes">
                Concentração nos últimos 5 dias úteis do mês
            </SectionTitle>
            {fimMesData.length ? (
                <ResponsiveContainer width="100%" height={280}>
                    <BarChart
                        data={fimMesData}
                        margin={{ top: 20, right: 16, bottom: 48, left: 0 }}
                    >
                        <CartesianGrid strokeDasharray="3 3" vertical={false} />
                        <XAxis
                            dataKey="periodo"
                            tick={{ fontSize: 10, fill: "#6b7280" }}
                            angle={-30}
                            textAnchor="end"
                            interval={0}
                        />
                        <YAxis tick={{ fontSize: 11, fill: "#6b7280" }} width={50} />
                        <Tooltip content={<TooltipQtd />} />
                        <Legend />
                        <Bar
                            dataKey="Resto do mês"
                            stackId="a"
                            fill="#93c5fd"
                            radius={[0, 0, 0, 0]}
                        />
                        <Bar
                            dataKey="Últ. 5 dias úteis"
                            stackId="a"
                            fill="#f97316"
                            radius={[4, 4, 0, 0]}
                        >
                            <LabelList
                                dataKey="pct"
                                position="top"
                                formatter={(v: number) => (v > 0 ? `${v}%` : "")}
                                style={{ fontSize: 10, fill: "#374151", fontWeight: 600 }}
                            />
                        </Bar>
                    </BarChart>
                </ResponsiveContainer>
            ) : (
                <EmptyState />
            )}

            <SectionTitle tipKey="concentracao_dia_semana">
                Entregas por dia da semana
            </SectionTitle>
            {d.dia_semana.length ? (
                <ResponsiveContainer width="100%" height={210}>
                    <BarChart
                        data={d.dia_semana}
                        margin={{ top: 16, right: 16, bottom: 8, left: 0 }}
                    >
                        <CartesianGrid strokeDasharray="3 3" vertical={false} />
                        <XAxis dataKey="dia" tick={{ fontSize: 11, fill: "#6b7280" }} />
                        <YAxis
                            tick={{ fontSize: 11, fill: "#6b7280" }}
                            width={50}
                            tickFormatter={(v) => fmt(v)}
                        />
                        <Tooltip content={<TooltipQtd />} />
                        <Bar dataKey="qtd_entregas" name="Entregas" fill="#6366f1" radius={[4, 4, 0, 0]}>
                            <LabelList
                                dataKey="qtd_entregas"
                                position="top"
                                formatter={(v: number) => fmt(v)}
                                style={{ fontSize: 9, fill: "#6b7280" }}
                            />
                        </Bar>
                    </BarChart>
                </ResponsiveContainer>
            ) : (
                <EmptyState />
            )}

            <SectionTitle tipKey="concentracao_dia_mes">
                Entregas por dia do mês
            </SectionTitle>
            {d.dia_mes.length ? (
                <ResponsiveContainer width="100%" height={200}>
                    <BarChart
                        data={d.dia_mes}
                        margin={{ top: 16, right: 16, bottom: 8, left: 0 }}
                    >
                        <CartesianGrid strokeDasharray="3 3" vertical={false} />
                        <XAxis dataKey="dia" tick={{ fontSize: 10, fill: "#6b7280" }} />
                        <YAxis
                            tick={{ fontSize: 11, fill: "#6b7280" }}
                            width={50}
                            tickFormatter={(v) => fmt(v)}
                        />
                        <Tooltip content={<TooltipQtd />} />
                        <Bar dataKey="qtd_entregas" name="Entregas" fill="#0ea5e9" radius={[4, 4, 0, 0]} />
                    </BarChart>
                </ResponsiveContainer>
            ) : (
                <EmptyState />
            )}
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

type TabKey = (typeof TABS)[number]["key"];

// ─── main page ───────────────────────────────────────────────────────────────

type ExploratoryDashboardPageProps = {
    embedded?: boolean;
};

export default function ExploratoryDashboardPage({ embedded = false }: ExploratoryDashboardPageProps) {
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

    const params: EDAParams = {
        data_inicial: dataInicial,
        data_final: dataFinal,
        granularidade,
    };

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
            toast.success("Análise carregada com sucesso");
        } catch {
            toast.error("Erro ao carregar as análises. Verifique o período selecionado.");
        } finally {
            setLoading(false);
        }
    }

    const hasData = resumo !== null;

    return (
        <div className={embedded ? "max-w-6xl mx-auto px-6 pb-6" : "p-5 max-w-6xl mx-auto"}>
            {!embedded ? (
                <div className="mb-5">
                    <h1 className="text-2xl font-bold text-gray-800">Explorador de Dados</h1>
                    <p className="text-sm text-gray-400 mt-0.5">
                        Diagnóstico completo da operação logística no período selecionado
                    </p>
                </div>
            ) : null}

            {/* Filter bar */}
            <div className="flex flex-wrap gap-4 items-end bg-white border rounded-xl p-4 mb-5 shadow-sm">
                <div>
                    <label className="block text-xs font-medium text-gray-500 mb-1">
                        Data inicial
                    </label>
                    <input
                        type="date"
                        value={dataInicial}
                        onChange={(e) => setDataInicial(e.target.value)}
                        className="border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-300 focus:outline-none"
                    />
                </div>
                <div>
                    <label className="block text-xs font-medium text-gray-500 mb-1">
                        Data final
                    </label>
                    <input
                        type="date"
                        value={dataFinal}
                        onChange={(e) => setDataFinal(e.target.value)}
                        className="border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-300 focus:outline-none"
                    />
                </div>
                <div>
                    <label className="block text-xs font-medium text-gray-500 mb-1 flex items-center gap-1">
                        Granularidade
                        <InfoTip
                            comercial="Define como os dados são agrupados nos gráficos temporais: por dia, mês ou ano."
                        />
                    </label>
                    <select
                        value={granularidade}
                        onChange={(e) =>
                            setGranularidade(e.target.value as EDAParams["granularidade"])
                        }
                        className="border rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-300 focus:outline-none"
                    >
                        <option value="diaria">Diária</option>
                        <option value="mensal">Mensal</option>
                        <option value="anual">Anual</option>
                    </select>
                </div>
                <button
                    onClick={handleAnalisar}
                    disabled={loading}
                    className="ml-auto px-5 py-2 bg-blue-600 hover:bg-blue-700 active:bg-blue-800 text-white rounded-lg text-sm font-semibold disabled:opacity-50 transition-colors shadow-sm"
                >
                    {loading ? "Carregando…" : "Analisar"}
                </button>
            </div>

            {loading && <Spinner />}

            {!loading && !hasData && (
                <div className="flex flex-col items-center justify-center py-20 text-gray-300">
                    <span className="text-5xl mb-3">📊</span>
                    <p className="text-base">
                        Selecione o período e clique em <b className="text-gray-400">Analisar</b>
                    </p>
                </div>
            )}

            {!loading && hasData && (
                <>
                    {/* Tab nav */}
                    <div className="flex flex-wrap gap-1 border-b mb-1">
                        {TABS.map((tab) => (
                            <button
                                key={tab.key}
                                onClick={() => setActiveTab(tab.key)}
                                className={`px-4 py-2 text-sm font-medium rounded-t-lg transition-colors ${activeTab === tab.key
                                        ? "bg-white border border-b-white -mb-px text-blue-600 shadow-sm"
                                        : "text-gray-500 hover:text-gray-700 hover:bg-gray-50"
                                    }`}
                            >
                                {tab.label}
                            </button>
                        ))}
                    </div>

                    {/* Tab content */}
                    <div className="bg-white border border-t-0 rounded-b-xl rounded-tr-xl p-5 shadow-sm">
                        {activeTab === "resumo" && resumo && <ResumoPanel d={resumo} />}
                        {activeTab === "qualidade" && qualidade && (
                            <QualidadePanel d={qualidade} />
                        )}
                        {activeTab === "temporal" && temporal && <TemporalPanel d={temporal} />}
                        {activeTab === "distribuicao" && distribuicao && (
                            <DistribuicaoPanel d={distribuicao} />
                        )}
                        {activeTab === "rankings" && rankings && <RankingsPanel d={rankings} />}
                        {activeTab === "geografico" && geografico && (
                            <GeograficoPanel d={geografico} />
                        )}
                        {activeTab === "correlacao" && correlacao && (
                            <CorrelacaoPanel d={correlacao} />
                        )}
                        {activeTab === "concentracao" && concentracao && (
                            <ConcentracaoPanel d={concentracao} />
                        )}
                    </div>
                </>
            )}
        </div>
    );
}
