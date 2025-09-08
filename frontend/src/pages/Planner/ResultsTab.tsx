//ResultsTab.tsx

import { useEffect, useState } from "react";
import { plan, planV2 } from "@/services/ml";
import {
    ResponsiveContainer,
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
} from "recharts";
import { fmtMoeda, fmtCompact } from "@/utils/format";

type ScenarioResult = {
    cenario: string;
    mes: string;
    custo_transfer_total?: number;
    custo_transferencia?: number;
    custo_last_mile: number;
    custo_total: number;
    frota?: Record<string, number>;
    hubs?: [string, string][];
    entregas?: number;
    peso?: number;
    volumes?: number;
    valor_nf?: number;
    mae?: number;
    mae_interval?: string;
    mse?: number;
    r2?: number;
    r2_interval?: string;
    accuracy?: number;
    f1?: number;
    roc_auc?: number;
};

const VALID_SCENARIOS = ["base", "baixo", "alto"] as const;
const SCENARIO_LABELS: Record<string, string> = {
    base: "Base (Referência)",
    baixo: "Otimista (Baixo)",
    alto: "Pessimista (Alto)",
};

export default function ResultsTab() {
    const [results, setResults] = useState<ScenarioResult[]>([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const controller = new AbortController();
        let timeoutId: number | undefined;

        async function fetchData() {
            setLoading(true);
            setError(null);

            try {
                timeoutId = window.setTimeout(() => controller.abort(), 20000);

                let content: any;
                try {
                    content = await planV2({
                        start_date: "2025-06-01",
                        months: 3,
                        scenarios: "base,baixo,alto",
                        fast: true,
                    });
                } catch {
                    content = await plan({
                        start_date: "2025-06-01",
                        months: 3,
                        scenarios: "base,baixo,alto",
                        fast: true,
                    });
                }

                if (!content || typeof content !== "object") {
                    throw new Error("Resposta inesperada do endpoint de planejamento.");
                }

                const parsed: ScenarioResult[] = [];
                Object.entries(content as Record<string, any>).forEach(([cenario, arr]) => {
                    // ❗ ignora chaves que não são cenários válidos
                    if (!VALID_SCENARIOS.includes(cenario as any)) return;

                    const list = Array.isArray(arr)
                        ? arr
                        : arr && typeof arr === "object"
                            ? [arr]
                            : [];

                    list.forEach((item: any) => {
                        parsed.push({
                            cenario,
                            mes: item.mes,
                            custo_transfer_total: item.custo_transfer_total,
                            custo_transferencia: item.custo_transferencia,
                            custo_last_mile: Number(item.custo_last_mile ?? 0),
                            custo_total: Number(item.custo_total ?? 0),
                            frota: item.frota,
                            hubs: item.hubs,
                            entregas: item.entregas,
                            peso: item.peso,
                            volumes: item.volumes,
                            valor_nf: item.valor_nf,
                            mae: item.mae,
                            mae_interval: item.mae_interval,
                            mse: item.mse,
                            r2: item.r2,
                            r2_interval: item.r2_interval,
                            accuracy: item.accuracy,
                            f1: item.f1,
                            roc_auc: item.roc_auc,
                        });
                    });
                });

                setResults(parsed);
            } catch (err: any) {
                if (err?.name === "AbortError") {
                    setError("⏱️ Tempo de resposta excedido. Tente novamente.");
                } else {
                    console.error("❌ Erro ao carregar resultados:", err);
                    setError(err?.response?.data?.detail ?? err.message ?? "Erro ao carregar resultados.");
                }
                setResults([]);
            } finally {
                if (timeoutId) clearTimeout(timeoutId);
                setLoading(false);
            }
        }

        fetchData();
        return () => {
            if (timeoutId) clearTimeout(timeoutId);
            controller.abort();
        };
    }, []);

    const handleExport = (format: "csv" | "json") => {
        if (!results.length) return;
        if (format === "json") {
            const blob = new Blob([JSON.stringify(results, null, 2)], { type: "application/json" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = "resultados.json";
            a.click();
            return;
        }
        const header =
            "Cenário,Mês,Custo Transferência,Custo Last Mile,Custo Total,MAE,MSE,R²,Frota,Hubs\n";
        const rows = results.map((r) => {
            const transf = r.custo_transfer_total ?? r.custo_transferencia ?? 0;
            return `${SCENARIO_LABELS[r.cenario] ?? r.cenario},${r.mes},${transf},${r.custo_last_mile},${r.custo_total},${r.mae ?? ""
                },${r.mse ?? ""},${r.r2 ?? ""},"${JSON.stringify(r.frota ?? {})}","${(r.hubs ?? []).map((h) => h.join(" ")).join("; ")
                }"`;
        });
        const blob = new Blob([header + rows.join("\n")], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "resultados.csv";
        a.click();
    };

    const aggregatedByScenario = Object.values(
        results.reduce<Record<string, any>>((acc, r) => {
            if (!acc[r.cenario]) {
                acc[r.cenario] = { ...r, count: 0, mae: 0, mse: 0, r2: 0 };
            }
            acc[r.cenario].count += 1;
            if (r.mae !== undefined) acc[r.cenario].mae += r.mae;
            if (r.mse !== undefined) acc[r.cenario].mse += r.mse;
            if (r.r2 !== undefined && r.r2 !== null) acc[r.cenario].r2 += r.r2;
            return acc;
        }, {})
    ).map((v) => ({
        ...v,
        mae: v.count ? v.mae / v.count : undefined,
        mse: v.count ? v.mse / v.count : undefined,
        r2: v.count ? v.r2 / v.count : undefined,
    }));

    return (
        <div className="space-y-6">
            <h2 className="text-xl font-bold">📊 Resultados Consolidados</h2>

            {loading && (
                <div className="p-3 bg-yellow-50 text-yellow-800 border border-yellow-200 rounded-lg">
                    ⏳ Carregando resultados consolidados...
                </div>
            )}

            {!loading && error && (
                <div className="p-3 bg-red-50 text-red-800 border border-red-200 rounded-lg">
                    {error}
                </div>
            )}

            {!loading && !error && results.length > 0 && (
                <>
                    <div className="p-3 bg-emerald-50 text-emerald-800 border border-emerald-200 rounded-lg">
                        ✅ Resultados carregados com sucesso!
                    </div>

                    <div className="bg-white rounded-xl shadow p-4">
                        <h3 className="font-semibold mb-3">Comparativo de Custos</h3>
                        <ResponsiveContainer width="100%" height={320}>
                            <BarChart
                                data={results.map((r) => ({
                                    ...r,
                                    cenario_label: SCENARIO_LABELS[r.cenario] ?? r.cenario,
                                    transf: r.custo_transfer_total ?? r.custo_transferencia ?? 0,
                                }))}
                            >
                                <XAxis dataKey="cenario_label" />
                                <YAxis tickFormatter={(v) => fmtCompact(Number(v))} />
                                <Tooltip formatter={(v: any, n) => [fmtMoeda(Number(v)), n]} />
                                <Legend />
                                <Bar dataKey="transf" fill="#3b82f6" name="Transferência" />
                                <Bar dataKey="custo_last_mile" fill="#f59e0b" name="Last Mile" />
                                <Bar dataKey="custo_total" fill="#10b981" name="Total" />
                            </BarChart>
                        </ResponsiveContainer>
                    </div>

                    <div className="bg-white rounded-xl shadow p-4 overflow-x-auto">
                        <table className="min-w-full text-sm border border-gray-200 divide-y divide-gray-200">
                            <thead className="bg-gray-50">
                                <tr>
                                    <th className="p-2 text-left">Cenário</th>
                                    <th className="p-2">Mês</th>
                                    <th className="p-2">Custo Transferência</th>
                                    <th className="p-2">Custo Last Mile</th>
                                    <th className="p-2">Custo Total</th>
                                    <th className="p-2">Entregas</th>
                                    <th className="p-2">Peso</th>
                                    <th className="p-2">Volumes</th>
                                </tr>
                            </thead>
                            <tbody>
                                {results.map((r, i) => {
                                    const transf = r.custo_transfer_total ?? r.custo_transferencia ?? 0;
                                    return (
                                        <tr key={i} className="border-t hover:bg-gray-50">
                                            <td className="p-2 font-medium">
                                                {SCENARIO_LABELS[r.cenario] ?? r.cenario}
                                            </td>
                                            <td className="p-2">{r.mes}</td>
                                            <td className="p-2 text-blue-600">{fmtMoeda(transf)}</td>
                                            <td className="p-2 text-orange-600">{fmtMoeda(r.custo_last_mile)}</td>
                                            <td className="p-2 font-bold text-emerald-600">{fmtMoeda(r.custo_total)}</td>
                                            <td className="p-2">{r.entregas ?? "-"}</td>
                                            <td className="p-2">{r.peso ?? "-"}</td>
                                            <td className="p-2">{r.volumes ?? "-"}</td>
                                        </tr>
                                    );
                                })}
                            </tbody>
                        </table>
                    </div>

                    {aggregatedByScenario.some((r) => r.mae || r.mse || r.r2) && (
                        <div className="bg-white rounded-xl shadow p-4 overflow-x-auto">
                            <h3 className="font-semibold mb-3">📐 Métricas dos Modelos (Média por Cenário)</h3>
                            <table className="min-w-full text-sm border border-gray-200 divide-y divide-gray-200">
                                <thead className="bg-gray-50">
                                    <tr>
                                        <th className="p-2 text-left">Cenário</th>
                                        {aggregatedByScenario.some((r) => r.mae !== undefined) && (
                                            <th className="p-2">MAE Médio</th>
                                        )}
                                        {aggregatedByScenario.some((r) => r.mse !== undefined) && (
                                            <th className="p-2">MSE Médio</th>
                                        )}
                                        {aggregatedByScenario.some((r) => r.r2 !== undefined) && (
                                            <th className="p-2">R² Médio</th>
                                        )}
                                    </tr>
                                </thead>
                                <tbody>
                                    {aggregatedByScenario.map((r, i) => (
                                        <tr key={i} className="border-t hover:bg-gray-50">
                                            <td className="p-2 font-medium">
                                                {SCENARIO_LABELS[r.cenario] ?? r.cenario}
                                            </td>
                                            {r.mae !== undefined && <td className="p-2">{r.mae.toFixed(2)}</td>}
                                            {r.mse !== undefined && <td className="p-2">{r.mse.toFixed(2)}</td>}
                                            {r.r2 !== undefined && <td className="p-2">{r.r2.toFixed(2)}</td>}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}

                    <div className="flex gap-3">
                        <button onClick={() => handleExport("csv")} className="btn">⬇️ Exportar CSV</button>
                        <button onClick={() => handleExport("json")} className="btn-secondary">⬇️ Exportar JSON</button>
                    </div>
                </>
            )}

            {!loading && !error && results.length === 0 && (
                <div className="p-3 bg-gray-50 text-gray-700 border border-gray-200 rounded-lg">
                    ⚠️ Nenhum resultado disponível. Gere um planejamento para visualizar.
                </div>
            )}
        </div>
    );
}
