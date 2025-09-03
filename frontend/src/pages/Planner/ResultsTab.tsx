// frontend/src/pages/Planner/ResultsTab.tsx
import { useEffect, useState } from "react";
import { plan } from "@/services/ml";
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
    custo_transferencia: number;
    custo_last_mile: number;
    custo_total: number;
    frota: Record<string, number>;
    hubs: [string, string][];
};

// 🔎 mapa para exibir rótulos amigáveis
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
                // timeout defensivo
                timeoutId = window.setTimeout(() => controller.abort(), 20000);

                const raw = await plan({
                    start_date: "2025-06-01",
                    months: 3,
                    scenarios: "base,baixo,alto",
                    fast: true,
                });

                const content =
                    (raw as any)?.content ??
                    (raw as any)?.data?.content ??
                    (raw as any)?.data ??
                    raw;

                if (!content || typeof content !== "object") {
                    throw new Error("Resposta inesperada do endpoint /ml/plan.");
                }

                const parsed: ScenarioResult[] = [];
                Object.entries(content as Record<string, any[]>).forEach(
                    ([cenario, arr]) => {
                        (arr ?? []).forEach((item: any) => {
                            parsed.push({
                                cenario,
                                mes: item.mes,
                                custo_transferencia: Number(item.custo_transferencia ?? 0),
                                custo_last_mile: Number(item.custo_last_mile ?? 0),
                                custo_total: Number(item.custo_total ?? 0),
                                frota: item.frota ?? {},
                                hubs: item.hubs ?? [],
                            });
                        });
                    }
                );

                setResults(parsed);
            } catch (err: any) {
                if (err?.name === "AbortError") {
                    setError("⏱️ Tempo de resposta excedido. Tente novamente.");
                } else {
                    console.error("❌ Erro ao carregar resultados:", err);
                    setError(
                        err?.response?.data?.detail ??
                        err.message ??
                        "❌ Ocorreu um erro ao carregar os resultados."
                    );
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
            const blob = new Blob([JSON.stringify(results, null, 2)], {
                type: "application/json",
            });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = "resultados.json";
            a.click();
            return;
        }
        const header =
            "Cenário,Mês,Custo Transferência,Custo Last Mile,Custo Total,Frota,Hubs\n";
        const rows = results.map(
            (r) =>
                `${SCENARIO_LABELS[r.cenario] ?? r.cenario},${r.mes},${r.custo_transferencia
                },${r.custo_last_mile},${r.custo_total},"${JSON.stringify(
                    r.frota
                )}","${r.hubs.map((h) => h.join(" ")).join("; ")}"`
        );
        const blob = new Blob([header + rows.join("\n")], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "resultados.csv";
        a.click();
    };

    return (
        <div className="space-y-6">
            <h2 className="text-xl font-bold">📊 Resultados Consolidados</h2>

            {loading && (
                <div className="p-3 bg-yellow-100 text-yellow-800 rounded-md">
                    ⏳ Carregando resultados consolidados...
                </div>
            )}

            {!loading && error && (
                <div className="p-3 bg-red-100 text-red-800 rounded-md">{error}</div>
            )}

            {!loading && !error && results.length > 0 && (
                <>
                    <div className="p-3 bg-emerald-100 text-emerald-800 rounded-md">
                        ✅ Resultados carregados com sucesso!
                    </div>

                    <ResponsiveContainer width="100%" height={300}>
                        <BarChart
                            data={results.map((r) => ({
                                ...r,
                                cenario_label: SCENARIO_LABELS[r.cenario] ?? r.cenario,
                            }))}
                        >
                            <XAxis dataKey="cenario_label" />
                            <YAxis tickFormatter={(v) => fmtCompact.format(Number(v))} />
                            <Tooltip
                                formatter={(v: any, n) => [fmtMoeda.format(Number(v)), n]}
                            />
                            <Legend />
                            <Bar
                                dataKey="custo_transferencia"
                                fill="#3b82f6"
                                name="Transferência"
                            />
                            <Bar dataKey="custo_last_mile" fill="#f59e0b" name="Last Mile" />
                            <Bar dataKey="custo_total" fill="#10b981" name="Total" />
                        </BarChart>
                    </ResponsiveContainer>

                    <div className="overflow-auto">
                        <table className="w-full text-sm border">
                            <thead className="bg-gray-100">
                                <tr>
                                    <th className="p-2 text-left">Cenário</th>
                                    <th className="p-2">Mês</th>
                                    <th className="p-2">Custo Transferência</th>
                                    <th className="p-2">Custo Last Mile</th>
                                    <th className="p-2">Custo Total</th>
                                    <th className="p-2">Frota</th>
                                    <th className="p-2">Hubs</th>
                                </tr>
                            </thead>
                            <tbody>
                                {results.map((r, i) => (
                                    <tr key={i} className="border-t">
                                        <td className="p-2 font-medium">
                                            {SCENARIO_LABELS[r.cenario] ?? r.cenario}
                                        </td>
                                        <td className="p-2">{r.mes}</td>
                                        <td className="p-2 text-blue-600">
                                            {fmtMoeda.format(r.custo_transferencia)}
                                        </td>
                                        <td className="p-2 text-orange-600">
                                            {fmtMoeda.format(r.custo_last_mile)}
                                        </td>
                                        <td className="p-2 font-bold text-emerald-600">
                                            {fmtMoeda.format(r.custo_total)}
                                        </td>
                                        <td className="p-2">
                                            {Object.entries(r.frota)
                                                .map(([k, v]) => `${k}: ${v}`)
                                                .join(", ")}
                                        </td>
                                        <td className="p-2">
                                            {r.hubs.map((h) => h.join(" ")).join("; ")}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>

                    <div className="flex gap-3">
                        <button
                            onClick={() => handleExport("csv")}
                            className="px-4 py-2 bg-blue-600 text-white rounded"
                        >
                            ⬇️ Exportar como CSV
                        </button>
                        <button
                            onClick={() => handleExport("json")}
                            className="px-4 py-2 bg-gray-700 text-white rounded"
                        >
                            ⬇️ Exportar como JSON
                        </button>
                    </div>
                </>
            )}

            {!loading && !error && results.length === 0 && (
                <div className="p-3 bg-gray-100 text-gray-700 rounded-md">
                    ⚠️ Nenhum resultado disponível. Gere um planejamento para visualizar
                    resultados.
                </div>
            )}
        </div>
    );
}
