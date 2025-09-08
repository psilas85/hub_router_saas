//frontend/src/pages/Planner/TrainCompareTab.tsx

// frontend/src/pages/Planner/TrainCompareTab.tsx
import { useState } from "react";
import { trainCompare } from "@/services/ml";
import type { TrainTarget } from "@/services/ml";

export default function TrainCompareTab() {
    const [target, setTarget] = useState<TrainTarget>("custo_total");
    const [start, setStart] = useState("2025-06-01");
    const [end, setEnd] = useState("2025-06-07");
    const [fast, setFast] = useState(true);
    const [loading, setLoading] = useState(false);
    const [rows, setRows] = useState<
        Array<{ algorithm: string; algorithm_label: string; metrics?: Record<string, number> }>
    >([]);
    const [msg, setMsg] = useState<string | null>(null);

    async function onRun() {
        setLoading(true);
        setMsg("🔄 Executando comparação de algoritmos...");
        try {
            const out = await trainCompare({
                dataset_name: "entregas",        // 👈 corrigido
                target_column: target,
                start_date: start,
                end_date: end,
                fast,
            });
            setRows(out?.results ?? []);
            setMsg("✅ Comparação concluída com sucesso!");
        } catch (e: any) {
            console.error(e);
            setMsg(e?.response?.data?.detail ?? "❌ Erro ao executar comparação.");
            setRows([]);
        } finally {
            setLoading(false);
        }
    }


    const metricKeys = Array.from(
        new Set(rows.flatMap((r) => Object.keys(r.metrics || {})))
    );

    return (
        <div className="space-y-6">
            {/* 🔹 Formulário */}
            <div className="bg-white rounded-xl shadow p-4 grid md:grid-cols-5 gap-4">
                <div>
                    <label className="block text-sm text-gray-600 mb-1">Target</label>
                    <select
                        className="input"
                        value={target}
                        onChange={(e) => setTarget(e.target.value as TrainTarget)}
                    >
                        <option value="custo_total">Custo Total (regressão)</option>
                        <option value="is_ponto_otimo">Ponto Ótimo (classificação)</option>
                    </select>
                </div>

                <div>
                    <label className="block text-sm text-gray-600 mb-1">Início</label>
                    <input
                        type="date"
                        className="input"
                        value={start}
                        onChange={(e) => setStart(e.target.value)}
                    />
                </div>

                <div>
                    <label className="block text-sm text-gray-600 mb-1">Fim</label>
                    <input
                        type="date"
                        className="input"
                        value={end}
                        onChange={(e) => setEnd(e.target.value)}
                    />
                </div>

                <div className="flex items-end">
                    <label className="inline-flex items-center gap-2 text-sm text-gray-700">
                        <input
                            type="checkbox"
                            checked={fast}
                            onChange={(e) => setFast(e.target.checked)}
                        />
                        Modo rápido
                    </label>
                </div>

                <div className="flex items-end">
                    <button className="btn w-full" onClick={onRun} disabled={loading}>
                        {loading ? "Treinando..." : "Comparar"}
                    </button>
                </div>
            </div>

            {/* 🔹 Mensagem */}
            {msg && (
                <div
                    className={`p-3 rounded-lg text-sm font-medium ${msg.startsWith("✅")
                        ? "bg-emerald-50 text-emerald-700 border border-emerald-200"
                        : msg.startsWith("❌")
                            ? "bg-red-50 text-red-700 border border-red-200"
                            : "bg-gray-50 text-gray-600 border border-gray-200"
                        }`}
                >
                    {msg}
                </div>
            )}

            {/* 🔹 Resultados */}
            {rows.length > 0 && metricKeys.length > 0 && (
                <div className="bg-white rounded-xl shadow p-4 overflow-x-auto">
                    <table className="min-w-full text-sm border border-gray-200 divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                            <tr>
                                <th className="p-2 text-left">Algoritmo</th>
                                {metricKeys.map((k) => (
                                    <th key={k} className="p-2 text-right">
                                        {k}
                                    </th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {rows.map((r, i) => (
                                <tr key={i} className="border-t hover:bg-gray-50">
                                    <td className="p-2 font-medium">
                                        {r.algorithm_label ?? r.algorithm}
                                    </td>
                                    {metricKeys.map((k) => (
                                        <td key={k} className="p-2 text-right">
                                            {r.metrics?.[k]?.toFixed?.(4) ?? "-"}
                                        </td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}
