//frontend/src/pages/Planner/TrainCompareTab.tsx

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

    async function onRun() {
        setLoading(true);
        try {
            const out = await trainCompare({
                dataset_name: "simulacoes",
                target_column: target,
                start_date: start,
                end_date: end,
                fast,
            });
            setRows(out?.results ?? []);
        } catch (e: any) {
            alert(e?.response?.data?.detail ?? e.message);
        } finally {
            setLoading(false);
        }
    }

    const metricKeys = Array.from(
        new Set(rows.flatMap(r => Object.keys(r.metrics || {})))
    );

    return (
        <div className="space-y-4">
            <div className="grid md:grid-cols-5 gap-3">
                <div>
                    <label className="block text-sm mb-1">Target</label>
                    <select
                        className="input"
                        value={target}
                        onChange={e => setTarget(e.target.value as TrainTarget)}
                    >
                        <option value="custo_total">Custo Total (regressão)</option>
                        <option value="is_ponto_otimo">Ponto Ótimo (classificação)</option>
                    </select>
                </div>
                <div>
                    <label className="block text-sm mb-1">Início</label>
                    <input
                        type="date"
                        className="input"
                        value={start}
                        onChange={e => setStart(e.target.value)}
                    />
                </div>
                <div>
                    <label className="block text-sm mb-1">Fim</label>
                    <input
                        type="date"
                        className="input"
                        value={end}
                        onChange={e => setEnd(e.target.value)}
                    />
                </div>
                <div className="flex items-end">
                    <label className="inline-flex items-center gap-2">
                        <input
                            type="checkbox"
                            checked={fast}
                            onChange={e => setFast(e.target.checked)}
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

            {rows.length > 0 && metricKeys.length > 0 && (
                <div className="overflow-auto">
                    <table className="w-full text-sm">
                        <thead>
                            <tr className="text-left border-b">
                                <th className="py-2 pr-3">Algoritmo</th>
                                {metricKeys.map(k => (
                                    <th key={k} className="py-2 pr-3">{k}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {rows.map((r, i) => (
                                <tr key={i} className="border-b">
                                    <td className="py-2 pr-3">
                                        {r.algorithm_label ?? r.algorithm}
                                    </td>
                                    {metricKeys.map(k => (
                                        <td key={k} className="py-2 pr-3">
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
