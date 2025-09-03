// frontend/src/pages/Planner/WhatIfTab.tsx
import { useState } from "react";
import { predict } from "@/services/ml";
import type { TrainTarget } from "@/services/ml";
import { fmtMoeda } from "@/utils/format";

type PredictOut = {
    prediction: number | null;
    probability: number | null;
    algorithm: string | null;
};

export default function WhatIfTab() {
    const [target, setTarget] = useState<TrainTarget>("custo_total");
    const [loading, setLoading] = useState(false);
    const [out, setOut] = useState<PredictOut | null>(null);

    const [feat, setFeat] = useState({
        k_clusters: 2,
        total_entregas: 150,
        custo_transfer_total: 10000,
        custo_last_mile: 8000,
        total_peso: 12000,
        total_volumes: 500,
        valor_total_nf: 250000,
    });

    function update(name: string, value: number) {
        if (value < 0) value = 0; // 🔒 não aceita negativos
        setFeat((prev) => ({ ...prev, [name]: value }));
    }

    async function onRun() {
        setLoading(true);
        try {
            const res = await predict({
                dataset_name: "simulacoes",
                target_column: target,
                features: feat as any,
            });

            setOut({
                prediction: res?.prediction ?? null,
                probability: res?.probability ?? null,
                algorithm: res?.algorithm ?? null,
            });
        } catch (e: any) {
            console.error("❌ Erro no What-if:", e);
            alert(e?.response?.data?.detail ?? e.message);
            setOut(null);
        } finally {
            setLoading(false);
        }
    }

    // 🔑 cálculo automático do custo total
    const custoTotal = feat.custo_transfer_total + feat.custo_last_mile;

    return (
        <div className="space-y-4">
            <h2 className="text-xl font-bold">🔮 Simulação What-if</h2>

            <div className="grid md:grid-cols-4 gap-3">
                <div>
                    <label className="block text-sm mb-1">Target</label>
                    <select
                        className="input"
                        value={target}
                        onChange={(e) => setTarget(e.target.value as TrainTarget)}
                    >
                        <option value="custo_total">Custo Total</option>
                        <option value="is_ponto_otimo">Ponto Ótimo</option>
                    </select>
                </div>

                <div>
                    <label className="block text-sm mb-1">Quantidade de Clusters</label>
                    <input
                        className="input"
                        type="number"
                        value={feat.k_clusters}
                        onChange={(e) => update("k_clusters", +e.target.value)}
                    />
                </div>

                <div>
                    <label className="block text-sm mb-1">Total de Entregas</label>
                    <input
                        className="input"
                        type="number"
                        value={feat.total_entregas}
                        onChange={(e) => update("total_entregas", +e.target.value)}
                    />
                </div>

                <div>
                    <label className="block text-sm mb-1">Custo de Transferência (R$)</label>
                    <input
                        className="input"
                        type="number"
                        value={feat.custo_transfer_total}
                        onChange={(e) => update("custo_transfer_total", +e.target.value)}
                    />
                </div>

                <div>
                    <label className="block text-sm mb-1">Custo Last Mile (R$)</label>
                    <input
                        className="input"
                        type="number"
                        value={feat.custo_last_mile}
                        onChange={(e) => update("custo_last_mile", +e.target.value)}
                    />
                </div>

                <div>
                    <label className="block text-sm mb-1">Peso Total (kg)</label>
                    <input
                        className="input"
                        type="number"
                        value={feat.total_peso}
                        onChange={(e) => update("total_peso", +e.target.value)}
                    />
                </div>

                <div>
                    <label className="block text-sm mb-1">Total de Volumes</label>
                    <input
                        className="input"
                        type="number"
                        value={feat.total_volumes}
                        onChange={(e) => update("total_volumes", +e.target.value)}
                    />
                </div>

                <div>
                    <label className="block text-sm mb-1">Valor Total NF (R$)</label>
                    <input
                        className="input"
                        type="number"
                        value={feat.valor_total_nf}
                        onChange={(e) => update("valor_total_nf", +e.target.value)}
                    />
                </div>
            </div>

            <div className="p-3 bg-gray-100 rounded text-sm">
                <b>Custo Total (auto):</b>{" "}
                <span className="text-emerald-700">{fmtMoeda.format(custoTotal)}</span>
            </div>

            <button className="btn" onClick={onRun} disabled={loading}>
                {loading ? "Calculando..." : "Rodar Simulação What-if"}
            </button>

            {out && (
                <div className="mt-4 p-4 border rounded-xl bg-gray-50 space-y-2">
                    <div>
                        <b>Algoritmo Selecionado:</b> {out.algorithm ?? "-"}
                    </div>

                    {target === "custo_total" ? (
                        <div>
                            <b>Predição de Custo:</b>{" "}
                            {out.prediction != null
                                ? fmtMoeda.format(out.prediction)
                                : "-"}
                        </div>
                    ) : (
                        <div>
                            <b>Classificação Ponto Ótimo:</b>{" "}
                            {out.prediction === 1 ? (
                                <span className="text-emerald-700">🟢 Ótimo</span>
                            ) : (
                                <span className="text-red-600">🔴 Não Ótimo</span>
                            )}
                        </div>
                    )}

                    {out.probability !== null && (
                        <div>
                            <b>Probabilidade:</b>{" "}
                            {(out.probability * 100).toFixed(1)}%
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
