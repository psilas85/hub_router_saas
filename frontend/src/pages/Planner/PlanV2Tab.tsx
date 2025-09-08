//PlanV2Tab.tsx

// frontend/src/pages/Planner/PlanV2Tab.tsx
import { useState } from "react";
import { planV2 } from "@/services/ml";
import {
    ResponsiveContainer,
    LineChart,
    Line,
    XAxis,
    YAxis,
    Tooltip,
    Legend,
} from "recharts";
import { fmtMoeda, fmtCompact } from "@/utils/format";

export default function PlanV2Tab() {
    const [start, setStart] = useState("2025-07-01");
    const [months, setMonths] = useState(3);
    const [data, setData] = useState<Record<string, any[]>>({});
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);

    async function handlePlan() {
        setLoading(true);
        setMsg("🔄 Gerando planejamento ML, por favor aguarde...");
        try {
            const res = await planV2({ start_date: start, months, fast: true });
            setData(res || {});
            if (Object.keys(res || {}).length > 0) {
                setMsg("✅ Planejamento ML concluído com sucesso!");
            } else {
                setMsg("⚠️ Nenhum resultado retornado pelo backend.");
            }
        } catch (err: any) {
            console.error("❌ Erro no plan_v2:", err);
            alert(err?.response?.data?.detail ?? err.message);
            setMsg("❌ Ocorreu um erro ao gerar o planejamento ML.");
            setData({});
        } finally {
            setLoading(false);
        }
    }

    // 👇 Pega o primeiro registro do cenário base (quando existir) para os KPIs
    const base0 = (data?.base && Array.isArray(data.base) && data.base[0]) || null;

    return (
        <div className="space-y-6">
            <h2 className="text-xl font-bold">📅 Planejamento com ML (V2)</h2>

            {/* Formulário */}
            <div className="flex gap-2 items-end">
                <div>
                    <label className="text-sm text-gray-600">Data inicial</label>
                    <input
                        type="date"
                        value={start}
                        onChange={(e) => setStart(e.target.value)}
                        className="input"
                    />
                </div>
                <div>
                    <label className="text-sm text-gray-600">Período (meses)</label>
                    <input
                        type="number"
                        value={months}
                        onChange={(e) => setMonths(Number(e.target.value))}
                        className="input"
                    />
                </div>
                <button onClick={handlePlan} className="btn" disabled={loading}>
                    {loading ? "Gerando..." : "Gerar Planejamento ML"}
                </button>
            </div>

            {/* Mensagem */}
            {msg && (
                <div
                    className={`text-sm font-medium rounded-lg p-3 ${msg.startsWith("✅")
                        ? "text-emerald-700 bg-emerald-50 border border-emerald-200"
                        : msg.startsWith("❌")
                            ? "text-red-700 bg-red-50 border border-red-200"
                            : msg.startsWith("⚠️")
                                ? "text-yellow-700 bg-yellow-50 border border-yellow-200"
                                : "text-gray-700 bg-gray-50 border border-gray-200"
                        }`}
                >
                    {msg}
                </div>
            )}

            {/* KPIs do cenário Base (primeiro item) */}
            {base0 && (
                <>
                    <h3 className="font-semibold">Indicadores (Cenário: Base • {base0.mes})</h3>
                    <div className="grid md:grid-cols-3 gap-4">
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Custo Transferência</p>
                            <p className="text-lg font-semibold text-blue-600">
                                {base0.custo_transfer_total != null
                                    ? fmtMoeda(base0.custo_transfer_total)
                                    : "-"}
                            </p>
                        </div>
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Custo Last Mile</p>
                            <p className="text-lg font-semibold text-orange-600">
                                {base0.custo_last_mile != null ? fmtMoeda(base0.custo_last_mile) : "-"}
                            </p>
                        </div>
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Custo Total</p>
                            <p className="text-lg font-semibold text-emerald-600">
                                {base0.custo_total != null ? fmtMoeda(base0.custo_total) : "-"}
                            </p>
                        </div>
                    </div>

                    <div className="grid md:grid-cols-3 gap-4">
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Entregas</p>
                            <p className="text-lg font-semibold text-indigo-600">
                                {base0.entregas != null ? fmtCompact(Number(base0.entregas)) : "-"}
                            </p>
                        </div>
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Peso</p>
                            <p className="text-lg font-semibold text-purple-600">
                                {base0.peso != null ? fmtCompact(Number(base0.peso)) : "-"}
                            </p>
                        </div>
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Volumes</p>
                            <p className="text-lg font-semibold text-pink-600">
                                {base0.volumes != null ? fmtCompact(Number(base0.volumes)) : "-"}
                            </p>
                        </div>
                    </div>
                </>
            )}

            {/* Resultados (gráfico) */}
            {Object.keys(data).length > 0 && (
                <ResponsiveContainer width="100%" height={350}>
                    <LineChart>
                        <XAxis dataKey="mes" />
                        <YAxis tickFormatter={(v) => fmtCompact(Number(v))} />
                        <Tooltip formatter={(v, n) => [fmtMoeda(Number(v)), n as string]} />
                        <Legend />
                        {Object.entries(data).map(([cenario, arr], i) => {
                            const list = Array.isArray(arr)
                                ? arr
                                : arr && typeof arr === "object"
                                    ? [arr]
                                    : [];
                            if (list.length === 0) return null;
                            return (
                                <>
                                    <Line
                                        key={`${cenario}-total`}
                                        data={list}
                                        dataKey="custo_total"
                                        name={`${cenario} Total`}
                                        stroke={["#10b981", "#f59e0b", "#3b82f6", "#ef4444"][i % 4]}
                                    />
                                    <Line
                                        key={`${cenario}-lm`}
                                        data={list}
                                        dataKey="custo_last_mile"
                                        name={`${cenario} LastMile`}
                                        stroke="#f59e0b"
                                        strokeDasharray="4 4"
                                    />
                                    <Line
                                        key={`${cenario}-tr`}
                                        data={list}
                                        dataKey="custo_transfer_total"
                                        name={`${cenario} Transfer`}
                                        stroke="#3b82f6"
                                        strokeDasharray="4 4"
                                    />
                                </>
                            );
                        })}
                    </LineChart>
                </ResponsiveContainer>
            )}
        </div>
    );
}
