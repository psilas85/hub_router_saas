// frontend/src/pages/Planner/PlanTab.tsx
import { useState } from "react";
import { plan } from "@/services/ml";

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

const SCENARIO_OPTIONS = [
    { value: "base", label: "Base (Referência)" },
    { value: "baixo", label: "Otimista (Baixo)" },
    { value: "alto", label: "Pessimista (Alto)" },
];

export default function PlanTab() {
    const [start, setStart] = useState("2025-06-01");
    const [months, setMonths] = useState(3);
    const [scenarios, setScenarios] = useState<string[]>(["base", "baixo", "alto"]);
    const [data, setData] = useState<Record<string, any[]>>({});
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);

    const handlePlan = async () => {
        setLoading(true);
        setMsg("🔄 Gerando planejamento, por favor aguarde...");

        try {
            const res = await plan({
                start_date: start,
                months,
                scenarios: scenarios.join(","),
                fast: true,
            });

            setData(res || {});
            setMsg("✅ Planejamento concluído com sucesso!");
        } catch (err: any) {
            console.error("❌ Erro ao buscar plano:", err);
            setMsg("❌ Ocorreu um erro ao gerar o planejamento.");
            alert(err?.response?.data?.detail ?? err.message);
            setData({});
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="space-y-6">
            <h2 className="text-xl font-bold">📅 Planejamento de Cenários</h2>

            {/* Formulário */}
            <div className="bg-white rounded-xl shadow p-4 grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                    <label className="text-sm text-gray-600">Data inicial</label>
                    <input
                        type="date"
                        value={start}
                        onChange={(e) => setStart(e.target.value)}
                        className="input w-full"
                    />
                </div>
                <div>
                    <label className="text-sm text-gray-600">Período (meses)</label>
                    <input
                        type="number"
                        value={months}
                        onChange={(e) => setMonths(parseInt(e.target.value) || 0)}
                        className="input w-full"
                    />
                </div>
                <div className="md:col-span-2">
                    <label className="text-sm text-gray-600">Cenários</label>
                    <select
                        multiple
                        className="input w-full h-24"
                        value={scenarios}
                        onChange={(e) => {
                            const selected = Array.from(e.target.selectedOptions).map((o) => o.value);
                            setScenarios(selected);
                        }}
                    >
                        {SCENARIO_OPTIONS.map((opt) => (
                            <option key={opt.value} value={opt.value}>
                                {opt.label}
                            </option>
                        ))}
                    </select>
                    <p className="text-xs text-gray-500 mt-1">
                        Segure CTRL (ou CMD no Mac) para selecionar múltiplos.
                    </p>
                </div>
                <div className="md:col-span-4 flex justify-end">
                    <button
                        onClick={handlePlan}
                        className={`btn ${loading ? "opacity-60 cursor-not-allowed" : ""}`}
                        disabled={loading}
                    >
                        {loading ? "Processando..." : "Gerar Planejamento"}
                    </button>
                </div>
            </div>

            {/* Mensagem */}
            {msg && (
                <div
                    className={`text-sm font-medium rounded-lg p-3 ${msg.startsWith("✅")
                            ? "text-emerald-700 bg-emerald-50 border border-emerald-200"
                            : msg.startsWith("❌")
                                ? "text-red-700 bg-red-50 border border-red-200"
                                : "text-gray-700 bg-gray-50 border border-gray-200"
                        }`}
                >
                    {msg}
                </div>
            )}

            {/* Resultados */}
            {Object.keys(data).length > 0 && (
                <>
                    {/* KPIs */}
                    <div className="grid md:grid-cols-3 gap-4 mt-4">
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Custo Transferência</p>
                            <p className="text-lg font-semibold text-blue-600">
                                {data.base?.[0]?.custo_transferencia != null
                                    ? fmtMoeda(data.base[0].custo_transferencia)
                                    : "-"}
                            </p>
                        </div>
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Custo Last Mile</p>
                            <p className="text-lg font-semibold text-orange-600">
                                {data.base?.[0]?.custo_last_mile != null
                                    ? fmtMoeda(data.base[0].custo_last_mile)
                                    : "-"}
                            </p>
                        </div>
                        <div className="p-4 bg-white border rounded-lg shadow-sm">
                            <p className="text-sm text-gray-500">Custo Total</p>
                            <p className="text-lg font-semibold text-emerald-600">
                                {data.base?.[0]?.custo_total != null
                                    ? fmtMoeda(data.base[0].custo_total)
                                    : "-"}
                            </p>
                        </div>
                    </div>

                    {/* Gráfico */}
                    <ResponsiveContainer width="100%" height={350}>
                        <LineChart>
                            <XAxis dataKey="mes" />
                            <YAxis tickFormatter={(v) => fmtCompact(Number(v))} />
                            <Tooltip
                                formatter={(value: any, name: any) => {
                                    const v = Number(value);
                                    if (name === "custo_total") return [fmtMoeda(v), "Custo Total"];
                                    if (name === "custo_transferencia") return [fmtMoeda(v), "Transferência"];
                                    if (name === "custo_last_mile") return [fmtMoeda(v), "Last Mile"];
                                    return [value, name];
                                }}
                            />
                            <Legend />
                            {Object.keys(data).map((scenario, idx) => {
                                const dataset = data[scenario] || [];
                                if (!Array.isArray(dataset) || dataset.length === 0) return null;

                                return (
                                    <>
                                        <Line
                                            key={`${scenario}-total`}
                                            type="monotone"
                                            dataKey="custo_total"
                                            data={dataset}
                                            name={`${scenario} • Total`}
                                            stroke={["#10b981", "#3b82f6", "#f59e0b", "#ef4444"][idx % 4]}
                                            strokeWidth={2}
                                        />
                                        <Line
                                            key={`${scenario}-transfer`}
                                            type="monotone"
                                            dataKey="custo_transferencia"
                                            data={dataset}
                                            name={`${scenario} • Transfer`}
                                            stroke="#3b82f6"
                                            strokeDasharray="4 4"
                                        />
                                        <Line
                                            key={`${scenario}-lastmile`}
                                            type="monotone"
                                            dataKey="custo_last_mile"
                                            data={dataset}
                                            name={`${scenario} • Last Mile`}
                                            stroke="#f59e0b"
                                            strokeDasharray="4 4"
                                        />
                                    </>
                                );
                            })}
                        </LineChart>
                    </ResponsiveContainer>
                </>
            )}
        </div>
    );
}
