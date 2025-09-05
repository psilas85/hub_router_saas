// src/pages/Middle_mile/VehiclesPage.tsx
// src/pages/middle_mile/VehiclesPage.tsx
import { useEffect, useMemo, useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { Loader2, PlusCircle, Pencil, Trash2 } from "lucide-react";

type Tarifa = {
    tipo_veiculo: string;
    custo_km: number;
    capacidade_min: number;
    capacidade_max: number;
};

export default function VehiclesPage() {
    const [tarifas, setTarifas] = useState<Tarifa[]>([]);
    const [filtro, setFiltro] = useState("");
    const [loading, setLoading] = useState(false);
    const [novo, setNovo] = useState<Tarifa>({
        tipo_veiculo: "",
        custo_km: 0,
        capacidade_min: 0,
        capacidade_max: 0,
    });

    const filtradas = useMemo(() => {
        if (!filtro) return tarifas;
        return tarifas.filter((t) =>
            t.tipo_veiculo.toLowerCase().includes(filtro.toLowerCase())
        );
    }, [tarifas, filtro]);

    async function refresh() {
        try {
            setLoading(true);
            const { data } = await api.get<Tarifa[]>("/costs_transfer/tarifas");
            setTarifas(data);
        } catch (e) {
            console.error("Falha ao listar tarifas:", e);
            toast.error("Não foi possível carregar as tarifas.");
        } finally {
            setLoading(false);
        }
    }

    useEffect(() => {
        refresh();
    }, []);

    async function adicionar() {
        if (!novo.tipo_veiculo.trim()) {
            toast.error("Informe o tipo de veículo.");
            return;
        }
        try {
            setLoading(true);
            await api.post("/costs_transfer/tarifas", {
                ...novo,
                custo_km: Number(novo.custo_km),
                capacidade_min: Number(novo.capacidade_min),
                capacidade_max: Number(novo.capacidade_max),
            });
            setNovo({
                tipo_veiculo: "",
                custo_km: 0,
                capacidade_min: 0,
                capacidade_max: 0,
            });
            toast.success("Veículo adicionado com sucesso!");
            await refresh();
        } catch (e) {
            console.error("Erro ao criar tarifa:", e);
            toast.error("Erro ao criar tarifa.");
        } finally {
            setLoading(false);
        }
    }

    async function remover(tipo_veiculo: string) {
        if (!window.confirm(`Remover tarifa de '${tipo_veiculo}'?`)) return;
        try {
            setLoading(true);
            await api.delete(
                `/costs_transfer/tarifas/${encodeURIComponent(tipo_veiculo)}`
            );
            toast.success("Tarifa removida com sucesso!");
            await refresh();
        } catch (e) {
            console.error("Erro ao remover:", e);
            toast.error("Erro ao remover tarifa.");
        } finally {
            setLoading(false);
        }
    }

    // Edição inline simples com prompt
    async function editar(item: Tarifa) {
        const custo = Number(
            window.prompt("Novo R$/km:", String(item.custo_km)) ?? item.custo_km
        );
        const min = Number(
            window.prompt("Nova Cap. min (kg):", String(item.capacidade_min)) ??
            item.capacidade_min
        );
        const max = Number(
            window.prompt("Nova Cap. max (kg):", String(item.capacidade_max)) ??
            item.capacidade_max
        );
        try {
            setLoading(true);
            await api.put(
                `/costs_transfer/tarifas/${encodeURIComponent(item.tipo_veiculo)}`,
                {
                    tipo_veiculo: item.tipo_veiculo,
                    custo_km: custo,
                    capacidade_min: min,
                    capacidade_max: max,
                }
            );
            toast.success("Tarifa atualizada!");
            await refresh();
        } catch (e) {
            console.error("Erro ao editar:", e);
            toast.error("Erro ao editar tarifa.");
        } finally {
            setLoading(false);
        }
    }

    return (
        <div className="max-w-6xl mx-auto p-6">
            <h1 className="text-2xl font-semibold text-gray-800 mb-6">
                Middle-Mile • Cadastro de Veículos (Tarifas)
            </h1>

            {/* Card de cadastro */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-6">
                <div className="grid grid-cols-1 md:grid-cols-5 gap-3">
                    {/* Tipo de veículo */}
                    <div className="col-span-2">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Tipo de veículo (slug)
                        </label>
                        <input
                            value={novo.tipo_veiculo}
                            onChange={(e) =>
                                setNovo((s) => ({ ...s, tipo_veiculo: e.target.value }))
                            }
                            placeholder="Ex.: toco, bitrem, vuc..."
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
                        />
                    </div>

                    {/* R$/km */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            R$/km
                        </label>
                        <input
                            type="number"
                            inputMode="decimal"
                            step="0.01"
                            min={0}
                            value={novo.custo_km}
                            onChange={(e) =>
                                setNovo((s) => ({ ...s, custo_km: Number(e.target.value) }))
                            }
                            placeholder="0,00"
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
                        />
                    </div>

                    {/* Capacidade mínima */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Cap. min (kg)
                        </label>
                        <input
                            type="number"
                            inputMode="decimal"
                            step="0.01"
                            min={0}
                            value={novo.capacidade_min}
                            onChange={(e) =>
                                setNovo((s) => ({
                                    ...s,
                                    capacidade_min: Number(e.target.value),
                                }))
                            }
                            placeholder="0"
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
                        />
                    </div>

                    {/* Capacidade máxima */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Cap. max (kg)
                        </label>
                        <input
                            type="number"
                            inputMode="decimal"
                            step="0.01"
                            min={0}
                            value={novo.capacidade_max}
                            onChange={(e) =>
                                setNovo((s) => ({
                                    ...s,
                                    capacidade_max: Number(e.target.value),
                                }))
                            }
                            placeholder="0"
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
                        />
                    </div>
                </div>

                <div className="mt-3 flex justify-end">
                    <button
                        onClick={adicionar}
                        disabled={loading}
                        className="inline-flex items-center bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg shadow-sm gap-2 disabled:opacity-60"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Salvando...
                            </>
                        ) : (
                            <>
                                <PlusCircle className="w-4 h-4" /> Adicionar
                            </>
                        )}
                    </button>
                </div>
            </div>

            {/* Filtro */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-4">
                <div className="flex gap-2">
                    <div className="flex-1">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Buscar por tipo de veículo
                        </label>
                        <input
                            value={filtro}
                            onChange={(e) => setFiltro(e.target.value)}
                            placeholder="Ex.: vuc, toco, truck…"
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500"
                        />
                    </div>
                    <div className="pt-6">
                        <button
                            onClick={refresh}
                            className="h-10 bg-emerald-50 hover:bg-emerald-100 text-emerald-700 px-4 rounded-lg border border-emerald-200"
                        >
                            Atualizar
                        </button>
                    </div>
                </div>
            </div>

            {/* Tabela */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                <table className="min-w-full">
                    <thead className="bg-gray-50 text-gray-700">
                        <tr>
                            <th className="px-4 py-3 text-left text-sm font-medium">
                                Tipo de veículo
                            </th>
                            <th className="px-4 py-3 text-right text-sm font-medium">
                                R$/km
                            </th>
                            <th className="px-4 py-3 text-right text-sm font-medium">
                                Cap. min (kg)
                            </th>
                            <th className="px-4 py-3 text-right text-sm font-medium">
                                Cap. max (kg)
                            </th>
                            <th className="px-4 py-3 text-center text-sm font-medium">
                                Ações
                            </th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                        {filtradas.length === 0 && (
                            <tr>
                                <td
                                    colSpan={5}
                                    className="px-4 py-6 text-center text-gray-500"
                                >
                                    Nenhuma tarifa.
                                </td>
                            </tr>
                        )}
                        {filtradas.map((t) => (
                            <tr key={t.tipo_veiculo}>
                                <td className="px-4 py-3">{t.tipo_veiculo}</td>
                                <td className="px-4 py-3 text-right">
                                    {t.custo_km.toLocaleString("pt-BR")}
                                </td>
                                <td className="px-4 py-3 text-right">
                                    {t.capacidade_min.toLocaleString("pt-BR")}
                                </td>
                                <td className="px-4 py-3 text-right">
                                    {t.capacidade_max.toLocaleString("pt-BR")}
                                </td>
                                <td className="px-4 py-2">
                                    <div className="flex gap-2 justify-center">
                                        <button
                                            onClick={() => editar(t)}
                                            className="px-3 py-1 text-emerald-700 bg-emerald-50 hover:bg-emerald-100 rounded-lg border border-emerald-200 flex items-center gap-1"
                                        >
                                            <Pencil className="w-4 h-4" /> Editar
                                        </button>
                                        <button
                                            onClick={() => remover(t.tipo_veiculo)}
                                            className="px-3 py-1 text-rose-700 bg-rose-50 hover:bg-rose-100 rounded-lg border border-rose-200 flex items-center gap-1"
                                        >
                                            <Trash2 className="w-4 h-4" /> Excluir
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
