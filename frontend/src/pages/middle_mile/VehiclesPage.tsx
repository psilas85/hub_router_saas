// src/pages/Middle_mile/VehiclesPage.tsx
import { useEffect, useMemo, useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { Loader2, PlusCircle, Pencil, Trash2, Save, XCircle } from "lucide-react";

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

    const [editando, setEditando] = useState<string | null>(null);
    const [editData, setEditData] = useState<Tarifa | null>(null);

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
            await api.post("/costs_transfer/tarifas", novo);
            setNovo({ tipo_veiculo: "", custo_km: 0, capacidade_min: 0, capacidade_max: 0 });
            toast.success("Veículo adicionado com sucesso!");
            await refresh();
        } catch (e) {
            console.error("Erro ao criar tarifa:", e);
            toast.error("Erro ao criar tarifa.");
        } finally {
            setLoading(false);
        }
    }

    function editar(item: Tarifa) {
        setEditando(item.tipo_veiculo);
        setEditData({ ...item });
    }

    function cancelarEdicao() {
        setEditando(null);
        setEditData(null);
    }

    async function salvarEdicao() {
        if (!editData) return;
        try {
            setLoading(true);
            await api.put(
                `/costs_transfer/tarifas/${encodeURIComponent(editando!)}`,
                editData
            );
            toast.success("Tarifa atualizada!");
            cancelarEdicao();
            await refresh();
        } catch (e) {
            console.error("Erro ao editar:", e);
            toast.error("Erro ao editar tarifa.");
        } finally {
            setLoading(false);
        }
    }

    async function remover(tipo_veiculo: string) {
        if (!window.confirm(`Remover tarifa de '${tipo_veiculo}'?`)) return;
        try {
            setLoading(true);
            await api.delete(`/costs_transfer/tarifas/${encodeURIComponent(tipo_veiculo)}`);
            toast.success("Tarifa removida com sucesso!");
            await refresh();
        } catch (e) {
            console.error("Erro ao remover:", e);
            toast.error("Erro ao remover tarifa.");
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
                    <div className="col-span-2">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Tipo de veículo
                        </label>
                        <input
                            value={novo.tipo_veiculo}
                            onChange={(e) => setNovo({ ...novo, tipo_veiculo: e.target.value })}
                            className="input"
                            placeholder="Ex.: toco, bitrem, vuc..."
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            R$/km
                        </label>
                        <input
                            type="number"
                            step="0.01"
                            value={novo.custo_km}
                            onChange={(e) => setNovo({ ...novo, custo_km: Number(e.target.value) })}
                            className="input text-right"
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Cap. min (kg)
                        </label>
                        <input
                            type="number"
                            value={novo.capacidade_min}
                            onChange={(e) => setNovo({ ...novo, capacidade_min: Number(e.target.value) })}
                            className="input text-right"
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Cap. max (kg)
                        </label>
                        <input
                            type="number"
                            value={novo.capacidade_max}
                            onChange={(e) => setNovo({ ...novo, capacidade_max: Number(e.target.value) })}
                            className="input text-right"
                        />
                    </div>
                </div>

                <div className="mt-3 flex justify-end">
                    <button
                        onClick={adicionar}
                        disabled={loading}
                        className="btn bg-emerald-600 text-white"
                    >
                        {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <PlusCircle className="w-4 h-4" />}
                        Adicionar
                    </button>
                </div>
            </div>

            {/* Filtro */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-4">
                <input
                    value={filtro}
                    onChange={(e) => setFiltro(e.target.value)}
                    placeholder="Buscar por tipo de veículo..."
                    className="input"
                />
            </div>

            {/* Tabela */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                <table className="min-w-full">
                    <thead className="bg-gray-50">
                        <tr>
                            <th className="px-4 py-3 text-left">Tipo</th>
                            <th className="px-4 py-3 text-right">R$/km</th>
                            <th className="px-4 py-3 text-right">Cap. min (kg)</th>
                            <th className="px-4 py-3 text-right">Cap. max (kg)</th>
                            <th className="px-4 py-3 text-center">Ações</th>
                        </tr>
                    </thead>
                    <tbody>
                        {filtradas.map((t) => (
                            <tr key={t.tipo_veiculo} className="border-t">
                                {editando === t.tipo_veiculo ? (
                                    <>
                                        <td className="px-4 py-2">
                                            <input
                                                className="input"
                                                value={editData?.tipo_veiculo || ""}
                                                onChange={(e) =>
                                                    setEditData({ ...editData!, tipo_veiculo: e.target.value })
                                                }
                                            />
                                        </td>
                                        <td className="px-4 py-2 text-right">
                                            <input
                                                type="number"
                                                className="input text-right"
                                                value={editData?.custo_km || 0}
                                                onChange={(e) =>
                                                    setEditData({ ...editData!, custo_km: Number(e.target.value) })
                                                }
                                            />
                                        </td>
                                        <td className="px-4 py-2 text-right">
                                            <input
                                                type="number"
                                                className="input text-right"
                                                value={editData?.capacidade_min || 0}
                                                onChange={(e) =>
                                                    setEditData({ ...editData!, capacidade_min: Number(e.target.value) })
                                                }
                                            />
                                        </td>
                                        <td className="px-4 py-2 text-right">
                                            <input
                                                type="number"
                                                className="input text-right"
                                                value={editData?.capacidade_max || 0}
                                                onChange={(e) =>
                                                    setEditData({ ...editData!, capacidade_max: Number(e.target.value) })
                                                }
                                            />
                                        </td>
                                        <td className="px-4 py-2 text-center flex gap-2 justify-center">
                                            <button className="btn" onClick={salvarEdicao}>
                                                <Save className="w-4 h-4" /> Salvar
                                            </button>
                                            <button className="btn-secondary" onClick={cancelarEdicao}>
                                                <XCircle className="w-4 h-4" /> Cancelar
                                            </button>
                                        </td>
                                    </>
                                ) : (
                                    <>
                                        <td className="px-4 py-2">{t.tipo_veiculo}</td>
                                        <td className="px-4 py-2 text-right">{t.custo_km.toFixed(2)}</td>
                                        <td className="px-4 py-2 text-right">{t.capacidade_min}</td>
                                        <td className="px-4 py-2 text-right">{t.capacidade_max}</td>
                                        <td className="px-4 py-2 text-center flex gap-2 justify-center">
                                            <button className="btn-secondary" onClick={() => editar(t)}>
                                                <Pencil className="w-4 h-4" /> Editar
                                            </button>
                                            <button className="btn-danger" onClick={() => remover(t.tipo_veiculo)}>
                                                <Trash2 className="w-4 h-4" /> Excluir
                                            </button>
                                        </td>
                                    </>
                                )}
                            </tr>
                        ))}
                        {filtradas.length === 0 && (
                            <tr>
                                <td colSpan={5} className="px-4 py-6 text-center text-gray-500">
                                    Nenhuma tarifa.
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
