//SimulationClusterCostPage.tsx

import { useEffect, useState } from "react";
import {
    listClusterCosts,
    saveClusterCosts,
    deleteClusterCost,
    type ClusterCost,
} from "@/services/simulationApi";
import { Plus, Trash2, Pencil } from "lucide-react";
import toast from "react-hot-toast";
import { Button } from "@/components/ui/button";

export default function SimulationClusterCostsPage() {
    const [costs, setCosts] = useState<ClusterCost[]>([]);
    const [novo, setNovo] = useState<ClusterCost>({
        limite_qtd_entregas: 0,
        custo_fixo_diario: 0,
        custo_variavel_por_entrega: 0,
    });

    const [editando, setEditando] = useState<number | null>(null);
    const [editCost, setEditCost] = useState<ClusterCost | null>(null);

    const carregar = async () => {
        try {
            const data = await listClusterCosts();
            setCosts(data);
        } catch {
            toast.error("Erro ao carregar custos");
        }
    };

    useEffect(() => {
        carregar();
    }, []);

    const salvarNovo = async () => {
        try {
            await saveClusterCosts(novo);
            toast.success("Custos cadastrados!");
            setNovo({
                limite_qtd_entregas: 0,
                custo_fixo_diario: 0,
                custo_variavel_por_entrega: 0,
            });
            carregar();
        } catch {
            toast.error("Erro ao salvar custos");
        }
    };

    const salvarEdicao = async () => {
        if (!editCost) return;
        try {
            await saveClusterCosts(editCost);
            toast.success("Custos atualizados!");
            setEditando(null);
            setEditCost(null);
            carregar();
        } catch {
            toast.error("Erro ao atualizar custos");
        }
    };

    const remover = async (id?: number) => {
        if (!id) return;
        try {
            await deleteClusterCost(id);
            toast.success("Custos removidos!");
            carregar();
        } catch {
            toast.error("Erro ao remover custos");
        }
    };

    return (
        <div className="p-6 max-w-5xl mx-auto">
            {/* Header */}
            <header className="mb-6">
                <h1 className="text-2xl md:text-3xl font-semibold text-slate-800">
                    💰 Custos de Centros
                </h1>
                <p className="text-slate-500 mt-1">
                    Cadastre e gerencie os custos fixos e variáveis aplicados por centro de cluster.
                </p>
            </header>

            {/* Formulário novo */}
            <section className="bg-white rounded-2xl shadow p-5 mb-8">
                <div className="flex items-center gap-2 mb-4">
                    <Plus className="w-5 h-5 text-emerald-600" />
                    <h2 className="font-medium text-slate-800">Adicionar nova faixa</h2>
                </div>

                <div className="grid md:grid-cols-4 gap-4">
                    <div>
                        <label className="block text-sm text-slate-600 mb-1">
                            Limite de Entregas
                        </label>
                        <input
                            className="input"
                            type="number"
                            value={novo.limite_qtd_entregas}
                            onChange={(e) =>
                                setNovo({ ...novo, limite_qtd_entregas: +e.target.value })
                            }
                            placeholder="Ex.: 100"
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-slate-600 mb-1">
                            Custo Fixo Diário (R$)
                        </label>
                        <input
                            className="input"
                            type="number"
                            value={novo.custo_fixo_diario}
                            onChange={(e) =>
                                setNovo({ ...novo, custo_fixo_diario: +e.target.value })
                            }
                            placeholder="Ex.: 500"
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-slate-600 mb-1">
                            Custo Variável por Entrega (R$)
                        </label>
                        <input
                            className="input"
                            type="number"
                            value={novo.custo_variavel_por_entrega}
                            onChange={(e) =>
                                setNovo({
                                    ...novo,
                                    custo_variavel_por_entrega: +e.target.value,
                                })
                            }
                            placeholder="Ex.: 2.50"
                        />
                    </div>
                    <div className="flex items-end">
                        <Button
                            onClick={salvarNovo}
                            className="w-full flex items-center gap-2 bg-emerald-600 hover:bg-emerald-700 text-white"
                        >
                            <Plus className="w-4 h-4" /> Adicionar
                        </Button>
                    </div>
                </div>
            </section>

            {/* Lista/Tabela */}
            <section className="bg-white rounded-2xl shadow overflow-hidden">
                <table className="min-w-full text-sm">
                    <thead className="bg-slate-50 text-slate-600">
                        <tr>
                            <th className="p-3 text-left">Limite de Entregas</th>
                            <th className="p-3 text-left">Custo Fixo Diário</th>
                            <th className="p-3 text-left">Custo Variável / Entrega</th>
                            <th className="p-3 text-right">Ações</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y">
                        {costs.map((c) => {
                            const isEditing = editando === c.id;
                            return (
                                <tr
                                    key={c.id ?? c.limite_qtd_entregas}
                                    className="hover:bg-slate-50/60"
                                >
                                    {isEditing ? (
                                        <>
                                            <td className="p-3">
                                                <input
                                                    type="number"
                                                    className="input"
                                                    value={editCost?.limite_qtd_entregas ?? 0}
                                                    onChange={(e) =>
                                                        setEditCost({
                                                            ...editCost!,
                                                            limite_qtd_entregas: +e.target.value,
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-3">
                                                <input
                                                    type="number"
                                                    className="input"
                                                    value={editCost?.custo_fixo_diario ?? 0}
                                                    onChange={(e) =>
                                                        setEditCost({
                                                            ...editCost!,
                                                            custo_fixo_diario: +e.target.value,
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-3">
                                                <input
                                                    type="number"
                                                    className="input"
                                                    value={editCost?.custo_variavel_por_entrega ?? 0}
                                                    onChange={(e) =>
                                                        setEditCost({
                                                            ...editCost!,
                                                            custo_variavel_por_entrega: +e.target.value,
                                                        })
                                                    }
                                                />
                                            </td>
                                        </>
                                    ) : (
                                        <>
                                            <td className="p-3 font-medium text-slate-800">
                                                {c.limite_qtd_entregas}
                                            </td>
                                            <td className="p-3">R$ {c.custo_fixo_diario.toFixed(2)}</td>
                                            <td className="p-3">R$ {c.custo_variavel_por_entrega.toFixed(2)}</td>
                                        </>
                                    )}

                                    <td className="p-3">
                                        <div className="flex justify-end gap-2">
                                            {isEditing ? (
                                                <>
                                                    <Button
                                                        size="sm"
                                                        className="bg-emerald-600 hover:bg-emerald-700 text-white"
                                                        onClick={salvarEdicao}
                                                    >
                                                        Salvar
                                                    </Button>
                                                    <Button
                                                        size="sm"
                                                        className="bg-gray-300 hover:bg-gray-400 text-slate-800"
                                                        onClick={() => {
                                                            setEditando(null);
                                                            setEditCost(null);
                                                        }}
                                                    >
                                                        Cancelar
                                                    </Button>
                                                </>
                                            ) : (
                                                <>
                                                    <Button
                                                        size="sm"
                                                        className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white"
                                                        onClick={() => {
                                                            setEditando(c.id!);
                                                            setEditCost(c);
                                                        }}
                                                    >
                                                        <Pencil className="w-4 h-4" /> Editar
                                                    </Button>
                                                    <Button
                                                        size="sm"
                                                        className="flex items-center gap-2 bg-red-600 hover:bg-red-700 text-white"
                                                        onClick={() => remover(c.id)}
                                                    >
                                                        <Trash2 className="w-4 h-4" /> Remover
                                                    </Button>
                                                </>
                                            )}
                                        </div>
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </section>
        </div>
    );
}
