import { useEffect, useState } from "react";
import {
    listClusterCosts,
    saveClusterCosts,
    deleteClusterCost,
    type ClusterCost,
} from "@/services/simulationApi";
import { Plus, Trash2 } from "lucide-react";
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
        <div className="p-6 max-w-4xl mx-auto">
            <h1 className="text-3xl font-bold text-slate-800 mb-2">
                💰 Custos de Centros
            </h1>
            <p className="text-slate-500 mb-8">
                Cadastre e gerencie os custos fixos e variáveis dos centros de cluster.
            </p>

            {/* Formulário novo */}
            <div className="bg-white rounded-xl shadow-md p-4 mb-6 grid grid-cols-4 gap-4">
                <input
                    className="input"
                    placeholder="Limite de entregas"
                    type="number"
                    value={novo.limite_qtd_entregas}
                    onChange={(e) =>
                        setNovo({ ...novo, limite_qtd_entregas: +e.target.value })
                    }
                />
                <input
                    className="input"
                    placeholder="Custo fixo diário"
                    type="number"
                    value={novo.custo_fixo_diario}
                    onChange={(e) =>
                        setNovo({ ...novo, custo_fixo_diario: +e.target.value })
                    }
                />
                <input
                    className="input"
                    placeholder="Custo variável por entrega"
                    type="number"
                    value={novo.custo_variavel_por_entrega}
                    onChange={(e) =>
                        setNovo({ ...novo, custo_variavel_por_entrega: +e.target.value })
                    }
                />
                <Button onClick={salvarNovo} className="flex items-center gap-2">
                    <Plus className="w-4 h-4" /> Adicionar
                </Button>
            </div>

            {/* Tabela */}
            <table className="min-w-full text-sm bg-white rounded-lg shadow">
                <thead>
                    <tr className="text-left border-b">
                        <th className="p-2">Limite Entregas</th>
                        <th className="p-2">Custo Fixo Diário</th>
                        <th className="p-2">Custo Variável/Entrega</th>
                        <th className="p-2 text-right">Ações</th>
                    </tr>
                </thead>
                <tbody>
                    {costs.map((c) => (
                        <tr key={c.id ?? c.limite_qtd_entregas} className="border-b">
                            {editando === c.id ? (
                                <>
                                    <td className="p-2">
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
                                    <td className="p-2">
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
                                    <td className="p-2">
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
                                    <td className="p-2">{c.limite_qtd_entregas}</td>
                                    <td className="p-2">R$ {c.custo_fixo_diario.toFixed(2)}</td>
                                    <td className="p-2">
                                        R$ {c.custo_variavel_por_entrega.toFixed(2)}
                                    </td>
                                </>
                            )}

                            <td className="p-2 text-right flex gap-2">
                                {editando === c.id ? (
                                    <>
                                        <Button onClick={salvarEdicao} variant="default">
                                            Salvar
                                        </Button>
                                        <Button
                                            onClick={() => {
                                                setEditando(null);
                                                setEditCost(null);
                                            }}
                                            variant="secondary"
                                        >
                                            Cancelar
                                        </Button>
                                    </>
                                ) : (
                                    <>
                                        <Button
                                            onClick={() => {
                                                setEditando(c.id!);
                                                setEditCost(c);
                                            }}
                                            variant="outline"
                                        >
                                            Editar
                                        </Button>
                                        <Button
                                            onClick={() => remover(c.id)}
                                            variant="destructive"
                                            className="flex items-center gap-2"
                                        >
                                            <Trash2 className="w-4 h-4" /> Remover
                                        </Button>
                                    </>
                                )}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}
