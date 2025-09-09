//SimulationHubsPage.tsx

import { useEffect, useState } from "react";
import {
    listHubs,
    createHub,
    updateHub,
    deleteHub,
    type Hub,
} from "@/services/simulationApi";
import { Plus, Trash2, Pencil } from "lucide-react";
import toast from "react-hot-toast";
import { Button } from "@/components/ui/button";

export default function SimulationHubsPage() {
    const [hubs, setHubs] = useState<Hub[]>([]);
    const [novo, setNovo] = useState<Hub>({
        nome: "",
        cidade: "",
        latitude: 0,
        longitude: 0,
    });

    const [editando, setEditando] = useState<number | null>(null);
    const [editHub, setEditHub] = useState<Hub | null>(null);

    const carregar = async () => {
        try {
            const data = await listHubs();
            setHubs(data);
        } catch {
            toast.error("Erro ao carregar hubs");
        }
    };

    useEffect(() => {
        carregar();
    }, []);

    const salvarNovo = async () => {
        try {
            await createHub(novo);
            toast.success("Hub cadastrado!");
            setNovo({ nome: "", cidade: "", latitude: 0, longitude: 0 });
            carregar();
        } catch {
            toast.error("Erro ao criar hub");
        }
    };

    const salvarEdicao = async () => {
        if (!editHub?.hub_id) return;
        try {
            await updateHub(editHub.hub_id, editHub);
            toast.success("Hub atualizado!");
            setEditando(null);
            setEditHub(null);
            carregar();
        } catch {
            toast.error("Erro ao atualizar hub");
        }
    };

    const remover = async (id?: number) => {
        if (!id) return;
        try {
            await deleteHub(id);
            toast.success("Hub removido!");
            carregar();
        } catch {
            toast.error("Erro ao remover hub");
        }
    };

    return (
        <div className="p-6 max-w-5xl mx-auto">
            {/* Header */}
            <header className="mb-6">
                <h1 className="text-2xl md:text-3xl font-semibold text-slate-800">
                    📍 Hubs de Distribuição
                </h1>
                <p className="text-slate-500 mt-1">
                    Cadastre e gerencie os hubs de distribuição usados nas simulações.
                </p>
            </header>

            {/* Formulário novo hub */}
            <section className="bg-white rounded-2xl shadow p-5 mb-8">
                <div className="flex items-center gap-2 mb-4">
                    <Plus className="w-5 h-5 text-emerald-600" />
                    <h2 className="font-medium text-slate-800">Adicionar novo hub</h2>
                </div>

                <div className="grid md:grid-cols-5 gap-4">
                    <div>
                        <label className="block text-sm text-slate-600 mb-1">Nome</label>
                        <input
                            className="input"
                            placeholder="Ex.: Hub Fortaleza"
                            value={novo.nome}
                            onChange={(e) => setNovo({ ...novo, nome: e.target.value })}
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-slate-600 mb-1">Cidade</label>
                        <input
                            className="input"
                            placeholder="Ex.: Fortaleza/CE"
                            value={novo.cidade}
                            onChange={(e) => setNovo({ ...novo, cidade: e.target.value })}
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-slate-600 mb-1">Latitude</label>
                        <input
                            type="number"
                            step="0.000001"
                            className="input"
                            placeholder="-3.7319"
                            value={novo.latitude}
                            onChange={(e) => setNovo({ ...novo, latitude: +e.target.value })}
                        />
                    </div>
                    <div>
                        <label className="block text-sm text-slate-600 mb-1">Longitude</label>
                        <input
                            type="number"
                            step="0.000001"
                            className="input"
                            placeholder="-38.5267"
                            value={novo.longitude}
                            onChange={(e) => setNovo({ ...novo, longitude: +e.target.value })}
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
                            <th className="p-3 text-left">Nome</th>
                            <th className="p-3 text-left">Cidade</th>
                            <th className="p-3 text-left">Latitude</th>
                            <th className="p-3 text-left">Longitude</th>
                            <th className="p-3 text-right">Ações</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y">
                        {hubs.map((h) => {
                            const isEditing = editando === h.hub_id;
                            return (
                                <tr
                                    key={h.hub_id ?? h.nome}
                                    className="hover:bg-slate-50/60"
                                >
                                    {isEditing ? (
                                        <>
                                            <td className="p-3">
                                                <input
                                                    className="input"
                                                    value={editHub?.nome || ""}
                                                    onChange={(e) =>
                                                        setEditHub({ ...editHub!, nome: e.target.value })
                                                    }
                                                />
                                            </td>
                                            <td className="p-3">
                                                <input
                                                    className="input"
                                                    value={editHub?.cidade || ""}
                                                    onChange={(e) =>
                                                        setEditHub({ ...editHub!, cidade: e.target.value })
                                                    }
                                                />
                                            </td>
                                            <td className="p-3">
                                                <input
                                                    type="number"
                                                    step="0.000001"
                                                    className="input"
                                                    value={editHub?.latitude ?? 0}
                                                    onChange={(e) =>
                                                        setEditHub({
                                                            ...editHub!,
                                                            latitude: +e.target.value,
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-3">
                                                <input
                                                    type="number"
                                                    step="0.000001"
                                                    className="input"
                                                    value={editHub?.longitude ?? 0}
                                                    onChange={(e) =>
                                                        setEditHub({
                                                            ...editHub!,
                                                            longitude: +e.target.value,
                                                        })
                                                    }
                                                />
                                            </td>
                                        </>
                                    ) : (
                                        <>
                                            <td className="p-3 font-medium text-slate-800">{h.nome}</td>
                                            <td className="p-3">{h.cidade}</td>
                                            <td className="p-3">{h.latitude}</td>
                                            <td className="p-3">{h.longitude}</td>
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
                                                            setEditHub(null);
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
                                                            setEditando(h.hub_id!);
                                                            setEditHub(h);
                                                        }}
                                                    >
                                                        <Pencil className="w-4 h-4" /> Editar
                                                    </Button>
                                                    <Button
                                                        size="sm"
                                                        className="flex items-center gap-2 bg-red-600 hover:bg-red-700 text-white"
                                                        onClick={() => remover(h.hub_id)}
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
