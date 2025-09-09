import { useEffect, useState } from "react";
import { listHubs, createHub, updateHub, deleteHub, type Hub } from "@/services/simulationApi";
import { Plus, Trash2 } from "lucide-react";
import toast from "react-hot-toast";
import { Button } from "@/components/ui/button";

export default function SimulationHubsPage() {
    const [hubs, setHubs] = useState<Hub[]>([]);
    const [novo, setNovo] = useState<Hub>({ nome: "", cidade: "", latitude: 0, longitude: 0 });

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
            toast.success("Hub criado!");
            setNovo({ nome: "", cidade: "", latitude: 0, longitude: 0 });
            carregar();
        } catch {
            toast.error("Erro ao criar hub");
        }
    };

    const salvarEdicao = async () => {
        if (!editHub || !editHub.hub_id) return;
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
            <h1 className="text-3xl font-bold text-slate-800 mb-2">📍 Cadastro de Hubs</h1>
            <p className="text-slate-500 mb-8">
                Gerencie os hubs de distribuição utilizados nas simulações.
            </p>

            {/* Formulário novo hub */}
            <div className="bg-white rounded-xl shadow-md p-6 mb-10 grid grid-cols-5 gap-4">
                <input
                    className="rounded-lg border border-slate-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-slate-500"
                    placeholder="Nome"
                    value={novo.nome}
                    onChange={(e) => setNovo({ ...novo, nome: e.target.value })}
                />
                <input
                    className="rounded-lg border border-slate-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-slate-500"
                    placeholder="Cidade"
                    value={novo.cidade}
                    onChange={(e) => setNovo({ ...novo, cidade: e.target.value })}
                />
                <input
                    className="rounded-lg border border-slate-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-slate-500"
                    placeholder="Latitude"
                    type="number"
                    value={novo.latitude}
                    onChange={(e) => setNovo({ ...novo, latitude: +e.target.value })}
                />
                <input
                    className="rounded-lg border border-slate-300 px-3 py-2 focus:outline-none focus:ring-2 focus:ring-slate-500"
                    placeholder="Longitude"
                    type="number"
                    value={novo.longitude}
                    onChange={(e) => setNovo({ ...novo, longitude: +e.target.value })}
                />
                <Button onClick={salvarNovo} className="flex items-center gap-2 rounded-lg">
                    <Plus className="w-4 h-4" /> Adicionar
                </Button>
            </div>

            {/* Tabela */}
            <div className="bg-white rounded-xl shadow-md overflow-hidden">
                <table className="min-w-full text-sm">
                    <thead className="bg-slate-100 text-slate-700">
                        <tr>
                            <th className="py-3 px-4 text-left font-medium">Nome</th>
                            <th className="py-3 px-4 text-left font-medium">Cidade</th>
                            <th className="py-3 px-4 text-left font-medium">Latitude</th>
                            <th className="py-3 px-4 text-left font-medium">Longitude</th>
                            <th className="py-3 px-4 text-right font-medium">Ações</th>
                        </tr>
                    </thead>
                    <tbody>
                        {hubs.map((h) => (
                            <tr
                                key={h.hub_id ?? h.nome}
                                className="border-b last:border-0 hover:bg-slate-50"
                            >
                                {editando === h.hub_id ? (
                                    <>
                                        <td className="py-3 px-4">
                                            <input
                                                className="rounded-lg border border-slate-300 px-2 py-1 w-full focus:outline-none focus:ring-2 focus:ring-slate-500"
                                                value={editHub?.nome || ""}
                                                onChange={(e) =>
                                                    setEditHub({ ...editHub!, nome: e.target.value })
                                                }
                                            />
                                        </td>
                                        <td className="py-3 px-4">
                                            <input
                                                className="rounded-lg border border-slate-300 px-2 py-1 w-full focus:outline-none focus:ring-2 focus:ring-slate-500"
                                                value={editHub?.cidade || ""}
                                                onChange={(e) =>
                                                    setEditHub({ ...editHub!, cidade: e.target.value })
                                                }
                                            />
                                        </td>
                                        <td className="py-3 px-4">
                                            <input
                                                type="number"
                                                className="rounded-lg border border-slate-300 px-2 py-1 w-full focus:outline-none focus:ring-2 focus:ring-slate-500"
                                                value={editHub?.latitude ?? 0}
                                                onChange={(e) =>
                                                    setEditHub({ ...editHub!, latitude: +e.target.value })
                                                }
                                            />
                                        </td>
                                        <td className="py-3 px-4">
                                            <input
                                                type="number"
                                                className="rounded-lg border border-slate-300 px-2 py-1 w-full focus:outline-none focus:ring-2 focus:ring-slate-500"
                                                value={editHub?.longitude ?? 0}
                                                onChange={(e) =>
                                                    setEditHub({ ...editHub!, longitude: +e.target.value })
                                                }
                                            />
                                        </td>
                                    </>
                                ) : (
                                    <>
                                        <td className="py-3 px-4">{h.nome}</td>
                                        <td className="py-3 px-4">{h.cidade}</td>
                                        <td className="py-3 px-4">{h.latitude}</td>
                                        <td className="py-3 px-4">{h.longitude}</td>
                                    </>
                                )}

                                <td className="py-3 px-4 text-right flex gap-2 justify-end">
                                    {editando === h.hub_id ? (
                                        <>
                                            <Button onClick={salvarEdicao} variant="default" className="rounded-lg">
                                                Salvar
                                            </Button>
                                            <Button
                                                onClick={() => {
                                                    setEditando(null);
                                                    setEditHub(null);
                                                }}
                                                variant="secondary"
                                                className="rounded-lg"
                                            >
                                                Cancelar
                                            </Button>
                                        </>
                                    ) : (
                                        <>
                                            <Button
                                                onClick={() => {
                                                    setEditando(h.hub_id!);
                                                    setEditHub(h);
                                                }}
                                                variant="outline"
                                                className="rounded-lg"
                                            >
                                                Editar
                                            </Button>
                                            <Button
                                                onClick={() => remover(h.hub_id)}
                                                variant="destructive"
                                                className="flex items-center gap-2 rounded-lg"
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
        </div>
    );
}
