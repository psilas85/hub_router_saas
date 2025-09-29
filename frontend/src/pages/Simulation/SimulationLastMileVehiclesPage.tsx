//hub_router_1.0.1/frontend/src/pages/Simulation/SimulationLastMileVehiclesPage.tsx

import { useEffect, useState } from "react";
import {
    simListLastMileVehicles,
    simAddLastMileVehicle,
    simDeleteLastMileVehicle,
    type SimulationLastMileVehicle,
} from "@/services/simulationApi";
import toast from "react-hot-toast";
import {
    PlusCircle,
    Trash2,
    Loader2,
    RefreshCw,
    Truck,
    Pencil,
    X,
} from "lucide-react";

export default function SimulationLastMileVehiclesPage() {
    const [veiculos, setVeiculos] = useState<SimulationLastMileVehicle[]>([]);
    const [novo, setNovo] = useState<SimulationLastMileVehicle>({
        veiculo: "",
        capacidade_kg_min: 0,
        capacidade_kg_max: 0,
        tarifa_km: 0,
        tarifa_entrega: 0,
    });
    const [editando, setEditando] = useState<string | null>(null);
    const [busca, setBusca] = useState("");
    const [loading, setLoading] = useState(false);

    const carregar = async () => {
        try {
            setLoading(true);
            const data = await simListLastMileVehicles();
            setVeiculos(data);
            console.log("Dados recebidos lastmile:", data);
        } catch (err) {
            console.error("Erro ao carregar veículos:", err);
            toast.error("Erro ao carregar veículos.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        carregar();
    }, []);

    const salvar = async () => {
        if (!novo.veiculo) {
            toast.error("Informe o tipo de veículo.");
            return;
        }
        try {
            setLoading(true);
            await simAddLastMileVehicle(novo); // upsert
            await carregar();
            toast.success(editando ? "Veículo atualizado!" : "Veículo adicionado!");
            setNovo({
                veiculo: "",
                capacidade_kg_min: 0,
                capacidade_kg_max: 0,
                tarifa_km: 0,
                tarifa_entrega: 0,
            });
            setEditando(null);
        } catch (err) {
            console.error("Erro ao salvar veículo:", err);
            toast.error("Erro ao salvar veículo.");
        } finally {
            setLoading(false);
        }
    };

    const iniciarEdicao = (v: SimulationLastMileVehicle) => {
        setNovo(v);
        setEditando(v.veiculo);
    };

    const cancelarEdicao = () => {
        setNovo({
            veiculo: "",
            capacidade_kg_min: 0,
            capacidade_kg_max: 0,
            tarifa_km: 0,
            tarifa_entrega: 0,
        });
        setEditando(null);
    };

    const excluir = async (veiculo: string) => {
        if (!confirm(`Remover o veículo '${veiculo}'?`)) return;
        try {
            await simDeleteLastMileVehicle(veiculo);
            setVeiculos(veiculos.filter((v) => v.veiculo !== veiculo));
            toast.success("Veículo removido.");
        } catch (err) {
            console.error("Erro ao excluir veículo:", err);
            toast.error("Erro ao excluir veículo.");
        }
    };

    const filtrados = veiculos.filter((v) =>
        (v.veiculo || "").toLowerCase().includes(busca.toLowerCase())
    );

    return (
        <div className="p-6 max-w-6xl mx-auto">
            <h1 className="text-2xl font-semibold mb-6 flex items-center gap-2">
                <Truck className="w-6 h-6 text-emerald-600" />
                Simulação • Veículos Last-Mile
            </h1>

            {/* Formulário de novo/edição */}
            <div className="bg-white rounded-2xl shadow p-4 grid grid-cols-1 md:grid-cols-5 gap-3 mb-6">
                <div>
                    <label className="block text-sm text-gray-600 mb-1">
                        Tipo de veículo
                    </label>
                    <input
                        type="text"
                        className="input"
                        value={novo.veiculo}
                        onChange={(e) => setNovo({ ...novo, veiculo: e.target.value })}
                        disabled={!!editando}
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">R$/km</label>
                    <input
                        type="number"
                        step="0.01"
                        className="input text-right"
                        value={novo.tarifa_km}
                        onChange={(e) =>
                            setNovo({ ...novo, tarifa_km: Number(e.target.value) })
                        }
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">R$/entrega</label>
                    <input
                        type="number"
                        step="0.01"
                        className="input text-right"
                        value={novo.tarifa_entrega}
                        onChange={(e) =>
                            setNovo({ ...novo, tarifa_entrega: Number(e.target.value) })
                        }
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">Cap. min (kg)</label>
                    <input
                        type="number"
                        className="input text-right"
                        value={novo.capacidade_kg_min}
                        onChange={(e) =>
                            setNovo({ ...novo, capacidade_kg_min: Number(e.target.value) })
                        }
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">Cap. máx (kg)</label>
                    <input
                        type="number"
                        className="input text-right"
                        value={novo.capacidade_kg_max}
                        onChange={(e) =>
                            setNovo({ ...novo, capacidade_kg_max: Number(e.target.value) })
                        }
                    />
                </div>
                <div className="md:col-span-5 flex justify-end gap-2">
                    {editando && (
                        <button className="btn-secondary flex items-center gap-2" onClick={cancelarEdicao}>
                            <X className="w-4 h-4" /> Cancelar
                        </button>
                    )}
                    <button
                        className="btn flex items-center gap-2"
                        onClick={salvar}
                        disabled={loading}
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Salvando…
                            </>
                        ) : editando ? (
                            <>💾 Salvar alterações</>
                        ) : (
                            <>
                                <PlusCircle className="w-4 h-4" /> Adicionar
                            </>
                        )}
                    </button>
                </div>
            </div>

            {/* Barra de busca */}
            <div className="bg-white rounded-2xl shadow p-4 mb-6">
                <label className="block text-sm text-gray-600 mb-1">
                    Buscar por tipo de veículo
                </label>
                <div className="flex gap-3">
                    <input
                        type="text"
                        placeholder="Ex: moto, fiorino..."
                        className="input flex-1"
                        value={busca}
                        onChange={(e) => setBusca(e.target.value)}
                    />
                    <button
                        className="btn-secondary flex items-center gap-2"
                        onClick={carregar}
                    >
                        <RefreshCw className="w-4 h-4" /> Atualizar
                    </button>
                </div>
            </div>

            {/* Tabela */}
            <div className="bg-white rounded-2xl shadow p-4 overflow-x-auto">
                {loading ? (
                    <p className="text-gray-500 italic">Carregando...</p>
                ) : (
                    <table className="w-full text-sm text-left border-collapse">
                        <thead>
                            <tr className="border-b bg-gray-50">
                                <th className="p-2">Tipo</th>
                                <th className="p-2 text-right">R$/km</th>
                                <th className="p-2 text-right">R$/entrega</th>
                                <th className="p-2 text-right">Cap. min (kg)</th>
                                <th className="p-2 text-right">Cap. máx (kg)</th>
                                <th className="p-2 text-center">Ações</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                            {filtrados.map((v, i) => (
                                <tr key={i} className="hover:bg-gray-50">
                                    <td className="p-2">{v.veiculo}</td>
                                    <td className="p-2 text-right">{v.tarifa_km.toFixed(2)}</td>
                                    <td className="p-2 text-right">{v.tarifa_entrega.toFixed(2)}</td>
                                    <td className="p-2 text-right">{v.capacidade_kg_min}</td>
                                    <td className="p-2 text-right">{v.capacidade_kg_max}</td>
                                    <td className="p-2 flex gap-2 justify-center">
                                        <button
                                            className="btn-warning flex items-center gap-1"
                                            onClick={() => iniciarEdicao(v)}
                                        >
                                            <Pencil className="w-4 h-4" /> Editar
                                        </button>
                                        <button
                                            className="btn-danger flex items-center gap-1"
                                            onClick={() => excluir(v.veiculo)}
                                        >
                                            <Trash2 className="w-4 h-4" /> Excluir
                                        </button>
                                    </td>
                                </tr>
                            ))}
                            {filtrados.length === 0 && (
                                <tr>
                                    <td
                                        className="p-2 text-gray-500 italic text-center"
                                        colSpan={6}
                                    >
                                        Nenhum veículo encontrado.
                                    </td>
                                </tr>
                            )}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    );
}
