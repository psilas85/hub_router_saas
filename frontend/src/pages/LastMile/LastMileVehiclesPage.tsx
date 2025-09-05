// src/pages/LastMile/LastMileVehiclesPage.tsx
// src/pages/LastMile/LastMileVehiclesPage.tsx
import { useEffect, useState } from "react";
import {
    lmListVehicles,
    lmAddVehicle,
    lmUpdateVehicle,
    lmDeleteVehicle,
} from "@/services/lastMileApi";
import type { VehicleCost } from "@/services/lastMileApi";
import toast from "react-hot-toast";
import {
    PlusCircle,
    Pencil,
    Trash2,
    Save,
    XCircle,
    Loader2,
    RefreshCw,
    Truck,
} from "lucide-react";

export default function LastMileVehiclesPage() {
    const [veiculos, setVeiculos] = useState<VehicleCost[]>([]);
    const [novo, setNovo] = useState<VehicleCost>({
        veiculo: "",
        custo_por_km: 0,
        custo_por_entrega: 0,
        peso_minimo_kg: 0,
        peso_maximo_kg: 0,
    });

    const [busca, setBusca] = useState("");
    const [loading, setLoading] = useState(false);
    const [editando, setEditando] = useState<string | null>(null);
    const [editData, setEditData] = useState<VehicleCost | null>(null);

    const carregar = async () => {
        try {
            setLoading(true);
            const data = await lmListVehicles();
            setVeiculos(data);
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

    const adicionar = async () => {
        if (!novo.veiculo) {
            toast.error("Informe o tipo de veículo.");
            return;
        }
        try {
            setLoading(true);
            const added = await lmAddVehicle(novo);
            setVeiculos([...veiculos, added]);
            setNovo({
                veiculo: "",
                custo_por_km: 0,
                custo_por_entrega: 0,
                peso_minimo_kg: 0,
                peso_maximo_kg: 0,
            });
            toast.success("Veículo adicionado!");
        } catch (err) {
            console.error("Erro ao adicionar veículo:", err);
            toast.error("Erro ao adicionar veículo.");
        } finally {
            setLoading(false);
        }
    };

    const editar = (v: VehicleCost) => {
        setEditando(v.veiculo);
        setEditData({ ...v });
    };

    const cancelarEdicao = () => {
        setEditando(null);
        setEditData(null);
    };

    const salvarEdicao = async () => {
        if (!editData) return;
        try {
            setLoading(true);
            const atualizado = await lmUpdateVehicle(editando!, editData);
            setVeiculos(
                veiculos.map((v) => (v.veiculo === editando ? atualizado : v))
            );
            cancelarEdicao();
            toast.success("Veículo atualizado!");
        } catch (err) {
            console.error("Erro ao salvar veículo:", err);
            toast.error("Erro ao salvar veículo.");
        } finally {
            setLoading(false);
        }
    };

    const excluir = async (veiculo: string) => {
        if (!confirm(`Remover o veículo '${veiculo}'?`)) return;
        try {
            await lmDeleteVehicle(veiculo);
            setVeiculos(veiculos.filter((v) => v.veiculo !== veiculo));
            toast.success("Veículo removido.");
        } catch (err) {
            console.error("Erro ao excluir veículo:", err);
            toast.error("Erro ao excluir veículo.");
        }
    };

    const filtrados = veiculos.filter((v) =>
        v.veiculo.toLowerCase().includes(busca.toLowerCase())
    );

    return (
        <div className="p-6 max-w-6xl mx-auto">
            <h1 className="text-2xl font-semibold mb-6 flex items-center gap-2">
                <Truck className="w-6 h-6 text-emerald-600" />
                Last-Mile • Cadastro de Veículos
            </h1>

            {/* Formulário de novo veículo */}
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
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">R$/km</label>
                    <input
                        type="number"
                        step="0.01"
                        className="input text-right"
                        value={novo.custo_por_km}
                        onChange={(e) =>
                            setNovo({ ...novo, custo_por_km: Number(e.target.value) })
                        }
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">R$/entrega</label>
                    <input
                        type="number"
                        step="0.01"
                        className="input text-right"
                        value={novo.custo_por_entrega}
                        onChange={(e) =>
                            setNovo({ ...novo, custo_por_entrega: Number(e.target.value) })
                        }
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">
                        Cap. min (kg)
                    </label>
                    <input
                        type="number"
                        className="input text-right"
                        value={novo.peso_minimo_kg}
                        onChange={(e) =>
                            setNovo({ ...novo, peso_minimo_kg: Number(e.target.value) })
                        }
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-600 mb-1">
                        Cap. máx (kg)
                    </label>
                    <input
                        type="number"
                        className="input text-right"
                        value={novo.peso_maximo_kg}
                        onChange={(e) =>
                            setNovo({ ...novo, peso_maximo_kg: Number(e.target.value) })
                        }
                    />
                </div>
                <div className="md:col-span-5 flex justify-end">
                    <button
                        className="btn flex items-center gap-2"
                        onClick={adicionar}
                        disabled={loading}
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Salvando…
                            </>
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
                    <button className="btn-secondary flex items-center gap-2" onClick={carregar}>
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
                                    {editando === v.veiculo ? (
                                        <>
                                            <td className="p-2">
                                                <input
                                                    className="input"
                                                    value={editData?.veiculo || ""}
                                                    onChange={(e) =>
                                                        setEditData({
                                                            ...editData!,
                                                            veiculo: e.target.value,
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-2">
                                                <input
                                                    type="number"
                                                    step="0.01"
                                                    className="input text-right"
                                                    value={editData?.custo_por_km || 0}
                                                    onChange={(e) =>
                                                        setEditData({
                                                            ...editData!,
                                                            custo_por_km: Number(e.target.value),
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-2">
                                                <input
                                                    type="number"
                                                    step="0.01"
                                                    className="input text-right"
                                                    value={editData?.custo_por_entrega || 0}
                                                    onChange={(e) =>
                                                        setEditData({
                                                            ...editData!,
                                                            custo_por_entrega: Number(e.target.value),
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-2">
                                                <input
                                                    type="number"
                                                    className="input text-right"
                                                    value={editData?.peso_minimo_kg || 0}
                                                    onChange={(e) =>
                                                        setEditData({
                                                            ...editData!,
                                                            peso_minimo_kg: Number(e.target.value),
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-2">
                                                <input
                                                    type="number"
                                                    className="input text-right"
                                                    value={editData?.peso_maximo_kg || 0}
                                                    onChange={(e) =>
                                                        setEditData({
                                                            ...editData!,
                                                            peso_maximo_kg: Number(e.target.value),
                                                        })
                                                    }
                                                />
                                            </td>
                                            <td className="p-2 flex gap-2 justify-center">
                                                <button className="btn flex items-center gap-1" onClick={salvarEdicao}>
                                                    <Save className="w-4 h-4" /> Salvar
                                                </button>
                                                <button
                                                    className="btn-secondary flex items-center gap-1"
                                                    onClick={cancelarEdicao}
                                                >
                                                    <XCircle className="w-4 h-4" /> Cancelar
                                                </button>
                                            </td>
                                        </>
                                    ) : (
                                        <>
                                            <td className="p-2">{v.veiculo}</td>
                                            <td className="p-2 text-right">{v.custo_por_km.toFixed(2)}</td>
                                            <td className="p-2 text-right">{v.custo_por_entrega.toFixed(2)}</td>
                                            <td className="p-2 text-right">{v.peso_minimo_kg}</td>
                                            <td className="p-2 text-right">{v.peso_maximo_kg}</td>
                                            <td className="p-2 flex gap-2 justify-center">
                                                <button
                                                    className="btn-secondary flex items-center gap-1"
                                                    onClick={() => editar(v)}
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
                                        </>
                                    )}
                                </tr>
                            ))}
                            {filtrados.length === 0 && (
                                <tr>
                                    <td className="p-2 text-gray-500 italic text-center" colSpan={6}>
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
