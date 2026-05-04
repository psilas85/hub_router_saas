import { useEffect, useState } from "react";
import { MapPin, Pencil, Plus, Trash2 } from "lucide-react";
import toast from "react-hot-toast";
import { Button } from "@/components/ui/button";
import {
    createClusterizationHub,
    deleteClusterizationHub,
    listClusterizationHubs,
    updateClusterizationHub,
    type ClusterizationHub,
    type ClusterizationHubCreate,
} from "@/services/clusterizationApi";

type HubForm = Omit<ClusterizationHubCreate, "latitude" | "longitude"> & {
    latitude: string;
    longitude: string;
};

const emptyHub: HubForm = {
    nome: "",
    endereco: "",
    latitude: "",
    longitude: "",
    hub_central: false,
    centro_cluster: false,
    ativo: true,
};

const parseCoordinate = (value: string) => Number(value.replace(",", "."));

const toPayload = (hub: HubForm): ClusterizationHubCreate => ({
    ...hub,
    latitude: parseCoordinate(hub.latitude),
    longitude: parseCoordinate(hub.longitude),
});

export default function ClusterizationHubsPage() {
    const [hubs, setHubs] = useState<ClusterizationHub[]>([]);
    const [novo, setNovo] = useState<HubForm>(emptyHub);
    const [editando, setEditando] = useState<number | null>(null);
    const [editHub, setEditHub] = useState<HubForm | null>(null);
    const [loading, setLoading] = useState(false);

    const carregar = async () => {
        setLoading(true);
        try {
            setHubs(await listClusterizationHubs());
        } catch {
            toast.error("Erro ao carregar hubs");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        carregar();
    }, []);

    const validar = (hub: HubForm) => {
        if (!hub.nome.trim() || !hub.endereco.trim()) {
            toast.error("Informe nome e endereço.");
            return false;
        }
        const latitude = parseCoordinate(hub.latitude);
        const longitude = parseCoordinate(hub.longitude);
        if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
            toast.error("Informe latitude e longitude válidas.");
            return false;
        }
        if (latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180) {
            toast.error("Latitude deve estar entre -90 e 90; longitude entre -180 e 180.");
            return false;
        }
        return true;
    };

    const salvarNovo = async () => {
        if (!validar(novo)) return;
        try {
            await createClusterizationHub(toPayload(novo));
            toast.success("Hub cadastrado!");
            setNovo(emptyHub);
            carregar();
        } catch {
            toast.error("Erro ao criar hub");
        }
    };

    const salvarEdicao = async () => {
        if (!editando || !editHub || !validar(editHub)) return;
        try {
            await updateClusterizationHub(editando, toPayload(editHub));
            toast.success("Hub atualizado!");
            setEditando(null);
            setEditHub(null);
            carregar();
        } catch {
            toast.error("Erro ao atualizar hub");
        }
    };

    const remover = async (id: number) => {
        try {
            await deleteClusterizationHub(id);
            toast.success("Hub removido!");
            carregar();
        } catch {
            toast.error("Erro ao remover hub");
        }
    };

    const Badge = ({ active, children }: { active: boolean; children: string }) => (
        <span className={`inline-flex rounded px-2 py-1 text-xs font-medium ${active ? "bg-emerald-100 text-emerald-700" : "bg-slate-100 text-slate-500"}`}>
            {children}
        </span>
    );

    return (
        <div className="p-6 max-w-6xl mx-auto">
            <header className="mb-6">
                <h1 className="text-2xl md:text-3xl font-semibold text-slate-800 flex items-center gap-2">
                    <MapPin className="w-7 h-7 text-emerald-600" />
                    Cadastro de Hubs
                </h1>
                <p className="text-slate-500 mt-1">
                    Gerencie o Hub Central usado na clusterização e marque centros de cluster para modos futuros.
                </p>
            </header>

            <section className="bg-white rounded-lg shadow p-5 mb-8">
                <div className="flex items-center gap-2 mb-4">
                    <Plus className="w-5 h-5 text-emerald-600" />
                    <h2 className="font-medium text-slate-800">Adicionar hub</h2>
                </div>

                <div className="grid md:grid-cols-5 gap-4">
                    <label className="block text-sm text-slate-600">
                        Nome
                        <input className="input mt-1" placeholder="Ex.: Hub Jandira" value={novo.nome} onChange={(e) => setNovo({ ...novo, nome: e.target.value })} />
                    </label>
                    <label className="block text-sm text-slate-600">
                        Endereço
                        <input className="input mt-1" placeholder="Rua, bairro, cidade - UF" value={novo.endereco} onChange={(e) => setNovo({ ...novo, endereco: e.target.value })} />
                    </label>
                    <label className="block text-sm text-slate-600">
                        Latitude
                        <input type="text" inputMode="decimal" className="input mt-1" placeholder="-23.517797" value={novo.latitude} onChange={(e) => setNovo({ ...novo, latitude: e.target.value })} />
                    </label>
                    <label className="block text-sm text-slate-600">
                        Longitude
                        <input type="text" inputMode="decimal" className="input mt-1" placeholder="-46.914575" value={novo.longitude} onChange={(e) => setNovo({ ...novo, longitude: e.target.value })} />
                    </label>
                    <div className="flex items-end">
                        <Button onClick={salvarNovo} className="w-full flex items-center gap-2 bg-emerald-600 hover:bg-emerald-700 text-white">
                            <Plus className="w-4 h-4" /> Adicionar
                        </Button>
                    </div>
                </div>

                <div className="flex flex-wrap gap-5 mt-4 text-sm">
                    <label className="flex items-center gap-2">
                        <input type="checkbox" checked={novo.hub_central} onChange={(e) => setNovo({ ...novo, hub_central: e.target.checked })} />
                        Hub Central
                    </label>
                    <label className="flex items-center gap-2">
                        <input type="checkbox" checked={novo.centro_cluster} onChange={(e) => setNovo({ ...novo, centro_cluster: e.target.checked })} />
                        Centro de cluster
                    </label>
                    <label className="flex items-center gap-2">
                        <input type="checkbox" checked={novo.ativo} onChange={(e) => setNovo({ ...novo, ativo: e.target.checked })} />
                        Ativo
                    </label>
                </div>
            </section>

            <section className="bg-white rounded-lg shadow overflow-x-auto">
                <table className="min-w-full text-sm">
                    <thead className="bg-slate-50 text-slate-600">
                        <tr>
                            <th className="p-3 text-left">Nome</th>
                            <th className="p-3 text-left">Endereço</th>
                            <th className="p-3 text-left">Latitude</th>
                            <th className="p-3 text-left">Longitude</th>
                            <th className="p-3 text-left">Atributos</th>
                            <th className="p-3 text-right">Ações</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y">
                        {loading && (
                            <tr>
                                <td className="p-4 text-slate-500" colSpan={6}>Carregando hubs...</td>
                            </tr>
                        )}
                        {!loading && hubs.length === 0 && (
                            <tr>
                                <td className="p-4 text-slate-500" colSpan={6}>Nenhum hub cadastrado.</td>
                            </tr>
                        )}
                        {hubs.map((hub) => {
                            const isEditing = editando === hub.id;
                            return (
                                <tr key={hub.id} className="hover:bg-slate-50/60">
                                    {isEditing ? (
                                        <>
                                            <td className="p-3"><input className="input" value={editHub?.nome || ""} onChange={(e) => setEditHub({ ...editHub!, nome: e.target.value })} /></td>
                                            <td className="p-3"><input className="input" value={editHub?.endereco || ""} onChange={(e) => setEditHub({ ...editHub!, endereco: e.target.value })} /></td>
                                            <td className="p-3"><input type="text" inputMode="decimal" className="input" value={editHub?.latitude ?? ""} onChange={(e) => setEditHub({ ...editHub!, latitude: e.target.value })} /></td>
                                            <td className="p-3"><input type="text" inputMode="decimal" className="input" value={editHub?.longitude ?? ""} onChange={(e) => setEditHub({ ...editHub!, longitude: e.target.value })} /></td>
                                            <td className="p-3">
                                                <div className="grid gap-2">
                                                    <label className="flex items-center gap-2"><input type="checkbox" checked={editHub?.hub_central || false} onChange={(e) => setEditHub({ ...editHub!, hub_central: e.target.checked })} /> Hub Central</label>
                                                    <label className="flex items-center gap-2"><input type="checkbox" checked={editHub?.centro_cluster || false} onChange={(e) => setEditHub({ ...editHub!, centro_cluster: e.target.checked })} /> Centro de cluster</label>
                                                    <label className="flex items-center gap-2"><input type="checkbox" checked={editHub?.ativo || false} onChange={(e) => setEditHub({ ...editHub!, ativo: e.target.checked })} /> Ativo</label>
                                                </div>
                                            </td>
                                        </>
                                    ) : (
                                        <>
                                            <td className="p-3 font-medium text-slate-800">{hub.nome}</td>
                                            <td className="p-3">{hub.endereco}</td>
                                            <td className="p-3">{hub.latitude}</td>
                                            <td className="p-3">{hub.longitude}</td>
                                            <td className="p-3">
                                                <div className="flex flex-wrap gap-2">
                                                    <Badge active={hub.hub_central}>Hub Central</Badge>
                                                    <Badge active={hub.centro_cluster}>Centro de cluster</Badge>
                                                    <Badge active={hub.ativo}>Ativo</Badge>
                                                </div>
                                            </td>
                                        </>
                                    )}
                                    <td className="p-3">
                                        <div className="flex justify-end gap-2">
                                            {isEditing ? (
                                                <>
                                                    <Button size="sm" className="bg-emerald-600 hover:bg-emerald-700 text-white" onClick={salvarEdicao}>Salvar</Button>
                                                    <Button size="sm" className="bg-gray-300 hover:bg-gray-400 text-slate-800" onClick={() => { setEditando(null); setEditHub(null); }}>Cancelar</Button>
                                                </>
                                            ) : (
                                                <>
                                                    <Button size="sm" className="flex items-center gap-2 bg-blue-600 hover:bg-blue-700 text-white" onClick={() => { setEditando(hub.id); setEditHub({ nome: hub.nome, endereco: hub.endereco, latitude: String(hub.latitude), longitude: String(hub.longitude), hub_central: hub.hub_central, centro_cluster: hub.centro_cluster, ativo: hub.ativo }); }}>
                                                        <Pencil className="w-4 h-4" /> Editar
                                                    </Button>
                                                    <Button size="sm" className="flex items-center gap-2 bg-red-600 hover:bg-red-700 text-white" onClick={() => remover(hub.id)}>
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
