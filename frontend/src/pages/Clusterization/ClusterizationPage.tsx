// src/pages/Clusterization/ClusterizationPage.tsx
import { useEffect, useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { Loader2, Network, PlayCircle, FileText, Map } from "lucide-react";
import { listClusterizationHubs, type ClusterizationHub } from "@/services/clusterizationApi";

type DataDisponivel = {
    data: string;
    quantidade_entregas: number;
};

function normalizeErrorDetail(value: any): string | undefined {
    if (!value) {
        return undefined;
    }
    if (typeof value === "string") {
        return value;
    }
    if (value.detail) {
        return normalizeErrorDetail(value.detail);
    }
    if (value.message) {
        return normalizeErrorDetail(value.message);
    }
    return undefined;
}

function getErrorMessage(err: any) {
    return normalizeErrorDetail(err.response?.data?.detail) || err.message;
}

export default function ClusterizationPage() {
    const [data, setData] = useState(""); // apenas uma data
    const [minEntregasClusterAlvo, setMinEntregasClusterAlvo] = useState(10);
    const [maxEntregasClusterAlvo, setMaxEntregasClusterAlvo] = useState(100);
    const [hubCentralId, setHubCentralId] = useState("");
    const [raioHub, setRaioHub] = useState(80.0);

    const [loading, setLoading] = useState(false);
    const [resultado, setResultado] = useState<any>(null);
    const [vizData, setVizData] = useState("");
    const [viz, setViz] = useState<any>(null);
    const [vizLoading, setVizLoading] = useState(false);
    const [datasDisponiveis, setDatasDisponiveis] = useState<DataDisponivel[]>([]);
    const [datasLoading, setDatasLoading] = useState(false);
    const [hubsCentrais, setHubsCentrais] = useState<ClusterizationHub[]>([]);
    const [hubsLoading, setHubsLoading] = useState(false);

    useEffect(() => {
        const carregarDadosIniciais = async () => {
            setDatasLoading(true);
            setHubsLoading(true);
            try {
                const [datasRes, hubsRes] = await Promise.all([
                    api.get("/clusterization/datas-disponiveis", {
                        params: { limit: 60 },
                    }),
                    listClusterizationHubs(),
                ]);

                const datas = datasRes.data.datas || [];
                const hubs = hubsRes.filter((hub) => hub.ativo && hub.hub_central);

                setHubsCentrais(hubs);
                if (hubs.length > 0) {
                    setHubCentralId((hubAtual) => hubAtual || String(hubs[0].id));
                }

                setDatasDisponiveis(datas);
                if (datas.length > 0) {
                    setData((dataAtual) => dataAtual || datas[0].data);
                }
            } catch (err: any) {
                toast.error("Não foi possível carregar dados iniciais: " + getErrorMessage(err));
            } finally {
                setDatasLoading(false);
                setHubsLoading(false);
            }
        };

        carregarDadosIniciais();
    }, []);

    // 👉 Executar clusterização
    const executar = async () => {
        if (!data) {
            toast.error("Informe a data.");
            return;
        }
        if (minEntregasClusterAlvo < 1 || maxEntregasClusterAlvo < 1) {
            toast.error("As quantidades de entregas por cluster devem ser maiores que zero.");
            return;
        }
        if (minEntregasClusterAlvo > maxEntregasClusterAlvo) {
            toast.error("O mínimo de entregas por cluster não pode ser maior que o máximo.");
            return;
        }
        if (!hubCentralId) {
            toast.error("Selecione o Hub Central.");
            return;
        }
        setLoading(true);
        setResultado(null);
        setViz(null);

        try {
            const payload = {
                data: data,
                min_entregas_por_cluster_alvo: minEntregasClusterAlvo,
                max_entregas_por_cluster_alvo: maxEntregasClusterAlvo,
                hub_central_id: Number(hubCentralId),
                raio_cluster_hub_central: raioHub,
            };

            console.log("[Clusterization] POST /clusterization/processar", {
                params: payload,
            });

            const res = await api.post("/clusterization/processar", null, {
                params: payload,
            });

            setResultado(res.data);
            if (res.data.datas?.length) {
                setVizData(res.data.datas[0]);
            }
            toast.success("Clusterização concluída com sucesso!");
        } catch (err: any) {
            toast.error(
                "Erro ao executar clusterização: " +
                getErrorMessage(err)
            );
        } finally {
            setLoading(false);
        }
    };

    // 👉 Visualizar relatórios/maps
    const visualizar = async () => {
        if (!vizData) {
            toast.error("Selecione uma data para visualizar.");
            return;
        }
        setVizLoading(true);
        setViz(null);
        try {
            const res = await api.get("/clusterization/visualizar", {
                params: { data: vizData },
            });
            setViz(res.data);
            toast.success("Visualização carregada!");
        } catch (err: any) {
            toast.error(
                "Erro ao visualizar clusterização: " +
                getErrorMessage(err)
            );
        } finally {
            setVizLoading(false);
        }
    };

    const buildExportUrl = (path: string) =>
        `${import.meta.env.VITE_API_URL}${path}`;

    return (
        <div className="p-6">
            <div className="max-w-4xl mx-auto bg-white shadow rounded-2xl p-6">
                <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
                    <Network className="w-5 h-5 text-emerald-600" />
                    Clusterização
                </h2>

                {/* Formulário de parâmetros */}
                <div className="grid gap-3">
                    <label className="text-sm font-medium">
                        Data:
                        <input
                            type="date"
                            value={data}
                            onChange={(e) => setData(e.target.value)}
                            className="border rounded px-3 py-2 w-full"
                        />
                    </label>
                    {datasDisponiveis.length > 0 && (
                        <label className="text-sm font-medium">
                            Datas com entregas:
                            <select
                                value={data}
                                onChange={(e) => setData(e.target.value)}
                                className="border rounded px-3 py-2 w-full"
                            >
                                {datasDisponiveis.map((item) => (
                                    <option key={item.data} value={item.data}>
                                        {item.data} - {item.quantidade_entregas} entregas
                                    </option>
                                ))}
                            </select>
                        </label>
                    )}
                    {datasLoading && (
                        <p className="text-sm text-gray-500 flex items-center gap-2">
                            <Loader2 className="w-4 h-4 animate-spin" />
                            Carregando datas disponíveis...
                        </p>
                    )}

                    <div className="flex gap-3">
                        <label className="flex-1 text-sm font-medium">
                            Mín. entregas por cluster alvo:
                            <input
                                type="number"
                                min={1}
                                value={minEntregasClusterAlvo}
                                onChange={(e) => setMinEntregasClusterAlvo(Number(e.target.value))}
                                className="border rounded px-3 py-2 w-full"
                            />
                        </label>
                        <label className="flex-1 text-sm font-medium">
                            Máx. entregas por cluster alvo:
                            <input
                                type="number"
                                min={minEntregasClusterAlvo}
                                value={maxEntregasClusterAlvo}
                                onChange={(e) => setMaxEntregasClusterAlvo(Number(e.target.value))}
                                className="border rounded px-3 py-2 w-full"
                            />
                        </label>
                    </div>

                    <label className="text-sm font-medium">
                        Hub Central:
                        <select
                            value={hubCentralId}
                            onChange={(e) => setHubCentralId(e.target.value)}
                            className="border rounded px-3 py-2 w-full"
                            disabled={hubsLoading}
                        >
                            <option value="">
                                {hubsLoading ? "Carregando hubs..." : "Selecione o Hub Central"}
                            </option>
                            {hubsCentrais.map((hub) => (
                                <option key={hub.id} value={hub.id}>
                                    {hub.nome} - {hub.endereco}
                                </option>
                            ))}
                        </select>
                    </label>
                    {!hubsLoading && hubsCentrais.length === 0 && (
                        <p className="text-sm text-red-600">
                            Cadastre e marque um hub ativo como Hub Central antes de executar.
                        </p>
                    )}

                    <label className="text-sm font-medium">
                        Raio cluster Hub Central (km):
                        <input
                            type="number"
                            step="0.1"
                            value={raioHub}
                            onChange={(e) => setRaioHub(Number(e.target.value))}
                            className="border rounded px-3 py-2 w-full"
                        />
                    </label>

                    <button
                        onClick={executar}
                        disabled={loading}
                        className="bg-emerald-600 text-white rounded-lg px-4 py-2 hover:bg-emerald-700 disabled:opacity-60 flex items-center gap-2"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Processando...
                            </>
                        ) : (
                            <>
                                <PlayCircle className="w-4 h-4" /> Executar
                            </>
                        )}
                    </button>
                </div>

                {/* Resultado da execução */}
                {resultado && (
                    <div className="mt-6 bg-gray-50 border rounded-lg p-4">
                        <p className="font-medium">{resultado.mensagem}</p>
                        {resultado.parametros && (
                            <p className="mt-2 text-sm text-gray-600">
                                Faixa alvo: {resultado.parametros.min_entregas_por_cluster_alvo} a{" "}
                                {resultado.parametros.max_entregas_por_cluster_alvo} entregas por cluster.
                            </p>
                        )}
                        {resultado.datas?.length > 0 && (
                            <>
                                <label className="block mt-3 text-sm">
                                    Data para visualizar:
                                    <select
                                        value={vizData}
                                        onChange={(e) => setVizData(e.target.value)}
                                        className="border rounded px-3 py-2 w-full"
                                    >
                                        {resultado.datas.map((d: string) => (
                                            <option key={d} value={d}>
                                                {d}
                                            </option>
                                        ))}
                                    </select>
                                </label>
                                <button
                                    onClick={visualizar}
                                    disabled={vizLoading}
                                    className="mt-3 bg-emerald-600 text-white rounded-lg px-4 py-2 hover:bg-emerald-700 disabled:opacity-60 flex items-center gap-2"
                                >
                                    {vizLoading ? (
                                        <>
                                            <Loader2 className="w-4 h-4 animate-spin" /> Gerando...
                                        </>
                                    ) : (
                                        <>
                                            <FileText className="w-4 h-4" /> Visualizar
                                        </>
                                    )}
                                </button>
                            </>
                        )}
                    </div>
                )}

                {/* Visualização */}
                {viz && (
                    <div className="mt-6 bg-white border rounded-lg p-4 shadow-sm">
                        <p className="font-semibold mb-2">
                            Arquivos de {viz.data}
                        </p>
                        <ul className="list-disc pl-5 text-emerald-700 text-sm">
                            <li>
                                <a
                                    href={buildExportUrl(viz.arquivos.mapa_html)}
                                    target="_blank"
                                    className="hover:underline flex items-center gap-1"
                                >
                                    <Map className="w-4 h-4" /> Baixar Mapa interativo
                                </a>
                            </li>
                            <li>
                                <a
                                    href={buildExportUrl(viz.arquivos.pdf)}
                                    target="_blank"
                                    className="hover:underline flex items-center gap-1"
                                >
                                    <FileText className="w-4 h-4" /> Baixar Relatório PDF
                                </a>
                            </li>
                        </ul>

                        {/* Prévia do mapa interativo */}
                        <div className="mt-4">
                            <h3 className="font-semibold mb-2">Prévia do mapa interativo:</h3>
                            <iframe
                                src={buildExportUrl(viz.arquivos.mapa_html)}
                                className="w-full h-[500px] border rounded-xl"
                            />
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}
