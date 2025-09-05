// src/pages/Clusterization/ClusterizationPage.tsx
import { useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { Loader2, Network, PlayCircle, FileText, Map } from "lucide-react";

export default function ClusterizationPage() {
    const [data, setData] = useState(""); // apenas uma data
    const [kMin, setKMin] = useState(2);
    const [kMax, setKMax] = useState(50);
    const [minEntregas, setMinEntregas] = useState(25);
    const [fundirClusters, setFundirClusters] = useState(false);
    const [desativarHub, setDesativarHub] = useState(false);
    const [raioHub, setRaioHub] = useState(80.0);

    const [loading, setLoading] = useState(false);
    const [resultado, setResultado] = useState<any>(null);
    const [vizData, setVizData] = useState("");
    const [viz, setViz] = useState<any>(null);
    const [vizLoading, setVizLoading] = useState(false);

    // 👉 Executar clusterização
    const executar = async () => {
        if (!data) {
            toast.error("Informe a data.");
            return;
        }
        setLoading(true);
        setResultado(null);
        setViz(null);

        try {
            const res = await api.post("/clusterization/processar", null, {
                params: {
                    data: data,
                    k_min: kMin,
                    k_max: kMax,
                    min_entregas_por_cluster: minEntregas,
                    fundir_clusters_pequenos: fundirClusters,
                    desativar_cluster_hub_central: desativarHub,
                    raio_cluster_hub_central: raioHub,
                },
            });

            setResultado(res.data);
            if (res.data.datas?.length) {
                setVizData(res.data.datas[0]);
            }
            toast.success("Clusterização concluída com sucesso!");
        } catch (err: any) {
            toast.error(
                "Erro ao executar clusterização: " +
                (err.response?.data?.detail || err.message)
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
                (err.response?.data?.detail || err.message)
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

                    <div className="flex gap-3">
                        <label className="flex-1 text-sm font-medium">
                            Nº mínimo de clusters:
                            <input
                                type="number"
                                min={1}
                                value={kMin}
                                onChange={(e) => setKMin(Number(e.target.value))}
                                className="border rounded px-3 py-2 w-full"
                            />
                        </label>
                        <label className="flex-1 text-sm font-medium">
                            Nº máximo de clusters:
                            <input
                                type="number"
                                min={kMin}
                                value={kMax}
                                onChange={(e) => setKMax(Number(e.target.value))}
                                className="border rounded px-3 py-2 w-full"
                            />
                        </label>
                    </div>

                    <label className="text-sm font-medium">
                        Mínimo de entregas por cluster:
                        <input
                            type="number"
                            min={1}
                            value={minEntregas}
                            onChange={(e) => setMinEntregas(Number(e.target.value))}
                            className="border rounded px-3 py-2 w-full"
                        />
                    </label>

                    <label className="flex items-center gap-2 text-sm">
                        <input
                            type="checkbox"
                            checked={fundirClusters}
                            onChange={(e) => setFundirClusters(e.target.checked)}
                        />
                        Fundir clusters pequenos
                    </label>

                    <label className="flex items-center gap-2 text-sm">
                        <input
                            type="checkbox"
                            checked={desativarHub}
                            onChange={(e) => setDesativarHub(e.target.checked)}
                        />
                        Desativar cluster Hub Central
                    </label>

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
