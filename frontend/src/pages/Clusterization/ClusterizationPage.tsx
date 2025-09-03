// frontend/src/pages/Clusterization/ClusterizationPage.tsx
import { useState } from "react";
import api from "@/services/api";

export default function ClusterizationPage() {
    const [data, setData] = useState(""); // apenas uma data
    const [kMin, setKMin] = useState(2);
    const [kMax, setKMax] = useState(50);
    const [minEntregas, setMinEntregas] = useState(25);
    const [fundirClusters, setFundirClusters] = useState(false);
    const [desativarHub, setDesativarHub] = useState(false);
    const [raioHub, setRaioHub] = useState(80.0);
    const [modoForcar, setModoForcar] = useState(false);

    const [loading, setLoading] = useState(false);
    const [resultado, setResultado] = useState<any>(null);
    const [vizData, setVizData] = useState("");
    const [viz, setViz] = useState<any>(null);
    const [vizLoading, setVizLoading] = useState(false);

    // 👉 Executar clusterização
    const executar = async () => {
        if (!data) {
            alert("Informe a data");
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
                    modo_forcar: modoForcar,
                },
            });

            setResultado(res.data);
            if (res.data.datas?.length) {
                setVizData(res.data.datas[0]);
            }
        } catch (err: any) {
            alert("Erro ao executar clusterização: " + (err.response?.data?.detail || err.message));
        } finally {
            setLoading(false);
        }
    };

    // 👉 Visualizar relatórios/maps
    const visualizar = async () => {
        if (!vizData) {
            alert("Selecione uma data para visualizar");
            return;
        }
        setVizLoading(true);
        setViz(null);
        try {
            const res = await api.get("/clusterization/visualizar", {
                params: { data: vizData },
            });
            setViz(res.data);
        } catch (err: any) {
            alert("Erro ao visualizar clusterização: " + (err.response?.data?.detail || err.message));
        } finally {
            setVizLoading(false);
        }
    };

    const buildExportUrl = (path: string) =>
        `${import.meta.env.VITE_API_URL}${path}`;

    return (
        <div className="p-6">
            <div className="max-w-3xl mx-auto bg-white shadow rounded-2xl p-6">
                <h2 className="text-xl font-bold mb-4">📊 Clusterização</h2>

                {/* Formulário de parâmetros */}
                <div className="grid gap-3">
                    <label>
                        Data:
                        <input
                            type="date"
                            value={data}
                            onChange={(e) => setData(e.target.value)}
                            className="border rounded p-2 w-full"
                        />
                    </label>
                    <div className="flex gap-3">
                        <label className="flex-1">
                            Nº mínimo de clusters:
                            <input
                                type="number"
                                min={1}
                                value={kMin}
                                onChange={(e) => setKMin(Number(e.target.value))}
                                className="border rounded p-2 w-full"
                            />
                        </label>
                        <label className="flex-1">
                            Nº máximo de clusters:
                            <input
                                type="number"
                                min={kMin}
                                value={kMax}
                                onChange={(e) => setKMax(Number(e.target.value))}
                                className="border rounded p-2 w-full"
                            />
                        </label>
                    </div>
                    <label>
                        Mínimo de entregas por cluster:
                        <input
                            type="number"
                            min={1}
                            value={minEntregas}
                            onChange={(e) => setMinEntregas(Number(e.target.value))}
                            className="border rounded p-2 w-full"
                        />
                    </label>

                    <label className="flex items-center gap-2">
                        <input
                            type="checkbox"
                            checked={fundirClusters}
                            onChange={(e) => setFundirClusters(e.target.checked)}
                        />
                        Fundir clusters pequenos
                    </label>

                    <label className="flex items-center gap-2">
                        <input
                            type="checkbox"
                            checked={desativarHub}
                            onChange={(e) => setDesativarHub(e.target.checked)}
                        />
                        Desativar cluster Hub Central
                    </label>

                    <label>
                        Raio cluster Hub Central (km):
                        <input
                            type="number"
                            step="0.1"
                            value={raioHub}
                            onChange={(e) => setRaioHub(Number(e.target.value))}
                            className="border rounded p-2 w-full"
                        />
                    </label>

                    <label className="flex items-center gap-2">
                        <input
                            type="checkbox"
                            checked={modoForcar}
                            onChange={(e) => setModoForcar(e.target.checked)}
                        />
                        Forçar reprocessamento
                    </label>

                    <button
                        onClick={executar}
                        disabled={loading}
                        className="bg-emerald-600 text-white rounded-xl px-4 py-2 hover:bg-emerald-700 disabled:opacity-60"
                    >
                        {loading ? "Processando..." : "Executar"}
                    </button>
                </div>

                {/* Resultado da execução */}
                {resultado && (
                    <div className="mt-6 border rounded p-3 bg-gray-50">
                        <p>{resultado.mensagem}</p>
                        {resultado.datas?.length > 0 && (
                            <>
                                <label className="block mt-3">
                                    <span className="text-sm">Data para visualizar:</span>
                                    <select
                                        value={vizData}
                                        onChange={(e) => setVizData(e.target.value)}
                                        className="border rounded p-2 w-full"
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
                                    className="mt-2 bg-emerald-600 text-white rounded-xl px-4 py-2 hover:bg-emerald-700 disabled:opacity-60"
                                >
                                    {vizLoading ? "Gerando..." : "Visualizar"}
                                </button>
                            </>
                        )}
                    </div>
                )}

                {/* Visualização */}
                {viz && (
                    <div className="mt-6 border rounded p-3">
                        <p><b>Arquivos de {viz.data}</b></p>
                        <ul className="list-disc pl-5 text-emerald-700">
                            <li>
                                <a
                                    href={buildExportUrl(viz.arquivos.mapa_html)}
                                    target="_blank"
                                >
                                    📍 Baixar Mapa interativo
                                </a>
                            </li>
                            <li>
                                <a
                                    href={buildExportUrl(viz.arquivos.pdf)}
                                    target="_blank"
                                >
                                    📄 Baixar Relatório PDF
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
