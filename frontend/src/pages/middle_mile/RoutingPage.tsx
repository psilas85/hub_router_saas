import { useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { Loader2, PlayCircle, FileText, Map } from "lucide-react";

type Artefatos = {
    tenant_id: string;
    data_inicial: string;
    data_final?: string | null;
    map_html_url?: string | null;
    pdf_url?: string | null;
};

// 🔧 helper para normalizar URLs (aceita absoluta ou relativa)
const resolveUrl = (path: string | null | undefined) => {
    if (!path) return null;
    if (path.startsWith("http")) return path;
    return `${import.meta.env.VITE_API_URL}/${path.replace(/^\/+/, "")}`;
};

export default function RoutingPage() {
    const [data, setData] = useState<string>("");
    const [params, setParams] = useState({
        modo_forcar: false,
        tempo_maximo: 1200,
        tempo_parada_leve: 10,
        peso_leve_max: 50,
        tempo_parada_pesada: 20,
        tempo_por_volume: 0.4,
    });
    const [loading, setLoading] = useState(false);
    const [artefatos, setArtefatos] = useState<Artefatos | null>(null);
    const [iframeKey, setIframeKey] = useState(0);

    const canRun = !!data;

    async function processar() {
        if (!canRun) {
            toast.error("Informe a data.");
            return;
        }
        try {
            setLoading(true);
            await api.post("/transfer_routing/processar", null, {
                params: {
                    data_inicial: data,
                    modo_forcar: params.modo_forcar,
                    tempo_maximo: params.tempo_maximo,
                    tempo_parada_leve: params.tempo_parada_leve,
                    peso_leve_max: params.peso_leve_max,
                    tempo_parada_pesada: params.tempo_parada_pesada,
                    tempo_por_volume: params.tempo_por_volume,
                },
            });
            toast.success("Roteirização processada com sucesso!");
            // 🔴 Não busca artefatos aqui
        } catch (e: any) {
            console.error(e);
            toast.error("Erro ao processar a roteirização.");
        } finally {
            setLoading(false);
        }
    }

    async function gerarArtefatosEBaixarPDF() {
        if (!canRun) {
            toast.error("Informe a data.");
            return;
        }
        try {
            setLoading(true);
            const resp = await api.get("/transfer_routing/visualizacao", {
                params: { data_inicial: data },
                responseType: "blob",
            });

            const blob = new Blob([resp.data], { type: "application/pdf" });
            const url = URL.createObjectURL(blob);
            const a = document.createElement("a");
            a.href = url;
            a.download = `relatorio_transferencias_${data}.pdf`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            URL.revokeObjectURL(url);

            toast.success("PDF gerado e baixado!");

            // ✅ Só aqui atualizamos o mapa/artefatos
            await buscarArtefatos();
        } catch (e) {
            console.error(e);
            toast.error("Erro ao gerar/baixar o PDF.");
        } finally {
            setLoading(false);
        }
    }

    async function buscarArtefatos() {
        if (!canRun) return;
        try {
            const { data: resp } = await api.get<Artefatos>(
                "/transfer_routing/artefatos",
                { params: { data_inicial: data } }
            );
            setArtefatos(resp);
            setIframeKey((k) => k + 1);
        } catch {
            setArtefatos(null);
        }
    }

    return (
        <div className="max-w-7xl mx-auto p-6">
            <h1 className="text-2xl font-semibold text-gray-800 mb-6 flex items-center gap-2">
                <PlayCircle className="w-6 h-6 text-emerald-600" />
                Middle-Mile • Roteirização
            </h1>

            {/* Card de parâmetros */}
            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-6">
                <div className="grid grid-cols-1 md:grid-cols-6 gap-3">
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Data
                        </label>
                        <input
                            type="date"
                            value={data}
                            onChange={(e) => setData(e.target.value)}
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 focus:ring-2 focus:ring-emerald-500"
                        />
                    </div>

                    {/* parâmetros principais */}
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Tempo máx. rota (min)
                        </label>
                        <input
                            type="number"
                            min={1}
                            value={params.tempo_maximo}
                            onChange={(e) =>
                                setParams((s) => ({ ...s, tempo_maximo: Number(e.target.value) }))
                            }
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right"
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Parada leve (min)
                        </label>
                        <input
                            type="number"
                            min={0}
                            value={params.tempo_parada_leve}
                            onChange={(e) =>
                                setParams((s) => ({ ...s, tempo_parada_leve: Number(e.target.value) }))
                            }
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right"
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Peso leve máx. (kg)
                        </label>
                        <input
                            type="number"
                            min={0}
                            value={params.peso_leve_max}
                            onChange={(e) =>
                                setParams((s) => ({ ...s, peso_leve_max: Number(e.target.value) }))
                            }
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right"
                        />
                    </div>
                    <div>
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Parada pesada (min)
                        </label>
                        <input
                            type="number"
                            min={0}
                            value={params.tempo_parada_pesada}
                            onChange={(e) =>
                                setParams((s) => ({ ...s, tempo_parada_pesada: Number(e.target.value) }))
                            }
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right"
                        />
                    </div>
                    <div className="md:col-span-2">
                        <label className="block text-sm font-medium text-gray-700 mb-1">
                            Tempo por volume (min/vol.)
                        </label>
                        <input
                            type="number"
                            step="0.01"
                            min={0}
                            value={params.tempo_por_volume}
                            onChange={(e) =>
                                setParams((s) => ({ ...s, tempo_por_volume: Number(e.target.value) }))
                            }
                            className="w-full rounded-lg border border-gray-300 px-3 py-2 text-right"
                        />
                    </div>

                    <div className="flex items-center gap-2 pt-6">
                        <input
                            id="modo_forcar"
                            type="checkbox"
                            checked={params.modo_forcar}
                            onChange={(e) =>
                                setParams((s) => ({ ...s, modo_forcar: e.target.checked }))
                            }
                            className="h-4 w-4 text-emerald-600 border-gray-300 rounded"
                        />
                        <label
                            htmlFor="modo_forcar"
                            className="text-sm text-gray-700 cursor-pointer"
                        >
                            Forçar sobrescrita
                        </label>
                    </div>
                </div>

                {/* Ações */}
                <div className="mt-4 flex flex-wrap gap-3">
                    <button
                        onClick={processar}
                        disabled={!canRun || loading}
                        className="bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg flex items-center gap-2 disabled:opacity-60"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Processando...
                            </>
                        ) : (
                            <>
                                <PlayCircle className="w-4 h-4" /> Processar Roteirização
                            </>
                        )}
                    </button>

                    <button
                        onClick={gerarArtefatosEBaixarPDF}
                        disabled={!canRun || loading}
                        className="bg-sky-600 hover:bg-sky-700 text-white px-4 py-2 rounded-lg flex items-center gap-2 disabled:opacity-60"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Gerando...
                            </>
                        ) : (
                            <>
                                <FileText className="w-4 h-4" /> Gerar & Baixar PDF
                            </>
                        )}
                    </button>
                </div>
            </div>

            {/* Artefatos */}
            {artefatos && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4 mb-6">
                    <div className="flex flex-wrap items-center gap-3">
                        {artefatos.map_html_url && (
                            <a
                                href={resolveUrl(artefatos.map_html_url) || "#"}
                                target="_blank"
                                rel="noreferrer"
                                className="bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg flex items-center gap-2"
                                download
                            >
                                <Map className="w-4 h-4" /> Baixar Mapa HTML
                            </a>
                        )}
                        {artefatos.pdf_url && (
                            <a
                                href={resolveUrl(artefatos.pdf_url) || "#"}
                                target="_blank"
                                rel="noreferrer"
                                className="bg-emerald-600 hover:bg-emerald-700 text-white px-4 py-2 rounded-lg flex items-center gap-2"
                                download
                            >
                                <FileText className="w-4 h-4" /> Baixar PDF
                            </a>
                        )}
                    </div>

                    {artefatos.map_html_url && (
                        <div className="mt-4 border rounded-lg overflow-hidden">
                            <iframe
                                key={iframeKey}
                                src={`${resolveUrl(artefatos.map_html_url) || ""}?v=${iframeKey}`}
                                title="Mapa Interativo - Transferências"
                                className="w-full"
                                style={{ height: "70vh" }}
                            />
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
