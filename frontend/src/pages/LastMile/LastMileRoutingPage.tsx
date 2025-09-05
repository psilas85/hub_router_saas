// src/pages/LastMile/LastMileRoutingPage.tsx
// src/pages/LastMile/LastMileRoutingPage.tsx
import { useState } from "react";
import { lmProcessRouting, lmBuscarArtefatos } from "@/services/lastMileApi";
import type { Artefato } from "@/services/lastMileApi";
import toast from "react-hot-toast";
import { Loader2, PlayCircle, FileText, Map } from "lucide-react";
import api from "@/services/api";


function todayISO() {
    const d = new Date();
    return d.toISOString().slice(0, 10);
}

export default function LastMileRoutingPage() {
    const [data, setData] = useState("");

    // parâmetros avançados (alinhados ao backend)
    const [entregasPorSub, setEntregasPorSub] = useState(25);
    const [tempoMax, setTempoMax] = useState(1200);
    const [paradaLeve, setParadaLeve] = useState(10);
    const [paradaPesada, setParadaPesada] = useState(20);
    const [tempoVol, setTempoVol] = useState(0.4);
    const [pesoLeveMax, setPesoLeveMax] = useState(50);
    const [restricaoLeve, setRestricaoLeve] = useState(false);
    const [modoForcar, setModoForcar] = useState(false);

    const [loading, setLoading] = useState(false);
    const [artefatos, setArtefatos] = useState<Artefato[]>([]);

    const dataValida = () => Boolean(data);

    async function processar() {
        if (!dataValida()) {
            toast.error("Informe a data.");
            return;
        }
        setLoading(true);
        try {
            await lmProcessRouting({
                data_inicial: data,
                entregas_por_subcluster: entregasPorSub,
                tempo_maximo_rota: tempoMax,
                tempo_parada_leve: paradaLeve,
                tempo_parada_pesada: paradaPesada,
                tempo_descarga_por_volume: tempoVol,
                peso_leve_max: pesoLeveMax,
                restricao_veiculo_leve_municipio: restricaoLeve,
                modo_forcar: modoForcar,
            });
            toast.success("Roteirização processada!");
        } catch (err) {
            console.error(err);
            toast.error("Erro ao processar roteirização.");
        } finally {
            setLoading(false);
        }
    }

    async function gerarArtefatos() {
        if (!dataValida()) {
            toast.error("Informe a data.");
            return;
        }
        setLoading(true);
        try {
            // 1️⃣ dispara geração no backend
            await api.get("/last_mile_routing/visualizar", {
                params: { data_inicial: data }
            });

            // 2️⃣ busca os arquivos gerados
            const resp = await lmBuscarArtefatos(data);
            setArtefatos(resp?.artefatos ?? []);

            if (!resp?.artefatos || resp.artefatos.length === 0) {
                toast("Nenhum artefato encontrado para esta data.", { icon: "⚠️" });
            } else {
                toast.success("Artefatos gerados e carregados!");
            }
        } catch (err) {
            console.error(err);
            toast.error("Erro ao gerar artefatos.");
        } finally {
            setLoading(false);
        }
    }


    async function baixarPDF(url: string) {
        try {
            const resp = await fetch(url);
            const blob = await resp.blob();
            const link = document.createElement("a");
            link.href = URL.createObjectURL(blob);
            link.download = url.split("/").pop() || "relatorio.pdf";
            link.click();
            toast.success("PDF baixado!");
        } catch (err) {
            console.error("Erro ao baixar PDF:", err);
            toast.error("Não foi possível baixar o PDF.");
        }
    }

    return (
        <div className="p-6 max-w-6xl mx-auto">
            <h1 className="text-2xl font-semibold mb-6 flex items-center gap-2">
                <PlayCircle className="w-6 h-6 text-emerald-600" />
                Last-Mile • Roteirização
            </h1>

            {/* 🔹 Formulário */}
            <div className="bg-white rounded-2xl shadow p-4 grid grid-cols-1 md:grid-cols-6 gap-3 mb-6">
                <div>
                    <label className="text-sm text-gray-600">Data</label>
                    <input
                        type="date"
                        className="input"
                        value={data}
                        onChange={(e) => setData(e.target.value)}
                        max={todayISO()}
                    />
                </div>

                <div>
                    <label className="text-sm text-gray-600">Entregas / subcluster</label>
                    <input
                        className="input"
                        type="number"
                        value={entregasPorSub}
                        onChange={(e) => setEntregasPorSub(Number(e.target.value))}
                    />
                </div>
                <div>
                    <label className="text-sm text-gray-600">Tempo máx. rota (min)</label>
                    <input
                        className="input"
                        type="number"
                        value={tempoMax}
                        onChange={(e) => setTempoMax(Number(e.target.value))}
                    />
                </div>
                <div>
                    <label className="text-sm text-gray-600">Parada leve (min)</label>
                    <input
                        className="input"
                        type="number"
                        value={paradaLeve}
                        onChange={(e) => setParadaLeve(Number(e.target.value))}
                    />
                </div>
                <div>
                    <label className="text-sm text-gray-600">Parada pesada (min)</label>
                    <input
                        className="input"
                        type="number"
                        value={paradaPesada}
                        onChange={(e) => setParadaPesada(Number(e.target.value))}
                    />
                </div>
                <div>
                    <label className="text-sm text-gray-600">
                        Tempo por volume (min/vol.)
                    </label>
                    <input
                        className="input"
                        type="number"
                        step="0.01"
                        value={tempoVol}
                        onChange={(e) => setTempoVol(Number(e.target.value))}
                    />
                </div>
                <div>
                    <label className="text-sm text-gray-600">Peso leve máx. (kg)</label>
                    <input
                        className="input"
                        type="number"
                        value={pesoLeveMax}
                        onChange={(e) => setPesoLeveMax(Number(e.target.value))}
                    />
                </div>

                <div className="md:col-span-3 flex items-center gap-2">
                    <input
                        id="rest"
                        type="checkbox"
                        checked={restricaoLeve}
                        onChange={(e) => setRestricaoLeve(e.target.checked)}
                    />
                    <label htmlFor="rest" className="text-sm">
                        Restringir veículos leves em rotas intermunicipais
                    </label>
                </div>
                <div className="md:col-span-3 flex items-center gap-2">
                    <input
                        id="forcar"
                        type="checkbox"
                        checked={modoForcar}
                        onChange={(e) => setModoForcar(e.target.checked)}
                    />
                    <label htmlFor="forcar" className="text-sm">
                        Forçar sobrescrita
                    </label>
                </div>

                <div className="md:col-span-6 flex gap-3">
                    <button
                        className="btn flex items-center gap-2"
                        onClick={processar}
                        disabled={loading}
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Processando…
                            </>
                        ) : (
                            <>
                                <PlayCircle className="w-4 h-4" /> Processar Roteirização
                            </>
                        )}
                    </button>
                    <button
                        className="btn-secondary flex items-center gap-2"
                        onClick={gerarArtefatos}
                        disabled={loading}
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Gerando…
                            </>
                        ) : (
                            <>
                                <Map className="w-4 h-4" /> Gerar Mapas & Relatórios
                            </>
                        )}
                    </button>
                </div>
            </div>

            {/* 🔹 Lista de Artefatos */}
            {artefatos.length > 0 &&
                artefatos.map((a) => (
                    <div key={a.data} className="bg-white rounded-xl shadow p-4 mb-6">
                        <h2 className="font-semibold mb-3 flex items-center gap-2">
                            <Map className="w-5 h-5 text-emerald-600" />
                            Artefatos {a.data}
                        </h2>
                        <div className="flex gap-3 mb-4">
                            <a
                                href={a.map_html_url}
                                target="_blank"
                                rel="noreferrer"
                                className="btn flex items-center gap-2"
                            >
                                <Map className="w-4 h-4" /> Baixar Mapa HTML
                            </a>
                            <button
                                className="btn flex items-center gap-2"
                                onClick={() => baixarPDF(a.pdf_url)}
                            >
                                <FileText className="w-4 h-4" /> Baixar PDF
                            </button>
                        </div>
                        <iframe
                            src={a.map_html_url}
                            title={`Mapa ${a.data}`}
                            className="w-full rounded-lg border"
                            style={{ height: "70vh" }}
                        />
                    </div>
                ))}
        </div>
    );
}
