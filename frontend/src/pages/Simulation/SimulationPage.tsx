// hub_router_1.0.1/frontend/src/pages/Simulation/SimulationPage.tsx

import { useState } from "react";
import {
    runSimulation,
    visualizeSimulation,
    type VisualizeSimulationResponse,
} from "@/services/simulationApi";
import toast from "react-hot-toast";
import {
    Play,
    BarChart3,
    FileText,
    Map,
    Loader2,
} from "lucide-react";

function todayISO() {
    const d = new Date();
    return d.toISOString().slice(0, 10);
}

// helper para montar URL absoluta
const resolveUrl = (path: string) => {
    if (!path) return "";
    if (path.startsWith("http")) return path;
    return `${import.meta.env.VITE_API_URL}${path}`;
};

export default function SimulationPage() {
    const [dataInicial, setDataInicial] = useState("");
    const [dataFinal, setDataFinal] = useState("");
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);
    const [artefatos, setArtefatos] = useState<VisualizeSimulationResponse | null>(null);

    const datasValidas = () =>
        Boolean(dataInicial && (!dataFinal || dataFinal >= dataInicial));

    const processar = async () => {
        setMsg(null);
        setArtefatos(null);
        if (!datasValidas()) {
            toast.error("Informe uma data inicial válida (a final é opcional).");
            return;
        }
        setLoading(true);
        try {
            const data = await runSimulation({
                data_inicial: dataInicial,
                data_final: dataFinal || undefined,
                modo_forcar: true,
            });
            setMsg(data.mensagem);
            toast.success("✅ Simulação processada!");
        } catch (e: any) {
            const errMsg = e?.response?.data?.detail || "Erro ao executar simulação.";
            setMsg(errMsg);
            toast.error(errMsg);
        } finally {
            setLoading(false);
        }
    };

    const gerarRelatorios = async () => {
        if (!dataInicial) {
            toast.error("Selecione a data inicial.");
            return;
        }
        try {
            const data = await visualizeSimulation(dataInicial);
            setArtefatos(data);
            setMsg("✅ Artefatos carregados.");
            toast.success("Artefatos carregados!");
        } catch (e: any) {
            const errMsg = e?.response?.data?.detail || "Erro ao carregar artefatos.";
            setMsg(errMsg);
            toast.error(errMsg);
        }
    };

    return (
        <div className="max-w-6xl mx-auto p-6">
            <h1 className="text-2xl font-bold mb-6 flex items-center gap-2">
                <BarChart3 className="w-6 h-6 text-emerald-600" />
                Simulação
            </h1>

            {/* Formulário */}
            <div className="grid md:grid-cols-4 gap-4 mb-6 bg-white rounded-2xl shadow p-4">
                <div>
                    <label className="block text-sm text-gray-700">Data inicial</label>
                    <input
                        type="date"
                        value={dataInicial}
                        max={todayISO()}
                        onChange={(e) => setDataInicial(e.target.value)}
                        className="input"
                    />
                </div>
                <div>
                    <label className="block text-sm text-gray-700">Data final (opcional)</label>
                    <input
                        type="date"
                        value={dataFinal}
                        max={todayISO()}
                        onChange={(e) => setDataFinal(e.target.value)}
                        className="input"
                    />
                </div>
                <div className="flex items-end">
                    <button
                        disabled={loading || !datasValidas()}
                        onClick={processar}
                        className="btn w-full flex items-center gap-2"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Processando...
                            </>
                        ) : (
                            <>
                                <Play className="w-4 h-4" /> Processar
                            </>
                        )}
                    </button>
                </div>
                <div className="flex items-end">
                    <button
                        disabled={!dataInicial}
                        onClick={gerarRelatorios}
                        className="btn-secondary w-full flex items-center gap-2"
                    >
                        <FileText className="w-4 h-4" /> Relatórios & Gráficos
                    </button>
                </div>
            </div>

            {/* Mensagens */}
            {msg && (
                <div className="mb-4 text-sm bg-emerald-50 text-emerald-700 border border-emerald-200 rounded-lg p-3">
                    {msg}
                </div>
            )}

            {/* Artefatos */}
            {artefatos && (
                <div className="border rounded-xl p-4 bg-gray-50">
                    <h2 className="font-semibold mb-4">Artefatos {artefatos.data}</h2>

                    {/* PDF Consolidado */}
                    {artefatos.relatorio_pdf && (
                        <div className="mb-4">
                            <a
                                href={resolveUrl(artefatos.relatorio_pdf)}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="btn flex items-center gap-2"
                            >
                                <FileText className="w-4 h-4" /> Baixar Relatório PDF
                            </a>
                        </div>
                    )}

                    {/* Gráfico comparativo */}
                    {artefatos.graficos && artefatos.graficos.length > 0 && (
                        <div className="mb-6">
                            <h3 className="font-medium mb-2">📊 Gráfico Comparativo de Custos</h3>
                            <img
                                src={resolveUrl(
                                    artefatos.graficos.find((g) =>
                                        g.includes(`grafico_simulacao_${artefatos.data}`)
                                    ) || artefatos.graficos[0] // fallback: mostra o primeiro disponível
                                )}
                                alt={`Gráfico comparativo de custos ${artefatos.data}`}
                                className="w-full border rounded bg-white"
                            />

                        </div>
                    )}

                    {/* Cenários */}
                    {Object.entries(artefatos.cenarios).map(([k, itens]) => (
                        <div key={k} className="mb-6 bg-white rounded-lg shadow p-4">
                            <h3 className="text-lg font-bold mb-2">
                                Cenário k={k}{" "}
                                {itens.otimo ? (
                                    <span className="text-emerald-600">🌟 (Ótimo)</span>
                                ) : null}
                            </h3>

                            {/* Mapa inline se ótimo */}
                            {itens.otimo && (itens.mapas?.length ?? 0) > 0 && (
                                <div className="mb-4">
                                    <h4 className="font-medium mb-2 flex items-center gap-2">
                                        <Map className="w-4 h-4" /> Mapa do Cenário Ótimo
                                    </h4>
                                    <iframe
                                        src={resolveUrl(
                                            itens.mapas?.find((m) => m.endsWith(".html")) || ""
                                        )}
                                        title={`Mapa k=${k}`}
                                        className="w-full h-[600px] border rounded-lg"
                                    />
                                </div>
                            )}

                            {itens.mapas?.length ? (
                                <div className="mb-3">
                                    <p className="font-medium">Links dos Mapas:</p>
                                    <ul className="list-disc list-inside text-sm">
                                        {itens.mapas.map((m) => (
                                            <li key={m}>
                                                <a
                                                    href={resolveUrl(m)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-blue-600 underline"
                                                >
                                                    {m.split("/").pop()}
                                                </a>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            ) : null}

                            {/* Tabelas Last-Mile */}
                            {itens.tabelas_lastmile && (
                                <div className="mb-3">
                                    <p className="font-medium">Tabelas Last-Mile:</p>
                                    <div className="flex flex-wrap gap-3">
                                        {itens.tabelas_lastmile.map((t) => (
                                            <img
                                                key={t}
                                                src={resolveUrl(t)}
                                                alt="Tabela Last-Mile"
                                                className="h-40 border rounded"
                                            />
                                        ))}
                                    </div>
                                </div>
                            )}

                            {/* Tabelas Transferências */}
                            {itens.tabelas_transferencias && (
                                <div className="mb-3">
                                    <p className="font-medium">Tabelas Transferências:</p>
                                    <div className="flex flex-wrap gap-3">
                                        {itens.tabelas_transferencias.map((t) => (
                                            <img
                                                key={t}
                                                src={resolveUrl(t)}
                                                alt="Tabela Transferência"
                                                className="h-40 border rounded"
                                            />
                                        ))}
                                    </div>
                                </div>
                            )}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
