// hub_router_1.0.1/frontend/src/pages/Simulation/SimulationPage.tsx

import { useState } from "react";
import {
    runSimulation,
    visualizeSimulation,
    type VisualizeSimulationResponse,
} from "@/services/simulationApi";

function todayISO() {
    const d = new Date();
    return d.toISOString().slice(0, 10);
}

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
            alert("Informe uma data inicial válida (a final é opcional).");
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
        } catch (e: any) {
            setMsg(e?.response?.data?.detail || "Erro ao executar simulação.");
        } finally {
            setLoading(false);
        }
    };

    const gerarRelatorios = async () => {
        if (!dataInicial) {
            alert("Selecione a data inicial.");
            return;
        }
        try {
            const data = await visualizeSimulation(dataInicial);
            console.log("artefatos carregados:", data);
            setArtefatos(data);
            setMsg("✅ Artefatos carregados.");
        } catch (e: any) {
            setMsg(e?.response?.data?.detail || "Erro ao carregar artefatos.");
        }
    };

    return (
        <div className="max-w-6xl mx-auto p-6">
            <h1 className="text-2xl font-bold mb-6">Simulação</h1>

            {/* Formulário */}
            <div className="grid md:grid-cols-4 gap-4 mb-6">
                <div>
                    <label className="block text-sm">Data inicial</label>
                    <input
                        type="date"
                        value={dataInicial}
                        max={todayISO()}
                        onChange={(e) => setDataInicial(e.target.value)}
                        className="input"
                    />
                </div>
                <div>
                    <label className="block text-sm">Data final (opcional)</label>
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
                        className="btn w-full"
                    >
                        {loading ? "Processando..." : "Processar Simulação"}
                    </button>
                </div>
                <div className="flex items-end">
                    <button
                        disabled={!dataInicial}
                        onClick={gerarRelatorios}
                        className="btn-secondary w-full"
                    >
                        Gerar Relatórios & Gráficos
                    </button>
                </div>
            </div>

            {/* Mensagens */}
            {msg && <div className="mb-4">{msg}</div>}

            {/* Artefatos */}
            {artefatos && (
                <div className="border rounded p-4 bg-gray-50">
                    <h2 className="font-semibold mb-4">Artefatos {artefatos.data}</h2>

                    {/* PDF Consolidado */}
                    {artefatos.relatorio_pdf && (
                        <div className="mb-4">
                            <a
                                href={artefatos.relatorio_pdf}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="btn"
                            >
                                📄 Baixar Relatório PDF
                            </a>
                        </div>
                    )}

                    {/* Gráfico comparativo único */}
                    {artefatos.graficos && artefatos.graficos.length > 0 && (
                        <div className="mb-6">
                            <h3 className="font-medium mb-2">📊 Gráfico Comparativo de Custos</h3>
                            {artefatos.graficos
                                .filter((g) => g.includes("grafico_simulacao_"))
                                .slice(0, 1)
                                .map((g) => (
                                    <img
                                        key={g}
                                        src={g}
                                        alt="Gráfico comparativo de custos"
                                        className="w-full border rounded bg-white"
                                    />
                                ))}
                        </div>
                    )}

                    {/* Loop por cenários */}
                    {Object.entries(artefatos.cenarios).map(([k, itens]) => (
                        <div key={k} className="mb-6">
                            <h3 className="text-lg font-bold mb-2">
                                Cenário k={k} {itens.otimo ? "🌟 (Ótimo)" : ""}
                            </h3>

                            {/* Se for o cenário ótimo → renderiza mapa inline */}
                            {itens.otimo && itens.mapas && itens.mapas.length > 0 && (
                                <div className="mb-6">
                                    <h4 className="font-medium mb-2">Mapa do Cenário Ótimo</h4>
                                    <iframe
                                        src={itens.mapas.find((m) => m.endsWith(".html"))}
                                        title={`Mapa k=${k}`}
                                        className="w-full h-[600px] border rounded"
                                    />
                                </div>
                            )}

                            {/* Para todos os cenários → apenas links */}
                            {itens.mapas && (
                                <div className="mb-3">
                                    <p className="font-medium">Links dos Mapas:</p>
                                    <ul className="list-disc list-inside">
                                        {itens.mapas.map((m) => (
                                            <li key={m}>
                                                <a
                                                    href={m}
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
                            )}

                            {/* Tabelas Last-Mile */}
                            {itens.tabelas_lastmile && (
                                <div className="mb-3">
                                    <p className="font-medium">Tabelas Last-Mile:</p>
                                    <div className="flex flex-wrap gap-3">
                                        {itens.tabelas_lastmile.map((t) => (
                                            <img
                                                key={t}
                                                src={t}
                                                alt="Tabela Last-Mile"
                                                className="h-40 border"
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
                                                src={t}
                                                alt="Tabela Transferência"
                                                className="h-40 border"
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
