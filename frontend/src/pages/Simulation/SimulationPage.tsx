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
    ChevronDown,
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

// 🔧 Accordion wrapper
function Accordion({
    title,
    children,
}: {
    title: string;
    children: React.ReactNode;
}) {
    const [open, setOpen] = useState(false);
    return (
        <div className="border rounded-lg mb-4">
            <button
                onClick={() => setOpen(!open)}
                className="w-full flex justify-between items-center p-3 bg-gray-100 rounded-lg"
            >
                <span className="font-semibold">{title}</span>
                <ChevronDown
                    className={`w-4 h-4 transition-transform ${open ? "rotate-180" : ""
                        }`}
                />
            </button>
            {open && <div className="p-4 grid md:grid-cols-3 gap-4">{children}</div>}
        </div>
    );
}

export default function SimulationPage() {
    const [dataInicial, setDataInicial] = useState("");
    const [dataFinal, setDataFinal] = useState("");
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);
    const [artefatos, setArtefatos] = useState<VisualizeSimulationResponse | null>(
        null
    );

    // 🔧 Estado centralizado com defaults alinhados ao main_simulation.py
    const [params, setParams] = useState({
        // Clusterização
        k_min: 2,
        k_max: 50,
        k_inicial_transferencia: 1,
        min_entregas_cluster: 25,
        fundir_clusters_pequenos: false,

        // Cluster hub
        desativar_cluster_hub: false,
        raio_hub_km: 80.0,

        // Tempos
        parada_leve: 10,
        parada_pesada: 20,
        tempo_volume: 0.4,

        // Operações
        velocidade: 60.0,
        limite_peso: 50.0,

        // Restrições
        restricao_veiculo_leve_municipio: false,
        peso_leve_max: 50.0,

        // Transferências
        tempo_max_transferencia: 1200,
        peso_max_transferencia: 15000.0,

        // Last-mile
        entregas_por_subcluster: 25,
        tempo_max_roteirizacao: 1200,
        tempo_max_k1: 2400,

        // Rotas excedentes
        permitir_rotas_excedentes: false,
    });

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
                ...params, // 🔑 envia todos os parâmetros configurados
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

            {/* Formulário principal */}
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

            {/* Accordions com parâmetros */}
            <div className="bg-white rounded-2xl shadow p-4 mb-6">
                <h2 className="font-semibold mb-4">⚙️ Parâmetros da Simulação</h2>

                <Accordion title="Clusterização">
                    <label>
                        k_min
                        <input
                            type="number"
                            value={params.k_min}
                            onChange={(e) => setParams({ ...params, k_min: +e.target.value })}
                            className="input"
                        />
                    </label>
                    <label>
                        k_max
                        <input
                            type="number"
                            value={params.k_max}
                            onChange={(e) => setParams({ ...params, k_max: +e.target.value })}
                            className="input"
                        />
                    </label>
                    <label>
                        k_inicial_transferencia
                        <input
                            type="number"
                            value={params.k_inicial_transferencia}
                            onChange={(e) =>
                                setParams({ ...params, k_inicial_transferencia: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Min. entregas cluster
                        <input
                            type="number"
                            value={params.min_entregas_cluster}
                            onChange={(e) =>
                                setParams({ ...params, min_entregas_cluster: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label className="flex gap-2 items-center">
                        <input
                            type="checkbox"
                            checked={params.fundir_clusters_pequenos}
                            onChange={(e) =>
                                setParams({ ...params, fundir_clusters_pequenos: e.target.checked })
                            }
                        />
                        Fundir clusters pequenos
                    </label>
                </Accordion>

                <Accordion title="Tempos Operacionais">
                    <label>
                        Parada leve (min)
                        <input
                            type="number"
                            value={params.parada_leve}
                            onChange={(e) =>
                                setParams({ ...params, parada_leve: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Parada pesada (min)
                        <input
                            type="number"
                            value={params.parada_pesada}
                            onChange={(e) =>
                                setParams({ ...params, parada_pesada: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Tempo por volume (min)
                        <input
                            type="number"
                            step="0.1"
                            value={params.tempo_volume}
                            onChange={(e) =>
                                setParams({ ...params, tempo_volume: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                </Accordion>

                <Accordion title="Transferências">
                    <label>
                        Tempo máx. transferência (min)
                        <input
                            type="number"
                            value={params.tempo_max_transferencia}
                            onChange={(e) =>
                                setParams({ ...params, tempo_max_transferencia: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Peso máx. transferência (kg)
                        <input
                            type="number"
                            value={params.peso_max_transferencia}
                            onChange={(e) =>
                                setParams({ ...params, peso_max_transferencia: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                </Accordion>

                <Accordion title="Last-mile">
                    <label>
                        Entregas por subcluster
                        <input
                            type="number"
                            value={params.entregas_por_subcluster}
                            onChange={(e) =>
                                setParams({ ...params, entregas_por_subcluster: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Tempo máx. roteirização (min)
                        <input
                            type="number"
                            value={params.tempo_max_roteirizacao}
                            onChange={(e) =>
                                setParams({ ...params, tempo_max_roteirizacao: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Tempo máx. k=1 (min)
                        <input
                            type="number"
                            value={params.tempo_max_k1}
                            onChange={(e) =>
                                setParams({ ...params, tempo_max_k1: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                </Accordion>

                <Accordion title="Restrições e Operações">
                    <label>
                        Velocidade média (km/h)
                        <input
                            type="number"
                            value={params.velocidade}
                            onChange={(e) =>
                                setParams({ ...params, velocidade: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Limite peso parada pesada (kg)
                        <input
                            type="number"
                            value={params.limite_peso}
                            onChange={(e) =>
                                setParams({ ...params, limite_peso: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label>
                        Peso leve máx. (kg)
                        <input
                            type="number"
                            value={params.peso_leve_max}
                            onChange={(e) =>
                                setParams({ ...params, peso_leve_max: +e.target.value })
                            }
                            className="input"
                        />
                    </label>
                    <label className="flex gap-2 items-center">
                        <input
                            type="checkbox"
                            checked={params.restricao_veiculo_leve_municipio}
                            onChange={(e) =>
                                setParams({
                                    ...params,
                                    restricao_veiculo_leve_municipio: e.target.checked,
                                })
                            }
                        />
                        Restrição veículo leve município
                    </label>
                    <label className="flex gap-2 items-center">
                        <input
                            type="checkbox"
                            checked={params.permitir_rotas_excedentes}
                            onChange={(e) =>
                                setParams({
                                    ...params,
                                    permitir_rotas_excedentes: e.target.checked,
                                })
                            }
                        />
                        Permitir rotas excedentes
                    </label>
                </Accordion>
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
                    {/* PDF */}
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
                    {/* Gráfico */}
                    {artefatos.graficos && artefatos.graficos.length > 0 && (
                        <div className="mb-6">
                            <h3 className="font-medium mb-2">📊 Gráfico Comparativo de Custos</h3>
                            <img
                                src={resolveUrl(
                                    artefatos.graficos.find((g) =>
                                        g.includes(`grafico_simulacao_${artefatos.data}`)
                                    ) || artefatos.graficos[0]
                                )}
                                alt={`Gráfico comparativo de custos ${artefatos.data}`}
                                className="w-full border rounded bg-white"
                            />
                        </div>
                    )}
                    {/* Cenários */}
                    {Object.entries(artefatos.cenarios).map(([k, itens]) => (
                        <div key={k} className="mb-6 bg-white rounded-lg shadow p-4">
                            <h3 className="text-lg font-bold mb-4">
                                Cenário k={k}{" "}
                                {itens.otimo && (
                                    <span className="text-emerald-600">🌟 (Ótimo)</span>
                                )}
                            </h3>

                            {/* Links para mapas */}
                            {itens.mapas && itens.mapas.length > 0 && (
                                <div className="mb-3">
                                    <h4 className="font-medium mb-1 flex items-center gap-2">
                                        <Map className="w-4 h-4" /> Mapas
                                    </h4>
                                    <ul className="list-disc ml-6">
                                        {itens.mapas.map((m, idx) => (
                                            <li key={idx}>
                                                <a
                                                    href={resolveUrl(m)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-emerald-600 hover:underline"
                                                >
                                                    Abrir mapa {idx + 1}
                                                </a>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}

                            {/* Links para tabelas */}
                            {itens.tabelas_lastmile && itens.tabelas_lastmile.length > 0 && (
                                <div className="mb-3">
                                    <h4 className="font-medium mb-1">Tabelas Last-mile</h4>
                                    <ul className="list-disc ml-6">
                                        {itens.tabelas_lastmile.map((f, idx) => (
                                            <li key={idx}>
                                                <a
                                                    href={resolveUrl(f)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-emerald-600 hover:underline"
                                                >
                                                    Abrir tabela {idx + 1}
                                                </a>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}

                            {itens.tabelas_transferencias && itens.tabelas_transferencias.length > 0 && (
                                <div className="mb-3">
                                    <h4 className="font-medium mb-1">Tabelas Transferências</h4>
                                    <ul className="list-disc ml-6">
                                        {itens.tabelas_transferencias.map((f, idx) => (
                                            <li key={idx}>
                                                <a
                                                    href={resolveUrl(f)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-emerald-600 hover:underline"
                                                >
                                                    Abrir tabela {idx + 1}
                                                </a>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}

                            {itens.tabelas_resumo && itens.tabelas_resumo.length > 0 && (
                                <div className="mb-3">
                                    <h4 className="font-medium mb-1">CSVs Resumo</h4>
                                    <ul className="list-disc ml-6">
                                        {itens.tabelas_resumo.map((f, idx) => (
                                            <li key={idx}>
                                                <a
                                                    href={resolveUrl(f)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-emerald-600 hover:underline"
                                                >
                                                    Baixar resumo {idx + 1}
                                                </a>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}

                            {itens.tabelas_detalhes && itens.tabelas_detalhes.length > 0 && (
                                <div className="mb-3">
                                    <h4 className="font-medium mb-1">CSVs Detalhes</h4>
                                    <ul className="list-disc ml-6">
                                        {itens.tabelas_detalhes.map((f, idx) => (
                                            <li key={idx}>
                                                <a
                                                    href={resolveUrl(f)}
                                                    target="_blank"
                                                    rel="noopener noreferrer"
                                                    className="text-emerald-600 hover:underline"
                                                >
                                                    Baixar detalhes {idx + 1}
                                                </a>
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}

                            {/* Embed só no cenário ótimo */}
                            {itens.otimo && itens.mapas?.some((m) => m.endsWith(".html")) && (
                                <div className="mt-4">
                                    <h4 className="font-medium mb-2">📍 Mapa do Cenário Ótimo</h4>
                                    <iframe
                                        src={resolveUrl(itens.mapas.find((m) => m.endsWith(".html")) || "")}
                                        title={`Mapa k=${k}`}
                                        className="w-full h-[600px] border rounded-lg"
                                    />
                                </div>
                            )}
                        </div>
                    ))}

                </div>
            )}
        </div>
    );
}
