import { useMemo, useState } from "react";
import {
    runSimulation,
    visualizeSimulation,
    getDistribuicaoK,
    getFrequenciaCidades,
    getKFixo,
    getFrotaKFixo,
    type VisualizeSimulationResponse,
    type DistribuicaoKResponse,
    type FrequenciaCidadesResponse,
    type KFixoResponse,
    type FrotaKFixoResponse,
} from "@/services/simulationApi";
import toast from "react-hot-toast";
import {
    Play,
    BarChart3,
    FileText,
    Map,
    Loader2,
    ChevronDown,
    BarChart2,
    Building2,
    Truck,
} from "lucide-react";

// Recharts
import {
    BarChart as RBarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    CartesianGrid,
    ResponsiveContainer,
    Label,
} from "recharts";

function todayISO() {
    const d = new Date();
    return d.toISOString().slice(0, 10);
}

// helper para montar URL absoluta
const resolveUrl = (path: string) => {
    if (!path) return "";
    if (path.startsWith("http")) return path;
    return `${import.meta.env.VITE_API_URL}${path.startsWith("/") ? path : `/${path}`
        }`;
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

type TabKey = "simulacao" | "cenarioVencedor" | "centroVencedor" | "custos" | "frota";

export default function SimulationPage() {
    const [activeTab, setActiveTab] = useState<TabKey>("simulacao");

    const [dataInicial, setDataInicial] = useState("");
    const [dataFinal, setDataFinal] = useState("");
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);
    const [artefatos, setArtefatos] =
        useState<VisualizeSimulationResponse | null>(null);

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

    // =========================
    // Ações: Processar simulação
    // =========================
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
                ...params,
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

    // =========================
    // Ações: Visualizar artefatos (por dataInicial)
    // =========================
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

    // =========================
    // ABA: Distribuição de k
    // =========================
    const [distKData, setDistKData] =
        useState<DistribuicaoKResponse["dados"]>([]);
    const [distKGraficoUrl, setDistKGraficoUrl] = useState<string | null>(null);
    const [loadingDistK, setLoadingDistK] = useState(false);

    const periodoDistribuicaoDefault = useMemo(() => {
        // padrão: últimos 90 dias (limitado a 12 meses no backend)
        const end =
            dataFinal && dataFinal >= (dataInicial || "")
                ? dataFinal
                : dataInicial || todayISO();
        const dEnd = new Date(end || todayISO());
        const dIni = new Date(dEnd);
        dIni.setDate(dEnd.getDate() - 89); // ~90 dias
        const toISO = (d: Date) => d.toISOString().slice(0, 10);
        return {
            data_inicial: dataInicial || toISO(dIni),
            data_final: end || toISO(dEnd),
        };
    }, [dataInicial, dataFinal]);

    const carregarDistribuicaoK = async () => {
        const di = periodoDistribuicaoDefault.data_inicial;
        const df = periodoDistribuicaoDefault.data_final;

        if (!di || !df || df < di) {
            toast.error(
                "Informe um intervalo de datas válido para a distribuição de k."
            );
            return;
        }

        setLoadingDistK(true);
        try {
            const result = await getDistribuicaoK({
                data_inicial: di,
                data_final: df,
            });
            setDistKData(result.dados || []);
            setDistKGraficoUrl(result.grafico ? resolveUrl(result.grafico) : null);
            toast.success("Distribuição de k carregada!");
        } catch (err: any) {
            toast.error(
                err?.response?.data?.detail || "Erro ao carregar distribuição de k."
            );
        } finally {
            setLoadingDistK(false);
        }
    };

    // =========================
    // ABA: Frequência de Cidades
    // =========================
    const [freqCidadesData, setFreqCidadesData] =
        useState<FrequenciaCidadesResponse["dados"]>([]);
    const [freqCidadesGraficoUrl, setFreqCidadesGraficoUrl] = useState<
        string | null
    >(null);
    const [loadingFreqCidades, setLoadingFreqCidades] = useState(false);

    const periodoCidadesDefault = useMemo(() => {
        const end =
            dataFinal && dataFinal >= (dataInicial || "")
                ? dataFinal
                : dataInicial || todayISO();
        const dEnd = new Date(end || todayISO());
        const dIni = new Date(dEnd);
        dIni.setDate(dEnd.getDate() - 89); // ~90 dias
        const toISO = (d: Date) => d.toISOString().slice(0, 10);
        return {
            data_inicial: dataInicial || toISO(dIni),
            data_final: end || toISO(dEnd),
        };
    }, [dataInicial, dataFinal]);

    const carregarFrequenciaCidades = async () => {
        const di = periodoCidadesDefault.data_inicial;
        const df = periodoCidadesDefault.data_final;

        if (!di || !df || df < di) {
            toast.error(
                "Informe um intervalo de datas válido para a frequência de cidades."
            );
            return;
        }

        setLoadingFreqCidades(true);
        try {
            const result = await getFrequenciaCidades({
                data_inicial: di,
                data_final: df,
            });
            setFreqCidadesData(result.dados || []);
            setFreqCidadesGraficoUrl(
                result.grafico ? resolveUrl(result.grafico) : null
            );
            toast.success("Frequência de cidades carregada!");
        } catch (err: any) {
            toast.error(
                err?.response?.data?.detail ||
                "Erro ao carregar frequência de cidades."
            );
        } finally {
            setLoadingFreqCidades(false);
        }
    };

    // =========================
    // ABA: k Fixo (custos consolidados)
    // =========================
    const [kFixoUsarMedia, setKFixoUsarMedia] = useState(false);
    const [kFixoData, setKFixoData] = useState<KFixoResponse["dados"]>([]);
    const [kFixoGraficoUrl, setKFixoGraficoUrl] = useState<string | null>(null);
    const [loadingKFixo, setLoadingKFixo] = useState(false);

    const periodoKFixoDefault = useMemo(() => {
        // mesmo padrão (~90 dias) se o usuário não preencher
        const end =
            dataFinal && dataFinal >= (dataInicial || "")
                ? dataFinal
                : dataInicial || todayISO();
        const dEnd = new Date(end || todayISO());
        const dIni = new Date(dEnd);
        dIni.setDate(dEnd.getDate() - 89);
        const toISO = (d: Date) => d.toISOString().slice(0, 10);
        return {
            data_inicial: dataInicial || toISO(dIni),
            data_final: end || toISO(dEnd),
        };
    }, [dataInicial, dataFinal]);

    const carregarKFixo = async () => {
        const di = periodoKFixoDefault.data_inicial;
        const df = periodoKFixoDefault.data_final;

        if (!di || !df || df < di) {
            toast.error("Informe um intervalo de datas válido para k fixo.");
            return;
        }

        setLoadingKFixo(true);
        try {
            const result = await getKFixo({
                data_inicial: di,
                data_final: df,
                usar_media: kFixoUsarMedia,
            });
            setKFixoData(result.dados || []);
            setKFixoGraficoUrl(result.grafico ? resolveUrl(result.grafico) : null);
            toast.success("Custos por k carregados!");
        } catch (err: any) {
            toast.error(
                err?.response?.data?.detail || "Erro ao carregar custos por k."
            );
        } finally {
            setLoadingKFixo(false);
        }
    };

    // Dados para gráfico de k fixo (escolhe série conforme modo)
    const kFixoChartData = useMemo(() => {
        if (!kFixoData?.length) return [];
        return kFixoData
            .slice()
            .sort((a, b) => a.k_clusters - b.k_clusters)
            .map((d) => ({
                k: d.k_clusters,
                valor: kFixoUsarMedia ? d.media_custo_total : d.soma_custo_total,
            }));
    }, [kFixoData, kFixoUsarMedia]);

    // =========================
    // ABA: Frota p/ k Fixo
    // =========================
    const [frotaKsInput, setFrotaKsInput] = useState<string>("8,9,10");
    const [frotaData, setFrotaData] = useState<FrotaKFixoResponse["dados"]>([]);
    const [loadingFrota, setLoadingFrota] = useState(false);

    const periodoFrotaDefault = useMemo(() => {
        const end =
            dataFinal && dataFinal >= (dataInicial || "")
                ? dataFinal
                : dataInicial || todayISO();
        const dEnd = new Date(end || todayISO());
        const dIni = new Date(dEnd);
        dIni.setDate(dEnd.getDate() - 89);
        const toISO = (d: Date) => d.toISOString().slice(0, 10);
        return {
            data_inicial: dataInicial || toISO(dIni),
            data_final: end || toISO(dEnd),
        };
    }, [dataInicial, dataFinal]);

    const carregarFrota = async () => {
        const di = periodoFrotaDefault.data_inicial;
        const df = periodoFrotaDefault.data_final;

        // força array de inteiros
        const kList = frotaKsInput
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean)
            .map((s) => Number(s))
            .filter((n) => Number.isFinite(n));

        if (!kList.length) {
            toast.error("Informe ao menos um valor de k (ex.: 8,9,10).");
            return;
        }
        if (!di || !df || df < di) {
            toast.error("Informe um intervalo de datas válido.");
            return;
        }

        console.log("🚚 Enviando frota_k_fixo:", { di, df, kList }); // debug

        setLoadingFrota(true);
        try {
            const result = await getFrotaKFixo({
                data_inicial: di,
                data_final: df,
                k: [...kList], // ✅ garante que vai no formato ?k=8&k=9&k=10
            });
            setFrotaData(result.dados || []);
            toast.success("Frota sugerida carregada!");
        } catch (err: any) {
            toast.error(
                err?.response?.data?.detail || "Erro ao carregar frota sugerida."
            );
        } finally {
            setLoadingFrota(false);
        }
    };


    // Agrega total de frota por k para um gráfico simples
    const frotaTotalPorK = useMemo(() => {
        const acc: Record<number, number> = {};
        for (const r of frotaData) {
            acc[r.k_clusters] = (acc[r.k_clusters] || 0) + r.frota_sugerida;
        }
        return Object.entries(acc)
            .map(([k, total]) => ({ k: Number(k), total }))
            .sort((a, b) => a.k - b.k);
    }, [frotaData]);

    return (
        <div className="max-w-6xl mx-auto p-6">
            <h1 className="text-2xl font-bold mb-6 flex items-center gap-2">
                <BarChart3 className="w-6 h-6 text-emerald-600" />
                Simulação
            </h1>

            {/* Tabs */}
            <div className="mb-6">
                <div className="inline-flex rounded-xl border bg-white shadow overflow-hidden">
                    <button
                        className={`px-4 py-2 flex items-center gap-2 ${activeTab === "simulacao" ? "bg-emerald-600 text-white" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("simulacao")}
                    >
                        <Play className="w-4 h-4" /> Simulação
                    </button>
                    <button
                        className={`px-4 py-2 flex items-center gap-2 ${activeTab === "cenarioVencedor" ? "bg-emerald-600 text-white" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("cenarioVencedor")}
                    >
                        <BarChart2 className="w-4 h-4" /> Cenário Vencedor
                    </button>
                    <button
                        className={`px-4 py-2 flex items-center gap-2 ${activeTab === "centroVencedor" ? "bg-emerald-600 text-white" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("centroVencedor")}
                    >
                        <Building2 className="w-4 h-4" /> Centro Vencedor
                    </button>
                    <button
                        className={`px-4 py-2 flex items-center gap-2 ${activeTab === "custos" ? "bg-emerald-600 text-white" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("custos")}
                    >
                        <BarChart3 className="w-4 h-4" /> Custos
                    </button>
                    <button
                        className={`px-4 py-2 flex items-center gap-2 ${activeTab === "frota" ? "bg-emerald-600 text-white" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("frota")}
                    >
                        <Map className="w-4 h-4" /> Frota
                    </button>
                </div>
            </div>

            {/* ===================== ABA: Simulação ===================== */}
            {activeTab === "simulacao" && (
                <>
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
                            <label className="block text-sm text-gray-700">
                                Data final (opcional)
                            </label>
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
                                        setParams({
                                            ...params,
                                            tempo_max_transferencia: +e.target.value,
                                        })
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
                                        setParams({
                                            ...params,
                                            peso_max_transferencia: +e.target.value,
                                        })
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
                                        setParams({
                                            ...params,
                                            entregas_por_subcluster: +e.target.value,
                                        })
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
                                        setParams({
                                            ...params,
                                            tempo_max_roteirizacao: +e.target.value,
                                        })
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
                            <h2 className="font-semibold mb-4">
                                Artefatos {artefatos.data}
                            </h2>

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
                                    <h3 className="font-medium mb-2">
                                        📊 Gráfico Comparativo de Custos
                                    </h3>
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
                            {Object.entries(artefatos.cenarios).map(([k, itens]: any) => (
                                <div key={k} className="mb-6 bg-white rounded-lg shadow p-4">
                                    <h3 className="text-lg font-bold mb-4">
                                        Cenário k={k}{" "}
                                        {itens.otimo && (
                                            <span className="text-emerald-600">🌟 (Ótimo)</span>
                                        )}
                                    </h3>

                                    {/* Mapas */}
                                    {itens.mapas && itens.mapas.length > 0 && (
                                        <div className="mb-3">
                                            <h4 className="font-medium mb-1 flex items-center gap-2">
                                                <Map className="w-4 h-4" /> Mapas
                                            </h4>
                                            <ul className="list-disc ml-6">
                                                {itens.mapas.map((m: string, idx: number) => (
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

                                    {/* Tabelas Last-mile */}
                                    {itens.tabelas_lastmile && itens.tabelas_lastmile.length > 0 && (
                                        <div className="mb-3">
                                            <h4 className="font-medium mb-1">Tabelas Last-mile</h4>
                                            <ul className="list-disc ml-6">
                                                {itens.tabelas_lastmile.map(
                                                    (f: string, idx: number) => (
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
                                                    )
                                                )}
                                            </ul>
                                        </div>
                                    )}

                                    {/* Tabelas Transferências */}
                                    {itens.tabelas_transferencias &&
                                        itens.tabelas_transferencias.length > 0 && (
                                            <div className="mb-3">
                                                <h4 className="font-medium mb-1">
                                                    Tabelas Transferências
                                                </h4>
                                                <ul className="list-disc ml-6">
                                                    {itens.tabelas_transferencias.map(
                                                        (f: string, idx: number) => (
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
                                                        )
                                                    )}
                                                </ul>
                                            </div>
                                        )}

                                    {/* CSVs Resumo */}
                                    {itens.tabelas_resumo && itens.tabelas_resumo.length > 0 && (
                                        <div className="mb-3">
                                            <h4 className="font-medium mb-1">CSVs Resumo</h4>
                                            <ul className="list-disc ml-6">
                                                {itens.tabelas_resumo.map(
                                                    (f: string, idx: number) => (
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
                                                    )
                                                )}
                                            </ul>
                                        </div>
                                    )}

                                    {/* CSVs Detalhes */}
                                    {itens.tabelas_detalhes &&
                                        itens.tabelas_detalhes.length > 0 && (
                                            <div className="mb-3">
                                                <h4 className="font-medium mb-1">CSVs Detalhes</h4>
                                                <ul className="list-disc ml-6">
                                                    {itens.tabelas_detalhes.map(
                                                        (f: string, idx: number) => (
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
                                                        )
                                                    )}
                                                </ul>
                                            </div>
                                        )}

                                    {/* Embed do cenário ótimo */}
                                    {itens.otimo &&
                                        itens.mapas?.some((m: string) => m.endsWith(".html")) && (
                                            <div className="mt-4">
                                                <h4 className="font-medium mb-2">
                                                    📍 Mapa do Cenário Ótimo
                                                </h4>
                                                <iframe
                                                    src={resolveUrl(
                                                        itens.mapas.find((m: string) => m.endsWith(".html")) ||
                                                        ""
                                                    )}
                                                    title={`Mapa k=${k}`}
                                                    className="w-full h-[600px] border rounded-lg"
                                                />
                                            </div>
                                        )}
                                </div>
                            ))}
                        </div>
                    )}
                </>
            )}

            {/* ===================== ABA: Distribuição de k ===================== */}
            {activeTab === "cenarioVencedor" && (
                <div className="bg-white rounded-2xl shadow p-4">
                    <h2 className="font-semibold mb-4 flex items-center gap-2">
                        <BarChart2 className="w-5 h-5 text-emerald-600" />
                        Distribuição de k (ponto ótimo) em um período
                    </h2>

                    {/* Filtros */}
                    <div className="grid md:grid-cols-3 gap-4 mb-4">
                        <div>
                            <label className="block text-sm text-gray-700">Data inicial</label>
                            <input
                                type="date"
                                value={periodoDistribuicaoDefault.data_inicial}
                                max={todayISO()}
                                onChange={(e) => setDataInicial(e.target.value)}
                                className="input"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-700">Data final</label>
                            <input
                                type="date"
                                value={periodoDistribuicaoDefault.data_final}
                                max={todayISO()}
                                onChange={(e) => setDataFinal(e.target.value)}
                                className="input"
                            />
                        </div>
                        <div className="flex items-end">
                            <button
                                onClick={carregarDistribuicaoK}
                                disabled={loadingDistK}
                                className="btn w-full flex items-center gap-2"
                            >
                                {loadingDistK ? (
                                    <>
                                        <Loader2 className="w-4 h-4 animate-spin" /> Carregando...
                                    </>
                                ) : (
                                    "Gerar gráfico de distribuição"
                                )}
                            </button>
                        </div>
                    </div>

                    {/* Resultado */}
                    {distKData.length === 0 && !loadingDistK && (
                        <div className="text-sm text-gray-500">
                            Defina o período e clique em{" "}
                            <strong>Gerar gráfico de distribuição</strong>.
                        </div>
                    )}

                    {distKData.length > 0 && (
                        <>
                            <div className="w-full h-[420px] border rounded-lg p-2">
                                <ResponsiveContainer width="100%" height="100%">
                                    <RBarChart data={distKData}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis dataKey="k_clusters" tickMargin={8}>
                                            <Label
                                                value="Clusters (k)"
                                                offset={-5}
                                                position="insideBottom"
                                            />
                                        </XAxis>
                                        <YAxis
                                            label={{
                                                value: "Frequência",
                                                angle: -90,
                                                position: "insideLeft",
                                            }}
                                            allowDecimals={false}
                                            tickMargin={6}
                                        />
                                        <Tooltip />
                                        <Bar dataKey="qtd" />
                                    </RBarChart>
                                </ResponsiveContainer>
                            </div>

                            {distKGraficoUrl && (
                                <div className="mt-4 text-right">
                                    <a
                                        href={distKGraficoUrl}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-emerald-600 hover:underline"
                                    >
                                        📥 Baixar gráfico oficial (PNG)
                                    </a>
                                </div>
                            )}
                        </>
                    )}
                </div>
            )}

            {/* ===================== ABA: Frequência de Cidades ===================== */}
            {activeTab === "centroVencedor" && (
                <div className="bg-white rounded-2xl shadow p-4">
                    <h2 className="font-semibold mb-4 flex items-center gap-2">
                        <Building2 className="w-5 h-5 text-emerald-600" />
                        Frequência das cidades centro nos pontos ótimos
                    </h2>

                    {/* Filtros */}
                    <div className="grid md:grid-cols-3 gap-4 mb-4">
                        <div>
                            <label className="block text-sm text-gray-700">Data inicial</label>
                            <input
                                type="date"
                                value={periodoCidadesDefault.data_inicial}
                                max={todayISO()}
                                onChange={(e) => setDataInicial(e.target.value)}
                                className="input"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-700">Data final</label>
                            <input
                                type="date"
                                value={periodoCidadesDefault.data_final}
                                max={todayISO()}
                                onChange={(e) => setDataFinal(e.target.value)}
                                className="input"
                            />
                        </div>
                        <div className="flex items-end">
                            <button
                                onClick={carregarFrequenciaCidades}
                                disabled={loadingFreqCidades}
                                className="btn w-full flex items-center gap-2"
                            >
                                {loadingFreqCidades ? (
                                    <>
                                        <Loader2 className="w-4 h-4 animate-spin" /> Carregando...
                                    </>
                                ) : (
                                    "Gerar gráfico de cidades"
                                )}
                            </button>
                        </div>
                    </div>

                    {/* Resultado */}
                    {freqCidadesData.length === 0 && !loadingFreqCidades && (
                        <div className="text-sm text-gray-500">
                            Defina o período e clique em{" "}
                            <strong>Gerar gráfico de cidades</strong>.
                        </div>
                    )}

                    {freqCidadesData.length > 0 && (
                        <div className="w-full h-[420px] border rounded-lg p-2">
                            <ResponsiveContainer width="100%" height="100%">
                                <RBarChart data={freqCidadesData}>
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="cluster_cidade" tickMargin={8}>
                                        <Label
                                            value="Cidades Centro"
                                            offset={-5}
                                            position="insideBottom"
                                        />
                                    </XAxis>
                                    <YAxis
                                        label={{
                                            value: "Frequência",
                                            angle: -90,
                                            position: "insideLeft",
                                        }}
                                        allowDecimals={false}
                                        tickMargin={6}
                                    />
                                    <Tooltip />
                                    <Bar dataKey="qtd" />
                                </RBarChart>
                            </ResponsiveContainer>
                        </div>
                    )}

                    {freqCidadesGraficoUrl && (
                        <div className="mt-4 text-right">
                            <a
                                href={freqCidadesGraficoUrl}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="text-emerald-600 hover:underline"
                            >
                                📥 Baixar gráfico oficial (PNG)
                            </a>
                        </div>
                    )}
                </div>
            )}

            {/* ===================== ABA: k Fixo (custos) ===================== */}
            {activeTab === "custos" && (
                <div className="bg-white rounded-2xl shadow p-4">
                    <h2 className="font-semibold mb-4 flex items-center gap-2">
                        <BarChart3 className="w-5 h-5 text-emerald-600" />
                        Custos consolidados para k fixo
                    </h2>

                    <div className="grid md:grid-cols-4 gap-4 mb-4">
                        <div>
                            <label className="block text-sm text-gray-700">Data inicial</label>
                            <input
                                type="date"
                                value={periodoKFixoDefault.data_inicial}
                                max={todayISO()}
                                onChange={(e) => setDataInicial(e.target.value)}
                                className="input"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-700">Data final</label>
                            <input
                                type="date"
                                value={periodoKFixoDefault.data_final}
                                max={todayISO()}
                                onChange={(e) => setDataFinal(e.target.value)}
                                className="input"
                            />
                        </div>
                        <label className="flex items-center gap-2">
                            <input
                                type="checkbox"
                                checked={kFixoUsarMedia}
                                onChange={(e) => setKFixoUsarMedia(e.target.checked)}
                            />
                            Usar média (em vez de soma)
                        </label>
                        <div className="flex items-end">
                            <button
                                onClick={carregarKFixo}
                                disabled={loadingKFixo}
                                className="btn w-full flex items-center gap-2"
                            >
                                {loadingKFixo ? (
                                    <>
                                        <Loader2 className="w-4 h-4 animate-spin" /> Carregando...
                                    </>
                                ) : (
                                    "Gerar custos por k"
                                )}
                            </button>
                        </div>
                    </div>

                    {kFixoData.length === 0 && !loadingKFixo && (
                        <div className="text-sm text-gray-500">
                            Defina o período e clique em <strong>Gerar custos por k</strong>.
                        </div>
                    )}

                    {kFixoData.length > 0 && (
                        <>
                            <div className="w-full h-[420px] border rounded-lg p-2 mb-4">
                                <ResponsiveContainer width="100%" height="100%">
                                    <RBarChart data={kFixoChartData}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis dataKey="k" tickMargin={8}>
                                            <Label
                                                value="k"
                                                offset={-5}
                                                position="insideBottom"
                                            />
                                        </XAxis>
                                        <YAxis
                                            label={{
                                                value: kFixoUsarMedia ? "Custo médio" : "Custo acumulado",
                                                angle: -90,
                                                position: "insideLeft",
                                            }}
                                            tickMargin={6}
                                        />
                                        <Tooltip />
                                        <Bar dataKey="valor" />
                                    </RBarChart>
                                </ResponsiveContainer>
                            </div>

                            {kFixoGraficoUrl && (
                                <div className="mt-2 text-right">
                                    <a
                                        href={kFixoGraficoUrl}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-emerald-600 hover:underline"
                                    >
                                        📥 Baixar gráfico oficial (PNG)
                                    </a>
                                </div>
                            )}

                            {/* Tabela rápida */}
                            <div className="overflow-auto mt-4">
                                <table className="min-w-full text-sm">
                                    <thead>
                                        <tr className="text-left border-b">
                                            <th className="py-2 pr-4">k</th>
                                            <th className="py-2 pr-4">Dias válidos</th>
                                            <th className="py-2 pr-4">Cobertura</th>
                                            <th className="py-2 pr-4">Custo (soma)</th>
                                            <th className="py-2 pr-4">Custo (média)</th>
                                            <th className="py-2 pr-4">Regret %</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {kFixoData
                                            .slice()
                                            .sort((a, b) => a.k_clusters - b.k_clusters)
                                            .map((r) => (
                                                <tr key={r.k_clusters} className="border-b">
                                                    <td className="py-2 pr-4">{r.k_clusters}</td>
                                                    <td className="py-2 pr-4">
                                                        {r.dias_presentes}/{r.total_dias}
                                                    </td>
                                                    <td className="py-2 pr-4">
                                                        {(r.cobertura_pct * 100).toFixed(1)}%
                                                    </td>
                                                    <td className="py-2 pr-4">
                                                        {r.soma_custo_total?.toLocaleString()}
                                                    </td>
                                                    <td className="py-2 pr-4">
                                                        {r.media_custo_total?.toLocaleString()}
                                                    </td>
                                                    <td className="py-2 pr-4">
                                                        {(r.regret_relativo * 100).toFixed(2)}%
                                                    </td>
                                                </tr>
                                            ))}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    )}
                </div>
            )}

            {/* ===================== ABA: Frota p/ k Fixo ===================== */}
            {activeTab === "frota" && (
                <div className="bg-white rounded-2xl shadow p-4">
                    <h2 className="font-semibold mb-4 flex items-center gap-2">
                        <Truck className="w-5 h-5 text-emerald-600" />
                        Frota média sugerida por k fixo
                    </h2>

                    <div className="grid md:grid-cols-4 gap-4 mb-4">
                        <div>
                            <label className="block text-sm text-gray-700">Data inicial</label>
                            <input
                                type="date"
                                value={periodoFrotaDefault.data_inicial}
                                max={todayISO()}
                                onChange={(e) => setDataInicial(e.target.value)}
                                className="input"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-700">Data final</label>
                            <input
                                type="date"
                                value={periodoFrotaDefault.data_final}
                                max={todayISO()}
                                onChange={(e) => setDataFinal(e.target.value)}
                                className="input"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-700">
                                Valores de k (ex.: 8,9,10)
                            </label>
                            <input
                                type="text"
                                value={frotaKsInput}
                                onChange={(e) => setFrotaKsInput(e.target.value)}
                                className="input"
                                placeholder="8,9,10"
                            />
                        </div>
                        <div className="flex items-end">
                            <button
                                onClick={carregarFrota}
                                disabled={loadingFrota}
                                className="btn w-full flex items-center gap-2"
                            >
                                {loadingFrota ? (
                                    <>
                                        <Loader2 className="w-4 h-4 animate-spin" /> Carregando...
                                    </>
                                ) : (
                                    "Gerar frota"
                                )}
                            </button>
                        </div>
                    </div>

                    {frotaData.length === 0 && !loadingFrota && (
                        <div className="text-sm text-gray-500">
                            Informe o período e os valores de <strong>k</strong>, depois
                            clique em <strong>Gerar frota</strong>.
                        </div>
                    )}

                    {frotaData.length > 0 && (
                        <>
                            {/* Gráfico simples: total de frota por k */}
                            <div className="w-full h-[380px] border rounded-lg p-2 mb-4">
                                <ResponsiveContainer width="100%" height="100%">
                                    <RBarChart data={frotaTotalPorK}>
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis dataKey="k" tickMargin={8}>
                                            <Label value="k" offset={-5} position="insideBottom" />
                                        </XAxis>
                                        <YAxis
                                            label={{
                                                value: "Frota total sugerida",
                                                angle: -90,
                                                position: "insideLeft",
                                            }}
                                            allowDecimals={false}
                                            tickMargin={6}
                                        />
                                        <Tooltip />
                                        <Bar dataKey="total" />
                                    </RBarChart>
                                </ResponsiveContainer>
                            </div>

                            {/* Tabela detalhada por tipo de veículo */}
                            <div className="overflow-auto">
                                <table className="min-w-full text-sm">
                                    <thead>
                                        <tr className="text-left border-b">
                                            <th className="py-2 pr-4">k</th>
                                            <th className="py-2 pr-4">Tipo de veículo</th>
                                            <th className="py-2 pr-4">Frota sugerida</th>
                                            <th className="py-2 pr-4">Cobertura</th>
                                            <th className="py-2 pr-4">Modo</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {frotaData
                                            .slice()
                                            .sort(
                                                (a, b) =>
                                                    a.k_clusters - b.k_clusters ||
                                                    a.tipo_veiculo.localeCompare(b.tipo_veiculo)
                                            )
                                            .map((r, i) => (
                                                <tr key={i} className="border-b">
                                                    <td className="py-2 pr-4">{r.k_clusters}</td>
                                                    <td className="py-2 pr-4">{r.tipo_veiculo}</td>
                                                    <td className="py-2 pr-4">{r.frota_sugerida}</td>
                                                    <td className="py-2 pr-4">
                                                        {(r.cobertura_pct * 100).toFixed(1)}%
                                                    </td>
                                                    <td className="py-2 pr-4">{r.modo}</td>
                                                </tr>
                                            ))}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    )}
                </div>
            )}
        </div>
    );
}
