// hub_router_1.0.1/frontend/src/pages/Simulation/SimulationPage.tsx
import { useMemo, useState, useEffect } from "react";
import {
    runSimulation,
    visualizeSimulation,
    getDistribuicaoK,
    getFrequenciaCidades,
    getKFixo,
    getFrotaKFixo,
    listHubs,
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
} from "lucide-react";
import {
    BarChart as RBarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    CartesianGrid,
    ResponsiveContainer,
    Label,
    Cell,
} from "recharts";

import { getColorForString } from "@/utils/colors";

function todayISO() {
    const d = new Date();
    return d.toISOString().slice(0, 10);
}

const resolveUrl = (path: string) => {
    if (!path) return "";
    if (path.startsWith("http")) return path;
    return `${import.meta.env.VITE_API_URL}${path.startsWith("/") ? path : `/${path}`}`;
};

// 🔧 Accordion wrapper
function Accordion({ title, children }: { title: string; children: React.ReactNode }) {
    const [open, setOpen] = useState(false);
    return (
        <div className="border rounded-lg mb-4">
            <button
                onClick={() => setOpen(!open)}
                className="w-full flex justify-between items-center p-3 bg-gray-100 rounded-lg"
            >
                <span className="font-semibold">{title}</span>
                <ChevronDown className={`w-4 h-4 transition-transform ${open ? "rotate-180" : ""}`} />
            </button>
            {open && <div className="p-4 grid md:grid-cols-3 gap-4">{children}</div>}
        </div>
    );
}

type TabKey = "simulacao" | "cenarioVencedor" | "centroVencedor" | "custos" | "frota";

function FrotaChartTable({ data, chartId }: { data: any[]; chartId: string }) {
    const chartData = useMemo(() => {
        return data
            .slice()
            .sort((a, b) => b.frota_sugerida - a.frota_sugerida)
            .map((r) => ({
                tipo: r.tipo_veiculo,
                frota: r.frota_sugerida,
                k_clusters: r.k_clusters,
                cobertura_pct: r.cobertura_pct,
                modo: r.modo,
            }));
    }, [data]);

    return (
        <>
            <div className="mb-4 text-emerald-700 font-semibold">
                🚚 Total de veículos sugeridos: {data.reduce((acc, r) => acc + r.frota_sugerida, 0)}
            </div>

            <div id={chartId} className="w-full h-[420px] border rounded-lg p-2 mb-2 bg-white">
                <ResponsiveContainer width="100%" height="100%">
                    <RBarChart
                        data={chartData}
                        layout="vertical"
                        margin={{ top: 10, right: 30, left: 80, bottom: 10 }}
                    >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" allowDecimals={false} />
                        <YAxis type="category" dataKey="tipo" width={120} />
                        <Tooltip formatter={(v: number) => `${v} veículos`} />
                        <Bar dataKey="frota" radius={[0, 6, 6, 0]} barSize={20}>
                            {chartData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={getColorForString(entry.tipo)} />
                            ))}
                        </Bar>
                    </RBarChart>
                </ResponsiveContainer>
            </div>

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
                        {chartData.map((r, i) => (
                            <tr key={i} className="border-b">
                                <td className="py-2 pr-4">{r.k_clusters}</td>
                                <td className="py-2 pr-4">{r.tipo}</td>
                                <td className="py-2 pr-4">{r.frota}</td>
                                <td className="py-2 pr-4">{(r.cobertura_pct * 100).toFixed(1)}%</td>
                                <td className="py-2 pr-4">{r.modo}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </>
    );
}

export default function SimulationPage() {
    const [activeTab, setActiveTab] = useState<TabKey>("simulacao");

    const [dataInicial, setDataInicial] = useState("");
    const [dataFinal, setDataFinal] = useState("");
    const [dataVisualizar, setDataVisualizar] = useState("");
    const [loading, setLoading] = useState(false);
    const [msg, setMsg] = useState<string | null>(null);
    const [artefatos, setArtefatos] = useState<VisualizeSimulationResponse | null>(null);
    const [minCobertura, setMinCobertura] = useState(70);


    // 🔧 Estado centralizado
    const [params, setParams] = useState({
        k_min: 2,
        k_max: 50,
        k_inicial_transferencia: 1,
        min_entregas_cluster: 25,
        fundir_clusters_pequenos: false,
        desativar_cluster_hub: false,
        raio_hub_km: 80.0,
        parada_leve: 10,
        parada_pesada: 20,
        tempo_volume: 0.4,
        velocidade: 60.0,
        limite_peso: 50.0,
        restricao_veiculo_leve_municipio: false,
        peso_leve_max: 50.0,
        tempo_max_transferencia: 1200,
        peso_max_transferencia: 15000.0,
        entregas_por_subcluster: 25,
        tempo_max_roteirizacao: 1200,
        tempo_max_k1: 2400,
        permitir_rotas_excedentes: false,
        modo_forcar: false,
    });

    // =========================
    // Hubs
    // =========================
    const [hubs, setHubs] = useState<{ hub_id: number; nome: string; cidade: string }[]>([]);
    const [hubId, setHubId] = useState<number | null>(null);

    useEffect(() => {
        const carregarHubs = async () => {
            try {
                const result = await listHubs();
                setHubs(result);
                if (result.length > 0 && !hubId) {
                    setHubId(result[0].hub_id);
                }
            } catch (err: any) {
                toast.error(err?.response?.data?.detail || "Erro ao carregar hubs.");
            }
        };
        carregarHubs();
    }, []);

    const datasValidas = () => Boolean(dataInicial && (!dataFinal || dataFinal >= dataInicial));


    // =========================
    // Ações: Processar simulação
    // =========================
    const processar = async () => {
        setMsg(null);
        setArtefatos(null);

        if (!datasValidas()) {
            toast.error("Informe uma data inicial válida.");
            return;
        }
        if (!hubId) {
            toast.error("Selecione um hub central.");
            return;
        }

        setLoading(true);
        try {
            // 🔹 Monta os parâmetros dinamicamente
            const simParams: any = {
                data_inicial: dataInicial,
                hub_id: hubId,
                ...params,
            };

            if (dataFinal && dataFinal.trim() !== "") {
                simParams.data_final = dataFinal;  // só adiciona se o usuário preencheu
            }

            await runSimulation(simParams);


            setMsg("⏳ Simulação enviada para processamento.");
        } catch (e: any) {
            const errMsg = e?.response?.data?.detail || "Erro ao executar simulação.";
            setMsg(errMsg);
            toast.error(errMsg);
        } finally {
            setLoading(false);
        }
    };


    // =========================
    // Ações: Visualizar artefatos (por dataVisualizar)
    // =========================
    const gerarRelatorios = async () => {
        if (!dataVisualizar) {
            toast.error("Selecione a data para visualizar.");
            return;
        }
        try {
            const data = await visualizeSimulation(dataVisualizar);
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
    const [freqCidadesGraficoUrl, setFreqCidadesGraficoUrl] = useState<string | null>(
        null
    );
    const [freqCidadesCsvUrl, setFreqCidadesCsvUrl] = useState<string | null>(null); // 👈 NOVO
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
            setFreqCidadesCsvUrl( // 👈 salva também o CSV
                result.csv ? resolveUrl(result.csv) : null
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
    const [kFixoData, setKFixoData] = useState<KFixoResponse["cenarios"]>([]);
    const [kFixoGraficoUrl, setKFixoGraficoUrl] = useState<string | null>(null);
    const [loadingKFixo, setLoadingKFixo] = useState(false);

    const periodoKFixoDefault = useMemo(() => {
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
                min_cobertura_parcial: minCobertura / 100,
            });

            setKFixoData(result.cenarios || []); // ✅ agora usa cenarios
            setKFixoGraficoUrl(result.grafico ? resolveUrl(result.grafico) : null);
            toast.success("Custos por k carregados!");
        } catch (err: any) {
            toast.error(err?.response?.data?.detail || "Erro ao carregar custos por k.");
        } finally {
            setLoadingKFixo(false);
        }
    };


    // =========================
    // ABA: Frota p/ k Fixo
    // =========================
    const [frotaK, setFrotaK] = useState<number | null>(null);
    const [frotaLastmile, setFrotaLastmile] = useState<FrotaKFixoResponse["lastmile"]>([]);
    const [frotaTransfer, setFrotaTransfer] = useState<FrotaKFixoResponse["transfer"]>([]);
    const [frotaCsvLastmile, setFrotaCsvLastmile] = useState<string | null>(null);
    const [frotaCsvTransfer, setFrotaCsvTransfer] = useState<string | null>(null);
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

        if (!frotaK) {
            toast.error("Informe um valor de k (ex.: 8)");
            return;
        }
        if (!di || !df || df < di) {
            toast.error("Informe um intervalo de datas válido.");
            return;
        }

        console.log("🚚 Enviando frota_k_fixo:", { di, df, frotaK }); // debug

        setLoadingFrota(true);
        try {
            const result = await getFrotaKFixo({
                data_inicial: di,
                data_final: df,
                k: frotaK, // ✅ apenas um k inteiro
            });

            setFrotaLastmile(result.lastmile || []);
            setFrotaTransfer(result.transfer || []);
            setFrotaCsvLastmile(result.csv_lastmile || null);
            setFrotaCsvTransfer(result.csv_transfer || null);


            toast.success("Frota sugerida carregada!");
        } catch (err: any) {
            toast.error(err?.response?.data?.detail || "Erro ao carregar frota sugerida.");
        } finally {
            setLoadingFrota(false);
        }
    };

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
                    <div className="bg-white rounded-2xl shadow p-4 mb-6">
                        {/* Linha 1 */}
                        <div className="grid md:grid-cols-4 gap-4 mb-4">
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

                            <div>
                                <label className="block text-sm text-gray-700">Hub Central</label>
                                <select
                                    value={hubId ?? ""}
                                    onChange={(e) => setHubId(Number(e.target.value))}
                                    className="input"
                                    disabled={!hubs.length}
                                >
                                    <option value="" disabled>
                                        {hubs.length ? "Selecione um hub" : "Carregando hubs..."}
                                    </option>
                                    {hubs.map((h) => (
                                        <option key={h.hub_id} value={h.hub_id}>
                                            {h.nome} ({h.cidade})
                                        </option>
                                    ))}
                                </select>
                            </div>

                            {/* Botão Processar */}
                            <div className="flex items-end">
                                <button
                                    disabled={loading || !datasValidas()}
                                    onClick={processar}
                                    className={`w-full flex items-center justify-center gap-2 px-5 py-2.5 rounded-xl font-medium shadow transition-all duration-200
                                        ${loading || !datasValidas()
                                            ? "bg-emerald-300 text-white cursor-not-allowed"
                                            : "bg-emerald-600 hover:bg-emerald-700 text-white hover:shadow-md"}`}
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
                        </div>

                        {/* Linha 2 */}
                        <div className="grid md:grid-cols-4 gap-4">
                            <div className="md:col-span-3">
                                <label className="block text-sm text-gray-700">Data para visualizar</label>
                                <input
                                    type="date"
                                    value={dataVisualizar}
                                    max={todayISO()}
                                    onChange={(e) => setDataVisualizar(e.target.value)}
                                    className="input w-full"
                                />
                            </div>

                            <div className="flex items-end">
                                <button
                                    disabled={!dataVisualizar}
                                    onClick={gerarRelatorios}
                                    className={`w-full flex items-center justify-center gap-2 px-5 py-2.5 rounded-xl font-medium border transition-all duration-200
                                        ${!dataVisualizar
                                            ? "border-gray-300 text-gray-400 cursor-not-allowed"
                                            : "border-emerald-600 text-emerald-600 hover:bg-emerald-50 hover:shadow-sm"}`}
                                >
                                    <FileText className="w-4 h-4" /> Gerar Relatórios & Gráficos
                                </button>
                            </div>
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
                            <label className="flex gap-2 items-center">
                                <input
                                    type="checkbox"
                                    checked={params.modo_forcar}
                                    onChange={(e) => setParams({ ...params, modo_forcar: e.target.checked })}
                                />
                                Forçar sobrescrita (modo_forcar)
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
                                        <Bar dataKey="qtd" fill="#4682B4" radius={[4, 4, 0, 0]}>
                                            {distKData.map((_, index) => (
                                                <Cell
                                                    key={`cell-${index}`}
                                                    fill="#4682B4"
                                                />
                                            ))}
                                        </Bar>
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
                        <>
                            {/* Gráfico Top 20 */}
                            <div className="w-full h-[420px] border rounded-lg p-2 mb-4 bg-white">
                                <ResponsiveContainer width="100%" height="100%">
                                    <RBarChart
                                        data={freqCidadesData}
                                        layout="vertical"
                                        margin={{ top: 10, right: 20, left: 120, bottom: 10 }}
                                    >
                                        <CartesianGrid strokeDasharray="3 3" />
                                        <XAxis
                                            type="number"
                                            allowDecimals={false}
                                            label={{
                                                value: "Frequência",
                                                position: "insideBottom",
                                                offset: -5,
                                            }}
                                        />
                                        <YAxis
                                            type="category"
                                            dataKey="cluster_cidade"
                                            tick={{ fontSize: 11 }}
                                            width={140}
                                        />
                                        <Tooltip />
                                        <Bar dataKey="qtd" fill="#10b981" radius={[0, 6, 6, 0]} />
                                    </RBarChart>
                                </ResponsiveContainer>
                            </div>

                            {/* Links de download */}
                            <div className="flex justify-between items-center mt-2">
                                {freqCidadesGraficoUrl && (
                                    <a
                                        href={freqCidadesGraficoUrl}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-emerald-600 hover:underline"
                                    >
                                        📥 Baixar gráfico oficial (PNG)
                                    </a>
                                )}
                                {freqCidadesCsvUrl && (
                                    <a
                                        href={freqCidadesCsvUrl}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-emerald-600 hover:underline"
                                    >
                                        📄 Baixar lista completa (CSV)
                                    </a>
                                )}
                            </div>

                            {/* Tabela Top 20 */}
                            <div className="overflow-auto mt-4">
                                <table className="min-w-full text-sm">
                                    <thead>
                                        <tr className="text-left border-b">
                                            <th className="py-2 pr-4 w-12">#</th>
                                            <th className="py-2 pr-4">Cidade</th>
                                            <th className="py-2 pr-4">Frequência</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {freqCidadesData.map((r, i) => (
                                            <tr key={i} className="border-b">
                                                <td className="py-2 pr-4">{i + 1}</td>
                                                <td className="py-2 pr-4">{r.cluster_cidade}</td>
                                                <td className="py-2 pr-4">{r.qtd}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    )}
                </div>
            )}


            {/* ===================== ABA: k Fixo (custos) =====================*/}
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
                        {/* 👇 Novo campo cobertura mínima */}
                        <div>
                            <label
                                className="block text-sm text-gray-700"
                                title="Mínimo de cobertura exigida em % dos dias válidos"
                            >
                                Cobertura mínima (%)
                            </label>
                            <input
                                type="number"
                                value={minCobertura}
                                onChange={(e) => setMinCobertura(+e.target.value)}
                                className="input"
                                min={0}
                                max={100}
                                step={1}
                                placeholder="Ex.: 70"
                            />
                        </div>
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
                            <div className="w-full h-[420px] border rounded-lg p-2 mb-4 bg-white">
                                <ResponsiveContainer width="100%" height="100%">
                                    <RBarChart data={kFixoData}>
                                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                        <XAxis dataKey="k_clusters" tickMargin={8}>
                                            <Label value="Clusters (k)" offset={-5} position="insideBottom" />
                                        </XAxis>
                                        <YAxis
                                            tickFormatter={(v) =>
                                                new Intl.NumberFormat("pt-BR", {
                                                    notation: "compact",
                                                    maximumFractionDigits: 1,
                                                }).format(v)
                                            }
                                            label={{
                                                value: "Custo consolidado (R$)",
                                                angle: -90,
                                                position: "insideLeft",
                                            }}
                                            tickMargin={6}
                                        />
                                        <Tooltip
                                            formatter={(v: number) =>
                                                new Intl.NumberFormat("pt-BR", {
                                                    style: "currency",
                                                    currency: "BRL",
                                                }).format(v)
                                            }
                                            labelFormatter={(k) => `k = ${k}`}
                                        />
                                        <Bar dataKey="custo_alvo" fill="#2563eb" radius={[6, 6, 0, 0]} />
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

                            <div className="overflow-auto mt-4">
                                <table className="min-w-full text-sm">
                                    <thead>
                                        <tr className="text-left border-b">
                                            <th className="py-2 pr-4">k</th>
                                            <th className="py-2 pr-4">Dias válidos</th>
                                            <th className="py-2 pr-4">Cobertura</th>
                                            <th className="py-2 pr-4">Custo alvo (R$)</th>
                                            <th className="py-2 pr-4">Regret %</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {kFixoData
                                            .slice()
                                            .sort(
                                                (a: KFixoResponse["cenarios"][0], b: KFixoResponse["cenarios"][0]) =>
                                                    a.k_clusters - b.k_clusters
                                            )
                                            .map((r: KFixoResponse["cenarios"][0]) => (
                                                <tr key={r.k_clusters} className="border-b">
                                                    <td className="py-2 pr-4">{r.k_clusters}</td>
                                                    <td className="py-2 pr-4">
                                                        {r.dias_presentes}/{r.total_dias}
                                                    </td>
                                                    <td className="py-2 pr-4">
                                                        {(r.cobertura_pct * 100).toFixed(1)}%
                                                    </td>
                                                    <td className="py-2 pr-4">
                                                        {r.custo_alvo?.toLocaleString("pt-BR", {
                                                            style: "currency",
                                                            currency: "BRL",
                                                        })}
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
                        Frota média sugerida por k fixo
                    </h2>

                    {/* Filtros */}
                    <div className="grid md:grid-cols-4 gap-4 mb-4">
                        <div>
                            <label className="block text-sm text-gray-700">Data inicial</label>
                            <input
                                type="date"
                                value={periodoFrotaDefault.data_inicial}
                                max={todayISO()}
                                onChange={(e) =>
                                    setDataInicial(e.target.value)
                                }
                                className="input"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-700">Data final</label>
                            <input
                                type="date"
                                value={periodoFrotaDefault.data_final}
                                max={todayISO()}
                                onChange={(e) =>
                                    setDataFinal(e.target.value)
                                }
                                className="input"
                            />
                        </div>
                        <div>
                            <label className="block text-sm text-gray-700">
                                Valor de k (ex.: 8)
                            </label>
                            <input
                                type="number"
                                value={frotaK || ""}
                                onChange={(e) => setFrotaK(Number(e.target.value))}
                                className="input"
                                placeholder="Informe um k"
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

                    {/* Sem dados */}
                    {!loadingFrota && !frotaLastmile.length && !frotaTransfer.length && (
                        <div className="text-sm text-gray-500">
                            Informe o período e o valor de <strong>k</strong>, depois clique em{" "}
                            <strong>Gerar frota</strong>.
                        </div>
                    )}

                    {/* Bloco Last-mile */}
                    {frotaLastmile.length > 0 && (
                        <div className="mb-10">
                            <h3 className="text-lg font-bold mb-3">🚚 Last-mile</h3>
                            <FrotaChartTable data={frotaLastmile} chartId="frotaLastmileChart" />
                            {frotaCsvLastmile && (
                                <div className="mt-2 text-right">
                                    <a
                                        href={frotaCsvLastmile}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-emerald-600 hover:underline"
                                    >
                                        📥 Baixar CSV Last-mile
                                    </a>
                                </div>
                            )}
                        </div>
                    )}

                    {/* Bloco Transferências */}
                    {frotaTransfer.length > 0 && (
                        <div>
                            <h3 className="text-lg font-bold mb-3">🚛 Transferências</h3>
                            <FrotaChartTable data={frotaTransfer} chartId="frotaTransferChart" />
                            {frotaCsvTransfer && (
                                <div className="mt-2 text-right">
                                    <a
                                        href={frotaCsvTransfer}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="text-emerald-600 hover:underline"
                                    >
                                        📥 Baixar CSV Transferências
                                    </a>
                                </div>
                            )}
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
