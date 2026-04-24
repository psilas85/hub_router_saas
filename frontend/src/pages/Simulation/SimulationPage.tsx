// hub_router_1.0.1/frontend/src/pages/Simulation/SimulationPage.tsx
import { useMemo, useState, useEffect } from "react";
import { useSimulationJob } from "@/hooks/useSimulationJob";
import {
    runSimulation,
    visualizeSimulation,
    getDistribuicaoK,
    getFrequenciaCidades,
    getKFixo,
    getFrotaKFixo,
    listHubs,
    getHistorico,          // 👈 novo
    type HistoricoSimulation,
    type VisualizeSimulationResponse,
    type DistribuicaoKResponse,
    type FrequenciaCidadesResponse,
    type KFixoResponse,
    type FrotaKFixoResponse,
    type RunSimulationParams,
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
    Settings2,
    History,
    TimerReset,
    Gauge,
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

function formatKLabel(k: string | number) {
    return String(k) === "0" ? "Hub único" : String(k);
}

function formatScenarioLabel(k: string | number) {
    if (String(k) === "0") {
        return "Cenário Hub único";
    }
    return `Cenário k=${k}`;
}

function todayISO() {
    const d = new Date();
    return d.toISOString().slice(0, 10);
}

const resolveUrl = (path: string) => {
    if (!path) return "";
    if (path.startsWith("http")) return path;
    return `${import.meta.env.VITE_API_URL}${path.startsWith("/") ? path : `/${path}`}`;
};

function extractApiErrorMessage(err: any, fallback: string) {
    const detail = err?.response?.data?.detail ?? err?.response?.data ?? err?.message;

    if (typeof detail === "string") {
        const nestedDetailMatch = detail.match(/["']detail["']\s*:\s*["']([^"']+)["']/i);
        if (nestedDetailMatch?.[1]) {
            return nestedDetailMatch[1];
        }

        const noDataMatch = detail.match(/Nenhum[^.]+\./i);
        if (noDataMatch?.[0]) {
            return noDataMatch[0];
        }

        return detail;
    }

    if (detail && typeof detail === "object") {
        if (typeof detail.detail === "string") {
            return detail.detail;
        }
        return fallback;
    }

    return fallback;
}

function isNoDataResponse(err: any) {
    const status = err?.response?.status;
    const message = extractApiErrorMessage(err, "");
    return status === 404 && /nenhum|não há dados/i.test(message);
}

type ParamState = Omit<
    RunSimulationParams,
    "data_inicial" | "data_final" | "hub_id"
>;

// 🔧 Accordion wrapper
function Accordion({ title, subtitle, defaultOpen = false, children }: { title: string; subtitle?: string; defaultOpen?: boolean; children: React.ReactNode }) {
    const [open, setOpen] = useState(defaultOpen);
    return (
        <div className="mb-3 overflow-hidden rounded-[24px] border border-slate-200 bg-white shadow-[0_10px_35px_rgba(15,23,42,0.05)]">
            <button
                onClick={() => setOpen(!open)}
                className="flex w-full items-center justify-between bg-gradient-to-r from-white via-white to-emerald-50/50 px-4 py-3"
            >
                <div className="text-left">
                    <div className="font-semibold text-slate-900">{title}</div>
                    {subtitle ? <div className="mt-0.5 text-sm text-slate-500">{subtitle}</div> : null}
                </div>
                <ChevronDown className={`w-4 h-4 transition-transform ${open ? "rotate-180" : ""}`} />
            </button>
            {open && <div className="grid gap-3 border-t border-slate-100 p-4 md:grid-cols-2 xl:grid-cols-3">{children}</div>}
        </div>
    );
}

function FieldLabel({ title, hint }: { title: string; hint?: string }) {
    return (
        <div className="mb-2 flex items-center gap-2">
            <div className="text-sm font-medium text-slate-700">{title}</div>
            {hint ? <HelpHint text={hint} /> : null}
        </div>
    );
}

function HelpHint({ text }: { text: string }) {
    return (
        <span className="group relative inline-flex">
            <span className="inline-flex h-5 w-5 cursor-help items-center justify-center rounded-full border border-slate-300 bg-white text-[11px] font-semibold text-slate-500 transition hover:border-emerald-300 hover:text-emerald-700">
                ?
            </span>
            <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-56 -translate-x-1/2 rounded-2xl border border-slate-200 bg-slate-900 px-3 py-2 text-xs font-medium leading-5 text-white shadow-xl group-hover:block">
                {text}
            </span>
        </span>
    );
}

function ToggleField({
    title,
    hint,
    checked,
    onChange,
}: {
    title: string;
    hint: string;
    checked: boolean;
    onChange: (checked: boolean) => void;
}) {
    return (
        <label className="flex cursor-pointer items-start gap-3 rounded-3xl border border-slate-200 bg-slate-50/70 p-4 transition hover:border-emerald-200 hover:bg-emerald-50/60">
            <input
                type="checkbox"
                checked={checked}
                onChange={(e) => onChange(e.target.checked)}
                className="mt-1 h-4 w-4 rounded border-slate-300 text-emerald-600 focus:ring-emerald-500"
            />
            <div>
                <div className="flex items-center gap-2 text-sm font-medium text-slate-800">
                    <span>{title}</span>
                    <HelpHint text={hint} />
                </div>
            </div>
        </label>
    );
}

type TabKey = "simulacao" | "cenarioVencedor" | "centroVencedor" | "custos" | "frota";
type BannerTone = "info" | "success" | "error";

type BannerState = {
    text: string;
    tone: BannerTone;
};


function isJobSuccess(status: string) {
    return status === "done" || status === "finished";
}

function isJobFailure(status: string) {
    return status === "error" || status === "failed";
}

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
                                <td className="py-2 pr-4">{formatKLabel(r.k_clusters)}</td>
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
    const [banner, setBanner] = useState<BannerState | null>(null);
    const [pendingJobId, setPendingJobId] = useState<string | null>(null);
    const [artefatos, setArtefatos] = useState<VisualizeSimulationResponse | null>(null);
    const [minCobertura, setMinCobertura] = useState(70);
    const [historico, setHistorico] = useState<HistoricoSimulation[]>([]);
    const [loadingHistorico, setLoadingHistorico] = useState(false);



    // 🔧 Estado centralizado
    const [params, setParams] = useState<ParamState>({
        modo_simulacao: "padrao",

        min_entregas_por_cluster_alvo: 10,
        max_entregas_por_cluster_alvo: 100,

        desativar_cluster_hub: false,
        raio_hub_km: 80.0,

        usar_outlier: false,
        distancia_outlier_km: undefined,

        tempo_parada_leve: 10,
        tempo_parada_pesada: 20,
        tempo_por_volume: 0.4,
        limite_peso_parada: 200,

        velocidade_kmh: 45.0,

        limite_peso_veiculo: 50.0,
        peso_max_transferencia: 18000.0,

        tempo_max_transferencia: 600,
        tempo_max_roteirizacao: 600,

        // 🔥 ADICIONAR AQUI
        tempo_max_k0: 1200,

        entregas_por_rota: 25,

        permitir_rotas_excedentes: true,

        // 🔥 NOVO PARAM
        permitir_veiculo_leve_intermunicipal: false,

        modo_forcar: false,

        tempo_especial_min: 180,
        tempo_especial_max: 300,
        max_especiais_por_rota: 1,
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
    const simulationInProgress = loading || pendingJobId !== null;

    const updateParam = <K extends keyof ParamState>(key: K, value: ParamState[K]) => {
        setParams((prev) => ({ ...prev, [key]: value }));
    };

    const handleModoSimulacaoChange = (
        modo: NonNullable<ParamState["modo_simulacao"]>
    ) => {
        setParams((prev) => {
            const nextParams: ParamState = {
                ...prev,
                modo_simulacao: modo,
            };

            if (modo === "time_windows") {
                nextParams.tempo_especial_min = prev.tempo_especial_min ?? 180;
                nextParams.tempo_especial_max = prev.tempo_especial_max ?? 300;
                nextParams.max_especiais_por_rota = prev.max_especiais_por_rota ?? 1;
            }

            return nextParams;
        });
    };

    const updateNumericParam = (key: keyof ParamState, rawValue: string) => {
        const trimmed = rawValue.trim();

        setParams((prev) => {
            if (trimmed === "") {
                return { ...prev, [key]: undefined };
            }

            const val = Number(trimmed);

            return {
                ...prev,
                [key]: isNaN(val) ? undefined : val,
            };
        });
    };

    // =========================
    // Ações: Processar simulação
    // =========================
    const processar = async () => {
        setBanner(null);
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
            const modoSelecionado = params.modo_simulacao ?? "padrao";
            const simParams: any = {
                data_inicial: dataInicial,
                hub_id: hubId,
                ...params,
                data_final: dataFinal && dataFinal.trim() !== "" ? dataFinal : dataInicial,
            };

            delete simParams.algoritmo_clusterizacao;
            delete simParams.algoritmo_roteirizacao;

            Object.keys(simParams).forEach((key) => {
                if (simParams[key] === undefined) {
                    delete simParams[key];
                }
            });

            console.log("🚀 PAYLOAD FINAL:", simParams);

            if (modoSelecionado !== "time_windows") {
                delete simParams.tempo_especial_min;
                delete simParams.tempo_especial_max;
                delete simParams.max_especiais_por_rota;
            }

            const response = await runSimulation(simParams);
            const novoPendingJobId = response.job_id ?? null;

            setPendingJobId(novoPendingJobId);

            setBanner({
                text: "⏳ Simulação enviada para processamento.",
                tone: "info"
            });

        } catch (e: any) {
            const errMsg = e?.response?.data?.detail || "Erro ao executar simulação.";
            setBanner({ text: errMsg, tone: "error" });
            toast.error(errMsg);
        } finally {
            setLoading(false);
        }
    };
    async function carregarHistorico(jobIdEmAcompanhamento?: string | null) {
        try {
            setLoadingHistorico(true);
            const dados = await getHistorico(10);
            const historicoAtualizado = dados.historico || [];

            setHistorico(historicoAtualizado);

            if (jobIdEmAcompanhamento) {
                const jobAtual = historicoAtualizado.find((item) => item.job_id === jobIdEmAcompanhamento);

                if (jobAtual?.status && isJobSuccess(jobAtual.status)) {
                    setBanner({
                        text: jobAtual.mensagem || "✅ Simulação concluída com sucesso.",
                        tone: "success",
                    });
                    setPendingJobId(null);
                } else if (jobAtual?.status && isJobFailure(jobAtual.status)) {
                    setBanner({
                        text: jobAtual.mensagem || "❌ A simulação falhou.",
                        tone: "error",
                    });
                    setPendingJobId(null);
                } else if (jobAtual?.status === "processing") {
                    setBanner({
                        text: jobAtual.mensagem || "⏳ Simulação em processamento.",
                        tone: "info",
                    });
                }
            }
        } catch (err: any) {
            toast.error("Erro ao carregar histórico de simulações");
        } finally {
            setLoadingHistorico(false);
        }
    }


    useEffect(() => {
        carregarHistorico();
    }, []);

    useSimulationJob({
        jobId: pendingJobId,
        onUpdate: (msg, tone) => {
            setBanner({ text: msg, tone });
        },
        onFinish: () => {
            setPendingJobId(null);
            carregarHistorico();
        },
        onError: () => {
            setPendingJobId(null);
        },
    });

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
            setBanner({ text: "✅ Artefatos carregados.", tone: "success" });
            toast.success("Artefatos carregados!");
        } catch (e: any) {
            const errMsg = e?.response?.data?.detail || "Erro ao carregar artefatos.";
            setBanner({ text: errMsg, tone: "error" });
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
            setDistKEmptyMessage(null);
            toast.success("Distribuição de k carregada!");
        } catch (err: any) {
            setDistKData([]);
            setDistKGraficoUrl(null);
            if (isNoDataResponse(err)) {
                setDistKEmptyMessage("Nenhum cenário vencedor foi encontrado para o período selecionado.");
            } else {
                setDistKEmptyMessage(null);
                toast.error(extractApiErrorMessage(err, "Erro ao carregar distribuição de k."));
            }
        } finally {
            setLoadingDistK(false);
        }
    };

    // =========================
    // ABA: Frequência de Cidades
    // =========================
    const [freqCidadesData, setFreqCidadesData] =
        useState<FrequenciaCidadesResponse["dados"]>([]);
    const [freqCidadesContexto, setFreqCidadesContexto] =
        useState<FrequenciaCidadesResponse["contexto"] | null>(null);
    const [freqCidadesGraficoUrl, setFreqCidadesGraficoUrl] = useState<string | null>(
        null
    );
    const [freqCidadesCsvUrl, setFreqCidadesCsvUrl] = useState<string | null>(null); // 👈 NOVO
    const [loadingFreqCidades, setLoadingFreqCidades] = useState(false);
    const [distKEmptyMessage, setDistKEmptyMessage] = useState<string | null>(null);
    const [freqCidadesEmptyMessage, setFreqCidadesEmptyMessage] = useState<string | null>(null);
    const [kFixoEmptyMessage, setKFixoEmptyMessage] = useState<string | null>(null);
    const [frotaEmptyMessage, setFrotaEmptyMessage] = useState<string | null>(null);

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
            setFreqCidadesContexto(result.contexto || null);
            setFreqCidadesGraficoUrl(
                result.grafico ? resolveUrl(result.grafico) : null
            );
            setFreqCidadesCsvUrl( // 👈 salva também o CSV
                result.csv ? resolveUrl(result.csv) : null
            );
            setFreqCidadesEmptyMessage(null);
            toast.success("Frequência de cidades carregada!");
        } catch (err: any) {
            setFreqCidadesData([]);
            setFreqCidadesContexto(null);
            setFreqCidadesGraficoUrl(null);
            setFreqCidadesCsvUrl(null);
            if (isNoDataResponse(err)) {
                setFreqCidadesEmptyMessage("Nenhum hub ou centro vencedor foi encontrado para o período selecionado.");
            } else {
                setFreqCidadesEmptyMessage(null);
                toast.error(extractApiErrorMessage(err, "Erro ao carregar frequência de cidades."));
            }
        } finally {
            setLoadingFreqCidades(false);
        }
    };


    // =========================
    // ABA: comparativo de custos por cenário
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
            toast.error("Informe um intervalo de datas válido para o comparativo de cenários.");
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
            setKFixoEmptyMessage(null);
            toast.success("Custos por k carregados!");
        } catch (err: any) {
            setKFixoData([]);
            setKFixoGraficoUrl(null);
            if (isNoDataResponse(err)) {
                setKFixoEmptyMessage("Nenhum cenário com dados de simulação foi encontrado para o período selecionado.");
            } else {
                setKFixoEmptyMessage(null);
                toast.error(extractApiErrorMessage(err, "Erro ao carregar custos por k."));
            }
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
    const [frotaResumo, setFrotaResumo] = useState<Pick<
        FrotaKFixoResponse,
        "k_consultado" | "escopo" | "transfer_aplicavel" | "mensagem_transfer"
    > | null>(null);
    const [loadingFrota, setLoadingFrota] = useState(false);
    const modoSelecionado = params.modo_simulacao ?? "padrao";

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

        if (!frotaK && frotaK !== 0) {
            toast.error("Informe um valor de k (ex.: 0 para Hub único ou 8)");
            return;
        }
        if (!di || !df || df < di) {
            toast.error("Informe um intervalo de datas válido.");
            return;
        }

        console.log("🚚 Enviando frota_k_fixo:", { di, df, frotaK });

        setLoadingFrota(true);
        try {
            const result = await getFrotaKFixo({
                data_inicial: di,
                data_final: df,
                k: frotaK,
            });

            setFrotaLastmile(result.lastmile || []);
            setFrotaTransfer(result.transfer || []);
            setFrotaCsvLastmile(result.csv_lastmile || null);
            setFrotaCsvTransfer(result.csv_transfer || null);
            setFrotaResumo({
                k_consultado: result.k_consultado,
                escopo: result.escopo,
                transfer_aplicavel: result.transfer_aplicavel,
                mensagem_transfer: result.mensagem_transfer,
            });
            setFrotaEmptyMessage(null);

            toast.success("Frota sugerida carregada!");
        } catch (err: any) {
            setFrotaLastmile([]);
            setFrotaTransfer([]);
            setFrotaCsvLastmile(null);
            setFrotaCsvTransfer(null);
            setFrotaResumo(null);
            if (isNoDataResponse(err)) {
                setFrotaEmptyMessage("Nenhuma frota sugerida foi encontrada para o período e k informados.");
            } else {
                setFrotaEmptyMessage(null);
                toast.error(extractApiErrorMessage(err, "Erro ao carregar frota sugerida."));
            }
        } finally {
            setLoadingFrota(false);
        }
    };


    return (
        <div className="mx-auto max-w-7xl p-6">
            <h1 className="mb-5 flex items-center gap-2 text-2xl font-bold text-slate-900">
                <BarChart3 className="w-6 h-6 text-emerald-600" />
                Simulação
            </h1>

            {/* Tabs */}
            <div className="mb-6 overflow-x-auto pb-1">
                <div className="inline-flex min-w-max rounded-2xl border border-slate-200 bg-white p-1 shadow-sm">
                    <button
                        className={`flex items-center gap-2 rounded-xl px-4 py-2.5 ${activeTab === "simulacao" ? "bg-emerald-600 text-white shadow-sm" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("simulacao")}
                    >
                        <Play className="w-4 h-4" /> Simulação
                    </button>
                    <button
                        className={`flex items-center gap-2 rounded-xl px-4 py-2.5 ${activeTab === "cenarioVencedor" ? "bg-emerald-600 text-white shadow-sm" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("cenarioVencedor")}
                    >
                        <BarChart2 className="w-4 h-4" /> Cenário Vencedor
                    </button>
                    <button
                        className={`flex items-center gap-2 rounded-xl px-4 py-2.5 ${activeTab === "centroVencedor" ? "bg-emerald-600 text-white shadow-sm" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("centroVencedor")}
                    >
                        <Building2 className="w-4 h-4" /> Centro Vencedor
                    </button>
                    <button
                        className={`flex items-center gap-2 rounded-xl px-4 py-2.5 ${activeTab === "custos" ? "bg-emerald-600 text-white shadow-sm" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("custos")}
                    >
                        <BarChart3 className="w-4 h-4" /> Custos
                    </button>
                    <button
                        className={`flex items-center gap-2 rounded-xl px-4 py-2.5 ${activeTab === "frota" ? "bg-emerald-600 text-white shadow-sm" : "text-gray-700 hover:bg-gray-50"}`}
                        onClick={() => setActiveTab("frota")}
                    >
                        <Map className="w-4 h-4" /> Frota
                    </button>
                </div>
            </div>

            {/* ===================== ABA: Simulação ===================== */}
            {activeTab === "simulacao" && (
                <>
                    {banner ? (
                        <div className={`mb-4 rounded-[24px] px-4 py-3 text-sm shadow-sm ${banner.tone === "success" ? "border border-emerald-200 bg-emerald-50 text-emerald-700" : banner.tone === "error" ? "border border-rose-200 bg-rose-50 text-rose-700" : "border border-amber-200 bg-amber-50 text-amber-700"}`}>
                            {banner.text}
                        </div>
                    ) : null}

                    <div className="mb-5 grid gap-5 xl:grid-cols-[minmax(0,1fr)_380px]">
                        <div className="space-y-3">
                            <div className="flex items-center gap-2 text-sm font-semibold uppercase tracking-[0.14em] text-slate-500">
                                <Settings2 className="h-4 w-4 text-emerald-600" />
                                Parâmetros da simulação
                            </div>

                            <Accordion title="Estratégia" subtitle="cenários e clusterização" defaultOpen>
                                <div>
                                    <FieldLabel title="Modo de simulação" />
                                    <select
                                        value={params.modo_simulacao}
                                        onChange={(e) =>
                                            handleModoSimulacaoChange(
                                                e.target.value as NonNullable<ParamState["modo_simulacao"]>
                                            )
                                        }
                                        className="input"
                                    >
                                        <option value="padrao">Padrão</option>
                                        <option value="balanceado">Balanceado</option>
                                        <option value="time_windows">Time Windows</option>
                                    </select>
                                    <p className="mt-2 text-xs text-slate-500">
                                        O modo define automaticamente a clusterização e a roteirização last-mile.
                                    </p>
                                </div>

                                <div>
                                    <FieldLabel title="Mín. entregas por cluster alvo" />
                                    <input
                                        type="number"
                                        value={params.min_entregas_por_cluster_alvo ?? ""}
                                        onChange={(e) => updateNumericParam("min_entregas_por_cluster_alvo", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Máx. entregas por cluster alvo" />
                                    <input
                                        type="number"
                                        value={params.max_entregas_por_cluster_alvo ?? ""}
                                        onChange={(e) => updateNumericParam("max_entregas_por_cluster_alvo", e.target.value)}
                                        className="input"
                                    />
                                </div>

                            </Accordion>

                            {modoSelecionado === "time_windows" && (
                                <Accordion title="Time Windows" subtitle="restrições especiais">
                                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                        <div>
                                            <FieldLabel title="Min. especial (min)" />
                                            <input
                                                type="number"
                                                value={params.tempo_especial_min ?? ""}
                                                onChange={(e) => updateNumericParam("tempo_especial_min", e.target.value)}
                                                className="input"
                                            />
                                        </div>

                                        <div>
                                            <FieldLabel title="Max. especial (min)" />
                                            <input
                                                type="number"
                                                value={params.tempo_especial_max ?? ""}
                                                onChange={(e) => updateNumericParam("tempo_especial_max", e.target.value)}
                                                className="input"
                                            />
                                        </div>

                                        <div>
                                            <FieldLabel title="Máx. especiais/rota" />
                                            <input
                                                type="number"
                                                value={params.max_especiais_por_rota ?? ""}
                                                onChange={(e) => updateNumericParam("max_especiais_por_rota", e.target.value)}
                                                className="input"
                                            />
                                        </div>
                                    </div>
                                </Accordion>
                            )}

                            <Accordion title="Hub e outliers">
                                <div>
                                    <FieldLabel title="Raio do hub (km)" />
                                    <input
                                        type="number"
                                        value={params.raio_hub_km ?? ""}
                                        onChange={(e) => updateNumericParam("raio_hub_km", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Distância outlier (km)" />
                                    <input
                                        type="number"
                                        disabled={!params.usar_outlier}
                                        value={params.distancia_outlier_km ?? ""}
                                        onChange={(e) => updateNumericParam("distancia_outlier_km", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <ToggleField
                                    title="Desativar cluster hub"
                                    hint="Ignora agrupamento pelo hub"
                                    checked={params.desativar_cluster_hub ?? false}
                                    onChange={(checked) => updateParam("desativar_cluster_hub", checked)}
                                />

                                <ToggleField
                                    title="Separar outliers"
                                    hint="Cria clusters separados para pontos distantes"
                                    checked={params.usar_outlier ?? false}
                                    onChange={(checked) => updateParam("usar_outlier", checked)}
                                />
                            </Accordion>

                            <Accordion title="Tempos">
                                <div>
                                    <FieldLabel title="Parada leve (min)" />
                                    <input
                                        type="number"
                                        value={params.tempo_parada_leve ?? ""}
                                        onChange={(e) => updateNumericParam("tempo_parada_leve", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Parada pesada (min)" />
                                    <input
                                        type="number"
                                        value={params.tempo_parada_pesada ?? ""}
                                        onChange={(e) => updateNumericParam("tempo_parada_pesada", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Tempo por volume" />
                                    <input
                                        type="number"
                                        value={params.tempo_por_volume ?? ""}
                                        onChange={(e) => updateNumericParam("tempo_por_volume", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Tempo máx. rotas (k > 0)" />
                                    <input
                                        type="number"
                                        value={params.tempo_max_roteirizacao ?? ""}
                                        onChange={(e) => updateNumericParam("tempo_max_roteirizacao", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Tempo máx. transferência" />
                                    <input
                                        type="number"
                                        value={params.tempo_max_transferencia ?? ""}
                                        onChange={(e) => updateNumericParam("tempo_max_transferencia", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Tempo máx. hub único (k=0)" />
                                    <input
                                        type="number"
                                        value={params.tempo_max_k0 ?? ""}
                                        onChange={(e) => updateNumericParam("tempo_max_k0", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Entregas por rota" />
                                    <input
                                        type="number"
                                        value={params.entregas_por_rota ?? ""}
                                        onChange={(e) => updateNumericParam("entregas_por_rota", e.target.value)}
                                        className="input"
                                    />
                                </div>
                            </Accordion>

                            <Accordion title="Capacidade">
                                <div>
                                    <FieldLabel title="Velocidade (km/h)" />
                                    <input
                                        type="number"
                                        value={params.velocidade_kmh ?? ""}
                                        onChange={(e) => updateNumericParam("velocidade_kmh", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Limite peso veículo (kg)" />
                                    <input
                                        type="number"
                                        value={params.limite_peso_veiculo ?? ""}
                                        onChange={(e) => updateNumericParam("limite_peso_veiculo", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Limite peso parada (kg)" />
                                    <input
                                        type="number"
                                        value={params.limite_peso_parada ?? ""}
                                        onChange={(e) => updateNumericParam("limite_peso_parada", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <div>
                                    <FieldLabel title="Peso máx. transferência" />
                                    <input
                                        type="number"
                                        value={params.peso_max_transferencia ?? ""}
                                        onChange={(e) => updateNumericParam("peso_max_transferencia", e.target.value)}
                                        className="input"
                                    />
                                </div>

                                <ToggleField
                                    title="Permitir rotas excedentes"
                                    hint="Permite rotas acima do tempo máximo"
                                    checked={params.permitir_rotas_excedentes ?? true}
                                    onChange={(checked) => updateParam("permitir_rotas_excedentes", checked)}
                                />

                                <ToggleField
                                    title="Permitir veículo leve intermunicipal"
                                    hint="Permite motos/utilitários atenderem cidades diferentes"
                                    checked={params.permitir_veiculo_leve_intermunicipal ?? false}
                                    onChange={(checked) =>
                                        updateParam("permitir_veiculo_leve_intermunicipal", checked)
                                    }
                                />
                            </Accordion>
                        </div>

                        <div className="space-y-5 xl:sticky xl:top-6 self-start">
                            <div className="rounded-[28px] border border-slate-200 bg-white p-4 shadow-[0_14px_34px_rgba(15,23,42,0.06)] lg:p-5">
                                <div className="flex items-center gap-2 text-sm font-semibold uppercase tracking-[0.14em] text-slate-500">
                                    <Play className="h-4 w-4 text-emerald-600" />
                                    Execução
                                </div>

                                <div className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-1">
                                    <div>
                                        <FieldLabel title="Data inicial" hint="primeiro dia do lote" />
                                        <input type="date" value={dataInicial} max={todayISO()} onChange={(e) => setDataInicial(e.target.value)} className="input" />
                                    </div>
                                    <div>
                                        <FieldLabel title="Data final" hint="opcional para processamento em faixa" />
                                        <input type="date" value={dataFinal} max={todayISO()} onChange={(e) => setDataFinal(e.target.value)} className="input" />
                                    </div>
                                </div>

                                <div className="mt-4">
                                    <FieldLabel title="Hub central" hint="origem do cenário Hub único e das transferências" />
                                    <select value={hubId ?? ""} onChange={(e) => setHubId(Number(e.target.value))} className="input" disabled={!hubs.length}>
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

                                <div className="mt-4 rounded-[24px] border border-slate-200 bg-slate-50 p-4">
                                    <label className="flex items-start gap-3 text-sm text-slate-700">
                                        <input
                                            type="checkbox"
                                            checked={params.modo_forcar ?? false}
                                            onChange={(e) => updateParam("modo_forcar", e.target.checked)}
                                            className="mt-1 h-4 w-4 rounded border-slate-300 text-emerald-600 focus:ring-emerald-500"
                                        />
                                        <span>
                                            <span className="flex items-center gap-2 font-medium text-slate-900">
                                                <span>Forçar sobrescrita</span>
                                                <HelpHint text="Remove resultados anteriores do mesmo período antes de rodar novamente." />
                                            </span>
                                        </span>
                                    </label>

                                    <button
                                        disabled={simulationInProgress || !datasValidas()}
                                        onClick={processar}
                                        className="btn mt-4 w-full gap-2 rounded-2xl px-5 py-3"
                                    >
                                        {simulationInProgress ? (
                                            <>
                                                <Loader2 className="h-4 w-4 animate-spin" />
                                                Processando
                                            </>
                                        ) : (
                                            <>
                                                <Play className="h-4 w-4" />
                                                Executar simulação
                                            </>
                                        )}
                                    </button>
                                </div>

                                <div className="mt-4 rounded-[24px] border border-slate-200 bg-slate-50 p-4">
                                    <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
                                        <FileText className="h-4 w-4 text-emerald-600" />
                                        Visualização
                                    </div>

                                    <div className="mt-3 rounded-2xl border border-slate-200 bg-white p-4">
                                        <FieldLabel title="Data para visualizar" hint="abre relatório, gráficos e mapas já gerados" />
                                        <input type="date" value={dataVisualizar} max={todayISO()} onChange={(e) => setDataVisualizar(e.target.value)} className="input" />
                                    </div>

                                    <button
                                        disabled={!dataVisualizar}
                                        onClick={gerarRelatorios}
                                        className="btn-secondary mt-3 w-full gap-2 rounded-2xl px-5 py-3"
                                    >
                                        <FileText className="h-4 w-4" />
                                        Abrir artefatos
                                    </button>
                                </div>
                            </div>

                            {artefatos ? (
                                <div className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-[0_10px_30px_rgba(15,23,42,0.05)]">
                                    <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
                                        <Gauge className="h-4 w-4 text-emerald-600" />
                                        Artefatos {artefatos.data}
                                    </div>

                                    {/* Prioriza cenário vencedor (ótimo) */}
                                    {(() => {
                                        const entries = Object.entries(artefatos.cenarios || {});
                                        const otimoEntry = entries.find(([_, v]: any) => v.otimo);
                                        if (!otimoEntry) return null;
                                        const [k, itens]: any = otimoEntry;
                                        // Exibe todos os mapas HTML do cenário ótimo, identificando o tipo
                                        const mapasHtml = (itens.mapas || []).filter((m: string) => m.endsWith('.html'));
                                        const mapaLabels = (m: string) => {
                                            if (m.includes('clusterizacao')) return 'Mapa de Clusterização';
                                            if (m.includes('transfer')) return 'Mapa de Transferências';
                                            if (m.includes('lastmile')) return 'Mapa de Roteirização';
                                            return 'Mapa';
                                        };
                                        return (
                                            <div className="rounded-2xl border-2 border-emerald-400 bg-emerald-50/40 p-4 mt-5 mb-8">
                                                <div className="flex items-center gap-3 mb-2">
                                                    <div className="text-base font-bold text-emerald-900">Cenário vencedor: {formatScenarioLabel(k)}</div>
                                                    <span className="rounded-full bg-emerald-100 px-3 py-1 text-xs font-semibold text-emerald-700">ótimo</span>
                                                </div>
                                                <div className="flex flex-wrap gap-3 mb-3">
                                                    {artefatos.relatorio_pdf && (
                                                        <a href={resolveUrl(artefatos.relatorio_pdf)} target="_blank" rel="noopener noreferrer" className="btn gap-2 rounded-xl">
                                                            <FileText className="h-4 w-4" /> Baixar relatório PDF
                                                        </a>
                                                    )}
                                                    {artefatos.graficos && artefatos.graficos.length > 0 && (
                                                        <a href={resolveUrl(artefatos.graficos.find((g) => g.includes(`grafico_simulacao_${artefatos.data}`)) || artefatos.graficos[0])} target="_blank" rel="noopener noreferrer" className="btn gap-2 rounded-xl">
                                                            <BarChart3 className="h-4 w-4" /> Baixar gráfico de custo
                                                        </a>
                                                    )}
                                                    {/* Botão para baixar Excel de entregas e rotas (agora como arquivo estático) */}
                                                    {artefatos.excel_entregas_rotas && (
                                                        <a
                                                            href={resolveUrl(artefatos.excel_entregas_rotas)}
                                                            target="_blank"
                                                            rel="noopener noreferrer"
                                                            className="btn gap-2 rounded-xl"
                                                        >
                                                            <FileText className="h-4 w-4" /> Baixar Excel entregas+rotas
                                                        </a>
                                                    )}
                                                    {mapasHtml.map((m: string) => (
                                                        <a key={m} href={resolveUrl(m)} target="_blank" rel="noopener noreferrer" className="btn gap-2 rounded-xl">
                                                            <Map className="h-4 w-4" /> {mapaLabels(m)}
                                                        </a>
                                                    ))}
                                                </div>
                                                <div className="grid gap-2 text-sm text-slate-700">
                                                    {itens.tabelas_lastmile?.length ? <a href={resolveUrl(itens.tabelas_lastmile[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Abrir tabela last-mile</a> : null}
                                                    {itens.tabelas_transferencias?.length ? <a href={resolveUrl(itens.tabelas_transferencias[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Abrir tabela de transferências</a> : null}
                                                    {itens.tabelas_resumo?.length ? <a href={resolveUrl(itens.tabelas_resumo[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Baixar CSV resumo</a> : null}
                                                    {itens.tabelas_detalhes?.length ? <a href={resolveUrl(itens.tabelas_detalhes[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Baixar CSV detalhes</a> : null}
                                                </div>
                                                {mapasHtml.length > 0 && (
                                                    <div className="mt-4 overflow-hidden rounded-2xl border border-slate-200 bg-white">
                                                        <iframe src={resolveUrl(mapasHtml[0])} title={`Cenário ótimo ${k}`} className="h-[420px] w-full" />
                                                    </div>
                                                )}
                                            </div>
                                        );
                                    })()}

                                    {/* Demais cenários */}
                                    <div className="mt-2 space-y-4">
                                        {Object.entries(artefatos.cenarios)
                                            .filter(([_, v]: any) => !v.otimo)
                                            .map(([k, itens]: any) => {
                                                // Exibe todos os mapas HTML do cenário, identificando o tipo
                                                const mapasHtml = (itens.mapas || []).filter((m: string) => m.endsWith('.html'));
                                                const mapaLabels = (m: string) => {
                                                    if (m.includes('clusterizacao')) return 'Mapa de Clusterização';
                                                    if (m.includes('transfer')) return 'Mapa de Transferências';
                                                    if (m.includes('lastmile')) return 'Mapa de Roteirização';
                                                    return 'Mapa';
                                                };
                                                return (
                                                    <div key={k} className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                                                        <div className="flex items-center gap-3 mb-2">
                                                            <div className="text-base font-semibold text-slate-900">{formatScenarioLabel(k)}</div>
                                                        </div>
                                                        <div className="flex flex-wrap gap-3 mb-3">
                                                            {mapasHtml.map((m: string) => (
                                                                <a key={m} href={resolveUrl(m)} target="_blank" rel="noopener noreferrer" className="btn gap-2 rounded-xl">
                                                                    <Map className="h-4 w-4" /> {mapaLabels(m)}
                                                                </a>
                                                            ))}
                                                        </div>
                                                        <div className="grid gap-2 text-sm text-slate-700">
                                                            {itens.tabelas_lastmile?.length ? <a href={resolveUrl(itens.tabelas_lastmile[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Abrir tabela last-mile</a> : null}
                                                            {itens.tabelas_transferencias?.length ? <a href={resolveUrl(itens.tabelas_transferencias[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Abrir tabela de transferências</a> : null}
                                                            {itens.tabelas_resumo?.length ? <a href={resolveUrl(itens.tabelas_resumo[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Baixar CSV resumo</a> : null}
                                                            {itens.tabelas_detalhes?.length ? <a href={resolveUrl(itens.tabelas_detalhes[0])} target="_blank" rel="noopener noreferrer" className="text-emerald-700 hover:underline">Baixar CSV detalhes</a> : null}
                                                        </div>
                                                    </div>
                                                );
                                            })}
                                    </div>
                                </div>
                            ) : null}

                            <div className="rounded-[28px] border border-slate-200 bg-white p-5 shadow-[0_10px_30px_rgba(15,23,42,0.05)]">
                                <div className="flex items-center justify-between gap-3">
                                    <div>
                                        <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
                                            <History className="h-4 w-4 text-emerald-600" />
                                            Histórico recente
                                        </div>
                                        <p className="mt-1 text-sm text-slate-500">Acompanhe os últimos jobs.</p>
                                    </div>
                                    <button onClick={() => carregarHistorico()} disabled={loadingHistorico} className="btn-secondary gap-2 rounded-xl px-3 py-2 text-sm">
                                        {loadingHistorico ? <Loader2 className="h-4 w-4 animate-spin" /> : <TimerReset className="h-4 w-4" />}
                                        Atualizar
                                    </button>
                                </div>

                                <div className="mt-4 space-y-3">
                                    {historico.length > 0 ? (
                                        historico.map((h) => (
                                            <div key={h.id} className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4">
                                                <div className="flex flex-wrap items-start justify-between gap-3">
                                                    <div>
                                                        <div className="text-sm font-medium text-slate-900">
                                                            {new Date(h.criado_em).toLocaleString("pt-BR", { dateStyle: "short", timeStyle: "short" })}
                                                        </div>
                                                        <div className="mt-1 text-xs font-mono text-slate-500" title={h.job_id}>{h.job_id}</div>
                                                    </div>
                                                    <span className={`rounded-full px-3 py-1 text-xs font-semibold ${h.status === "finished" ? "bg-emerald-100 text-emerald-700" : h.status === "failed" ? "bg-rose-100 text-rose-700" : "bg-amber-100 text-amber-700"}`}>
                                                        {h.status}
                                                    </span>
                                                </div>
                                                <p className="mt-3 line-clamp-2 text-sm text-slate-600" title={h.mensagem}>{h.mensagem}</p>
                                                <div className="mt-3 flex justify-end">
                                                    <button onClick={() => setPendingJobId(h.job_id)} className="btn-secondary gap-2 rounded-xl px-3 py-2 text-sm">
                                                        <TimerReset className="h-4 w-4" />
                                                        Ver status
                                                    </button>
                                                </div>
                                            </div>
                                        ))
                                    ) : (
                                        <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 p-5 text-sm text-slate-500">
                                            Nenhum histórico encontrado.
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                </>
            )
            }

            {/* ===================== ABA: Distribuição de k ===================== */}
            {
                activeTab === "cenarioVencedor" && (
                    <div className="bg-white rounded-2xl shadow p-4">
                        <h2 className="font-semibold mb-4 flex items-center gap-2">
                            <BarChart2 className="w-5 h-5 text-emerald-600" />
                            Distribuição de k (ponto ótimo) em um período
                        </h2>

                        {/* Filtros */}
                        <div className="grid md:grid-cols-3 gap-4 mb-4">
                            <div>
                                <FieldLabel title="Data inicial" hint="início do período analisado" />
                                <input
                                    type="date"
                                    value={periodoDistribuicaoDefault.data_inicial}
                                    max={todayISO()}
                                    onChange={(e) => setDataInicial(e.target.value)}
                                    className="input"
                                />
                            </div>
                            <div>
                                <FieldLabel title="Data final" hint="fim do período analisado" />
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
                            <div className="text-sm text-gray-500">{distKEmptyMessage || "Defina o período para carregar a distribuição."}</div>
                        )}

                        {distKData.length > 0 && (
                            <>
                                <div className="w-full h-[420px] border rounded-lg p-2">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <RBarChart data={distKData}>
                                            <CartesianGrid strokeDasharray="3 3" />
                                            <XAxis dataKey="k_clusters" tickMargin={8} tickFormatter={formatKLabel}>
                                                <Label
                                                    value="Cenários"
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
                )
            }

            {/* ===================== ABA: Frequência de Cidades ===================== */}
            {
                activeTab === "centroVencedor" && (
                    <div className="bg-white rounded-2xl shadow p-4">
                        <h2 className="font-semibold mb-4 flex items-center gap-2">
                            <Building2 className="w-5 h-5 text-emerald-600" />
                            {freqCidadesContexto?.somente_hub_unico
                                ? "Hub vencedor no período"
                                : "Frequência das cidades centro nos pontos ótimos"}
                        </h2>

                        {/* Filtros */}
                        <div className="grid md:grid-cols-3 gap-4 mb-4">
                            <div>
                                <FieldLabel title="Data inicial" hint="início do período analisado" />
                                <input
                                    type="date"
                                    value={periodoCidadesDefault.data_inicial}
                                    max={todayISO()}
                                    onChange={(e) => setDataInicial(e.target.value)}
                                    className="input"
                                />
                            </div>
                            <div>
                                <FieldLabel title="Data final" hint="fim do período analisado" />
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
                            <div className="text-sm text-gray-500">{freqCidadesEmptyMessage || "Defina o período para carregar as cidades."}</div>
                        )}

                        {freqCidadesData.length > 0 && (
                            <>
                                {freqCidadesContexto && (
                                    <div className="mb-4 rounded-2xl border border-emerald-100 bg-emerald-50 px-4 py-3 text-sm text-emerald-900">
                                        {freqCidadesContexto.somente_hub_unico
                                            ? `Os pontos ótimos do período foram Hub único em ${freqCidadesContexto.dias_hub_unico}/${freqCidadesContexto.total_dias_otimos} dias. O gráfico abaixo representa o hub vencedor${freqCidadesContexto.hub_cidade ? `: ${freqCidadesContexto.hub_cidade}` : ""}.`
                                            : freqCidadesContexto.dias_hub_unico > 0
                                                ? `O período mistura ${freqCidadesContexto.dias_hub_unico} dia(s) de Hub único e ${freqCidadesContexto.dias_clusterizados} dia(s) com clusterização vencedora.`
                                                : `O período possui ${freqCidadesContexto.dias_clusterizados} dia(s) com clusterização vencedora.`}
                                    </div>
                                )}

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

                                {/* Tabela Top 3 */}
                                <div className="overflow-auto mt-4">
                                    <table className="min-w-full text-sm">
                                        <thead>
                                            <tr className="text-left border-b">
                                                <th className="py-2 pr-4 w-12">#</th>
                                                <th className="py-2 pr-4">{freqCidadesContexto?.somente_hub_unico ? "Hub" : "Cidade"}</th>
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
                )
            }


            {/* ===================== ABA: comparativo de custos por cenário =====================*/}
            {
                activeTab === "custos" && (
                    <div className="bg-white rounded-2xl shadow p-4">
                        <h2 className="font-semibold mb-4 flex items-center gap-2">
                            <BarChart3 className="w-5 h-5 text-emerald-600" />
                            Comparativo de custos por cenário
                        </h2>

                        <div className="grid md:grid-cols-4 gap-4 mb-4">
                            <div>
                                <FieldLabel title="Data inicial" hint="início do período consolidado" />
                                <input
                                    type="date"
                                    value={periodoKFixoDefault.data_inicial}
                                    max={todayISO()}
                                    onChange={(e) => setDataInicial(e.target.value)}
                                    className="input"
                                />
                            </div>
                            <div>
                                <FieldLabel title="Data final" hint="fim do período consolidado" />
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
                                <FieldLabel title="Cobertura mínima (%)" hint="percentual mínimo de dias válidos exigido para considerar um k" />
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
                            <div className="text-sm text-gray-500">{kFixoEmptyMessage || "Defina o período para carregar os custos."}</div>
                        )}

                        {kFixoData.length > 0 && (
                            <>
                                <div className="w-full h-[420px] border rounded-lg p-2 mb-4 bg-white">
                                    <ResponsiveContainer width="100%" height="100%">
                                        <RBarChart data={kFixoData}>
                                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                                            <XAxis dataKey="k_clusters" tickMargin={8} tickFormatter={formatKLabel}>
                                                <Label value="Cenários" offset={-5} position="insideBottom" />
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
                                                labelFormatter={(k) => `Cenário ${formatKLabel(String(k))}`}
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
                                                        <td className="py-2 pr-4">{formatKLabel(r.k_clusters)}</td>
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
                )
            }

            {/* ===================== ABA: Frota p/ k Fixo ===================== */}
            {
                activeTab === "frota" && (
                    <div className="bg-white rounded-2xl shadow p-4">
                        <h2 className="font-semibold mb-4 flex items-center gap-2">
                            Frota média sugerida por k fixo
                        </h2>

                        {/* Filtros */}
                        <div className="grid md:grid-cols-4 gap-4 mb-4">
                            <div>
                                <FieldLabel title="Data inicial" hint="início do período da frota" />
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
                                <FieldLabel title="Data final" hint="fim do período da frota" />
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
                                <FieldLabel title="Valor de k" hint="use 0 para Hub único; informe outro número para um cenário clusterizado específico" />
                                <input
                                    type="number"
                                    value={frotaK ?? ""}
                                    onChange={(e) => setFrotaK(Number(e.target.value))}
                                    className="input"
                                    placeholder="Ex.: 0 ou 8"
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
                            <div className="text-sm text-gray-500">{frotaEmptyMessage || "Defina período e k para carregar a frota."}</div>
                        )}

                        {frotaResumo?.mensagem_transfer && (
                            <div className="mb-4 rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                                {frotaResumo.mensagem_transfer}
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

                        {!frotaTransfer.length && frotaResumo?.transfer_aplicavel && !loadingFrota && (
                            <div className="text-sm text-gray-500">Nenhuma frota de transferências encontrada para o período e k informado.</div>
                        )}
                    </div>
                )
            }
        </div >
    );
}
