// src/pages/Clusterization/ClusterizationPage.tsx
import { useEffect, useRef, useState, type ReactNode } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { CalendarDays, ChevronDown, ChevronLeft, ChevronRight, Loader2, Network, PlayCircle, FileText, Map, Search, X, CheckCircle2, Circle, AlertCircle } from "lucide-react";
import { listClusterizationHubs, type ClusterizationHub } from "@/services/clusterizationApi";
import { MapContainer, TileLayer, CircleMarker, Popup, Marker } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";

// ─── Paleta de cores por cluster ───────────────────────────────────────────
const CLUSTER_COLORS = [
    "#2563eb", "#16a34a", "#dc2626", "#d97706", "#7c3aed",
    "#0891b2", "#be185d", "#65a30d", "#ea580c", "#0d9488",
    "#6d28d9", "#b45309", "#047857", "#1d4ed8", "#c026d3",
];
function clusterColor(clusterId: any): string {
    if (String(clusterId) === "9999") return "#6b7280";
    const n = typeof clusterId === "number" ? clusterId : parseInt(String(clusterId), 10);
    return CLUSTER_COLORS[Math.abs(n) % CLUSTER_COLORS.length];
}

type ClusterSummary = {
    cluster: number | string;
    cluster_cidade: string | null;
    centro_lat: number;
    centro_lon: number;
    quantidade_entregas: number;
    peso_total_kg: number;
    quantidade_volumes: number;
    cte_valor_nf_total: number;
    cte_valor_frete_total: number;
};

type ResultadoJSON = {
    data: string;
    total_clusters: number;
    total_entregas: number;
    clusters: ClusterSummary[];
    pontos: { cluster: number | string; lat: number; lon: number }[];
};

// ─── Ícone estrela para centro de cluster ──────────────────────────────────
function centerIcon(color: string) {
    return L.divIcon({
        className: "",
        html: `<div style="width:18px;height:18px;border-radius:50%;background:${color};border:3px solid white;box-shadow:0 0 4px rgba(0,0,0,.4)"></div>`,
        iconSize: [18, 18],
        iconAnchor: [9, 9],
    });
}

// ─── Mapa react-leaflet ────────────────────────────────────────────────────
function ClusterMap({ resultado }: { resultado: ResultadoJSON }) {
    const center: [number, number] = resultado.clusters.length > 0
        ? [resultado.clusters[0].centro_lat, resultado.clusters[0].centro_lon]
        : [-5, -39];

    return (
        <MapContainer center={center} zoom={7} style={{ height: 420, width: "100%" }} scrollWheelZoom={false}>
            <TileLayer
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            />
            {resultado.pontos.map((p, i) => (
                <CircleMarker
                    key={i}
                    center={[p.lat, p.lon]}
                    radius={4}
                    pathOptions={{ color: clusterColor(p.cluster), fillColor: clusterColor(p.cluster), fillOpacity: 0.7, weight: 0 }}
                />
            ))}
            {resultado.clusters.map((c) => (
                <Marker key={`centro-${c.cluster}`} position={[c.centro_lat, c.centro_lon]} icon={centerIcon(clusterColor(c.cluster))}>
                    <Popup>
                        <strong>{c.cluster_cidade || `Cluster ${c.cluster}`}</strong><br />
                        {c.quantidade_entregas} entregas · {c.peso_total_kg?.toFixed(0)} kg
                    </Popup>
                </Marker>
            ))}
        </MapContainer>
    );
}

// ─── Tabela de clusters ────────────────────────────────────────────────────
function TabelaClusters({ clusters }: { clusters: ClusterSummary[] }) {
    const brl = (v: number) => v?.toLocaleString("pt-BR", { style: "currency", currency: "BRL", maximumFractionDigits: 0 });
    return (
        <div className="overflow-x-auto rounded-lg border">
            <table className="w-full text-sm">
                <thead className="bg-slate-50 text-xs text-slate-600 uppercase">
                    <tr>
                        <th className="px-3 py-2 text-left">Cluster</th>
                        <th className="px-3 py-2 text-left">Cidade centro</th>
                        <th className="px-3 py-2 text-right">Entregas</th>
                        <th className="px-3 py-2 text-right">Peso (kg)</th>
                        <th className="px-3 py-2 text-right">Volumes</th>
                        <th className="px-3 py-2 text-right">Valor NF</th>
                        <th className="px-3 py-2 text-right">Receita frete</th>
                    </tr>
                </thead>
                <tbody>
                    {clusters.map((c) => (
                        <tr key={c.cluster} className="border-t hover:bg-slate-50">
                            <td className="px-3 py-2">
                                <span className="inline-flex items-center gap-1.5">
                                    <span className="w-3 h-3 rounded-full flex-shrink-0" style={{ background: clusterColor(c.cluster) }} />
                                    {String(c.cluster) === "9999" ? "HUB" : `#${c.cluster}`}
                                </span>
                            </td>
                            <td className="px-3 py-2 text-slate-600">{c.cluster_cidade || "—"}</td>
                            <td className="px-3 py-2 text-right font-medium">{c.quantidade_entregas}</td>
                            <td className="px-3 py-2 text-right">{c.peso_total_kg?.toLocaleString("pt-BR", { maximumFractionDigits: 0 })}</td>
                            <td className="px-3 py-2 text-right">{c.quantidade_volumes}</td>
                            <td className="px-3 py-2 text-right">{brl(c.cte_valor_nf_total)}</td>
                            <td className="px-3 py-2 text-right">{brl(c.cte_valor_frete_total)}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

// ─── Stepper de progresso ──────────────────────────────────────────────────
const STEPS = ["Buscando dados", "Clusterizando", "Ajustando centros", "Salvando", "Concluído"];
function stepIndex(stepText: string): number {
    const t = (stepText || "").toLowerCase();
    if (t.includes("busca") || t.includes("conecta") || t.includes("enfilei")) return 0;
    if (t.includes("cluster")) return 1;
    if (t.includes("ajusta") || t.includes("centro")) return 2;
    if (t.includes("salva") || t.includes("finaliz")) return 3;
    if (t.includes("concluí") || t.includes("conclui")) return 4;
    return 0;
}

function Stepper({ step, isError }: { step: string; isError: boolean }) {
    const current = isError ? -1 : stepIndex(step);
    return (
        <ol className="flex items-center gap-0 w-full mt-3">
            {STEPS.map((label, i) => {
                const done = !isError && i < current;
                const active = !isError && i === current;
                return (
                    <li key={label} className="flex-1 flex flex-col items-center relative">
                        {i < STEPS.length - 1 && (
                            <div className={`absolute top-3 left-1/2 w-full h-0.5 ${done || active ? "bg-emerald-500" : "bg-slate-200"}`} />
                        )}
                        <div className={`relative z-10 w-6 h-6 rounded-full flex items-center justify-center text-xs
                            ${isError ? "bg-red-100 text-red-500" :
                                done ? "bg-emerald-500 text-white" :
                                    active ? "bg-emerald-600 text-white ring-4 ring-emerald-100" :
                                        "bg-slate-200 text-slate-400"}`}>
                            {isError ? <AlertCircle className="w-3.5 h-3.5" /> :
                                done ? <CheckCircle2 className="w-3.5 h-3.5" /> :
                                    active ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> :
                                        <Circle className="w-3.5 h-3.5" />}
                        </div>
                        <span className={`mt-1 text-[10px] text-center leading-tight hidden sm:block
                            ${done ? "text-emerald-600" : active ? "text-emerald-700 font-medium" : "text-slate-400"}`}>
                            {label}
                        </span>
                    </li>
                );
            })}
        </ol>
    );
}

function MultiSelectCentros({
    options,
    selected,
    onChange,
    loading,
}: {
    options: ClusterizationHub[];
    selected: number[];
    onChange: (ids: number[]) => void;
    loading: boolean;
}) {
    const [open, setOpen] = useState(false);
    const ref = useRef<HTMLDivElement>(null);

    useEffect(() => {
        function handleClick(e: MouseEvent) {
            if (ref.current && !ref.current.contains(e.target as Node)) {
                setOpen(false);
            }
        }
        document.addEventListener("mousedown", handleClick);
        return () => document.removeEventListener("mousedown", handleClick);
    }, []);

    const toggle = (id: number) => {
        onChange(selected.includes(id) ? selected.filter((c) => c !== id) : [...selected, id]);
    };

    const label =
        selected.length === 0
            ? "Selecione os centros pré-definidos"
            : selected.length === options.length
                ? "Todos os centros selecionados"
                : `${selected.length} centro${selected.length > 1 ? "s" : ""} selecionado${selected.length > 1 ? "s" : ""}`;

    return (
        <div ref={ref} className="relative">
            <button
                type="button"
                onClick={() => setOpen((v) => !v)}
                disabled={loading}
                className="w-full flex items-center justify-between border rounded px-3 py-2 bg-white text-sm disabled:opacity-60 hover:border-emerald-400 transition"
            >
                <span className={selected.length === 0 ? "text-gray-400" : "text-gray-800"}>{label}</span>
                <ChevronDown className={`w-4 h-4 text-gray-400 transition-transform ${open ? "rotate-180" : ""}`} />
            </button>

            {selected.length > 0 && (
                <div className="flex flex-wrap gap-1 mt-1.5">
                    {selected.map((id) => {
                        const hub = options.find((h) => h.id === id);
                        return hub ? (
                            <span
                                key={id}
                                className="inline-flex items-center gap-1 rounded-full bg-emerald-100 text-emerald-800 text-xs px-2 py-0.5"
                            >
                                {hub.nome}
                                <button
                                    type="button"
                                    onClick={() => toggle(id)}
                                    className="hover:text-emerald-600"
                                >
                                    ✕
                                </button>
                            </span>
                        ) : null;
                    })}
                </div>
            )}

            {open && (
                <div className="absolute z-50 mt-1 w-full rounded-lg border bg-white shadow-lg max-h-64 overflow-y-auto">
                    {loading && (
                        <p className="px-4 py-3 text-sm text-gray-500 flex items-center gap-2">
                            <Loader2 className="w-4 h-4 animate-spin" /> Carregando...
                        </p>
                    )}
                    {!loading && options.length === 0 && (
                        <p className="px-4 py-3 text-sm text-gray-500">
                            Nenhum hub marcado como Centro de Cluster ativo.
                        </p>
                    )}
                    {!loading && options.length > 0 && (
                        <>
                            <label className="flex items-center gap-3 px-4 py-2 border-b hover:bg-slate-50 cursor-pointer text-sm font-medium">
                                <input
                                    type="checkbox"
                                    checked={selected.length === options.length}
                                    onChange={() =>
                                        onChange(selected.length === options.length ? [] : options.map((h) => h.id))
                                    }
                                    className="w-4 h-4 accent-emerald-600"
                                />
                                Selecionar todos
                            </label>
                            {options.map((hub) => (
                                <label
                                    key={hub.id}
                                    className="flex items-center gap-3 px-4 py-2 hover:bg-slate-50 cursor-pointer"
                                >
                                    <input
                                        type="checkbox"
                                        checked={selected.includes(hub.id)}
                                        onChange={() => toggle(hub.id)}
                                        className="w-4 h-4 accent-emerald-600"
                                    />
                                    <span className="text-sm">
                                        <span className="font-medium">{hub.nome}</span>
                                        <span className="text-gray-500 ml-1">— {hub.endereco}</span>
                                    </span>
                                </label>
                            ))}
                        </>
                    )}
                </div>
            )}
        </div>
    );
}

type DataDisponivel = {
    data: string;
    quantidade_entregas: number;
};

type JobState = {
    job_id: string;
    status: "processing" | "done" | "error";
    progress: number;
    step: string;
    error?: string;
};

const POLLING_INTERVAL_MS = 700;
const MIN_PROGRESS_VISIBLE_MS = 1800;
const DATAS_PAGE_SIZE = 5;

function normalizeErrorDetail(value: any): string | undefined {
    if (!value) {
        return undefined;
    }
    if (typeof value === "string") {
        return value;
    }
    if (value.detail) {
        return normalizeErrorDetail(value.detail);
    }
    if (value.message) {
        return normalizeErrorDetail(value.message);
    }
    return undefined;
}

function getErrorMessage(err: any) {
    return normalizeErrorDetail(err.response?.data?.detail) || err.message;
}

function HelpHint({ text }: { text: string }) {
    return (
        <span className="group relative inline-flex align-middle">
            <span className="inline-flex h-4.5 w-4.5 cursor-help items-center justify-center rounded-full border border-slate-300 bg-white text-[10px] font-semibold text-slate-500 transition hover:border-emerald-300 hover:text-emerald-700">
                ?
            </span>
            <span className="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-56 -translate-x-1/2 rounded-2xl border border-slate-200 bg-slate-900 px-3 py-2 text-xs font-medium leading-5 text-white shadow-xl group-hover:block">
                {text}
            </span>
        </span>
    );
}

function FieldLabel({ title, hint }: { title: string; hint?: string }) {
    return (
        <div className="mb-1 flex items-center gap-2 text-xs font-medium text-slate-600">
            <span>{title}</span>
            {hint ? <HelpHint text={hint} /> : null}
        </div>
    );
}

function FieldHelpText({ children }: { children: ReactNode }) {
    return <p className="mt-1 text-xs leading-5 text-slate-500">{children}</p>;
}

export default function ClusterizationPage() {
    const [data, setData] = useState(""); // apenas uma data
    const [modo, setModo] = useState<"automatico" | "predefinido">("automatico");
    const [minEntregasClusterAlvo, setMinEntregasClusterAlvo] = useState(30);
    const [maxEntregasClusterAlvo, setMaxEntregasClusterAlvo] = useState(120);
    const [hubCentralId, setHubCentralId] = useState("");
    const [raioHub, setRaioHub] = useState(80.0);
    const [centrosSelecionados, setCentrosSelecionados] = useState<number[]>([]);

    const [loading, setLoading] = useState(false);
    const [resultadoJSON, setResultadoJSON] = useState<ResultadoJSON | null>(null);
    const [resultadoJSONLoading, setResultadoJSONLoading] = useState(false);
    const [viz, setViz] = useState<any>(null);
    const [vizLoading, setVizLoading] = useState(false);
    const [datasDisponiveis, setDatasDisponiveis] = useState<DataDisponivel[]>([]);
    const [datasLoading, setDatasLoading] = useState(false);
    const [datasOffset, setDatasOffset] = useState(0);
    const [datasHasMore, setDatasHasMore] = useState(false);
    const [dataInicioFiltro, setDataInicioFiltro] = useState("");
    const [dataFimFiltro, setDataFimFiltro] = useState("");
    const [hubsCentrais, setHubsCentrais] = useState<ClusterizationHub[]>([]);
    const [hubsCentroCluster, setHubsCentroCluster] = useState<ClusterizationHub[]>([]);
    const [hubsLoading, setHubsLoading] = useState(false);
    const [jobState, setJobState] = useState<JobState | null>(null);
    const [jobStartedAt, setJobStartedAt] = useState<number | null>(null);

    const carregarDatasDisponiveis = async (offset = 0, usarFiltros = true) => {
        setDatasLoading(true);
        try {
            const params: Record<string, string | number> = {
                limit: DATAS_PAGE_SIZE,
                offset,
            };
            if (usarFiltros && dataInicioFiltro) {
                params.data_inicio = dataInicioFiltro;
            }
            if (usarFiltros && dataFimFiltro) {
                params.data_fim = dataFimFiltro;
            }

            const res = await api.get("/clusterization/datas-disponiveis", { params });
            const datas = res.data.datas || [];

            setDatasDisponiveis(datas);
            setDatasOffset(offset);
            setDatasHasMore(Boolean(res.data.pagination?.has_more));

            if (datas.length > 0 && (!data || !datas.some((item: DataDisponivel) => item.data === data))) {
                setData(datas[0].data);
            }
            if (datas.length === 0) {
                setData("");
            }
        } catch (err: any) {
            toast.error("Não foi possível carregar datas disponíveis: " + getErrorMessage(err));
        } finally {
            setDatasLoading(false);
        }
    };

    const limparFiltrosDatas = () => {
        setDataInicioFiltro("");
        setDataFimFiltro("");
        carregarDatasDisponiveis(0, false);
    };

    useEffect(() => {
        const carregarDadosIniciais = async () => {
            setDatasLoading(true);
            setHubsLoading(true);
            try {
                const [datasRes, hubsRes] = await Promise.all([
                    api.get("/clusterization/datas-disponiveis", {
                        params: { limit: DATAS_PAGE_SIZE, offset: 0 },
                    }),
                    listClusterizationHubs(),
                ]);

                const datas = datasRes.data.datas || [];
                const hubs = hubsRes.filter((hub) => hub.ativo && hub.hub_central);
                const centros = hubsRes.filter((hub) => hub.ativo && hub.centro_cluster);

                setHubsCentrais(hubs);
                setHubsCentroCluster(centros);
                if (hubs.length > 0) {
                    setHubCentralId((hubAtual) => hubAtual || String(hubs[0].id));
                }

                setDatasDisponiveis(datas);
                setDatasOffset(0);
                setDatasHasMore(Boolean(datasRes.data.pagination?.has_more));
                if (datas.length > 0) {
                    setData((dataAtual) => dataAtual || datas[0].data);
                }
            } catch (err: any) {
                toast.error("Não foi possível carregar dados iniciais: " + getErrorMessage(err));
            } finally {
                setDatasLoading(false);
                setHubsLoading(false);
            }
        };

        carregarDadosIniciais();
    }, []);

    // 👉 Executar clusterização
    const executar = async () => {
        if (!data) {
            toast.error("Informe a data.");
            return;
        }
        if (modo === "automatico") {
            if (minEntregasClusterAlvo < 1 || maxEntregasClusterAlvo < 1) {
                toast.error("As quantidades de entregas por cluster devem ser maiores que zero.");
                return;
            }
            if (minEntregasClusterAlvo > maxEntregasClusterAlvo) {
                toast.error("O mínimo de entregas por cluster não pode ser maior que o máximo.");
                return;
            }
        }
        if (modo === "predefinido" && centrosSelecionados.length === 0) {
            toast.error("Selecione ao menos um centro pré-definido.");
            return;
        }
        if (!hubCentralId) {
            toast.error("Selecione o hub central.");
            return;
        }
        setLoading(true);
        setResultadoJSON(null);
        setViz(null);
        setJobState(null);
        setJobStartedAt(Date.now());

        try {
            const payload: Record<string, any> = {
                data: data,
                hub_central_id: Number(hubCentralId),
                raio_cluster_hub_central: raioHub,
                modo_clusterizacao: modo,
            };
            if (modo === "automatico") {
                payload.min_entregas_por_cluster_alvo = minEntregasClusterAlvo;
                payload.max_entregas_por_cluster_alvo = maxEntregasClusterAlvo;
            } else {
                payload.centros_ids = centrosSelecionados;
            }

            console.log("[Clusterization] POST /clusterization/jobs", payload);

            const res = await api.post("/clusterization/jobs", payload);

            setJobState({
                job_id: res.data.job_id,
                status: "processing",
                progress: Math.max(5, res.data.progress ?? 0),
                step: res.data.step ?? "Enfileirado",
            });
            toast.success("Clusterização enfileirada.");
        } catch (err: any) {
            toast.error(
                "Erro ao executar clusterização: " +
                getErrorMessage(err)
            );
            setLoading(false);
        }
    };

    const carregarResultadoJSON = async (dataVisualizacao: string) => {
        setResultadoJSONLoading(true);
        try {
            const res = await api.get("/clusterization/resultado", { params: { data: dataVisualizacao } });
            setResultadoJSON(res.data);
        } catch {
            // silencia — o resultado em arquivo ainda estará disponível
        } finally {
            setResultadoJSONLoading(false);
        }
    };

    const carregarVisualizacao = async (dataVisualizacao: string, automatico = false) => {
        if (!dataVisualizacao) {
            toast.error("Selecione uma data para visualizar.");
            return;
        }
        setVizLoading(true);
        setViz(null);
        try {
            const res = await api.get("/clusterization/visualizar", {
                params: { data: dataVisualizacao },
            });
            setViz(res.data);
            toast.success(automatico ? "Mapa gerado automaticamente." : "Visualização carregada!");
        } catch (err: any) {
            toast.error(
                "Erro ao visualizar clusterização: " +
                getErrorMessage(err)
            );
        } finally {
            setVizLoading(false);
        }
    };

    useEffect(() => {
        if (!jobState?.job_id || jobState.status !== "processing") return;

        let finishTimeout: ReturnType<typeof setTimeout> | null = null;

        const concluirJob = (result: any) => {
            setJobState({
                job_id: jobState.job_id,
                status: "done",
                progress: 100,
                step: "Concluído",
            });
            if (result?.datas?.length) {
                const primeiraData = result.datas[0];
                carregarResultadoJSON(primeiraData);
                carregarVisualizacao(primeiraData, true);
            }
            setLoading(false);
            toast.success("Clusterização concluída com sucesso!");
        };

        const interval = setInterval(async () => {
            try {
                const res = await api.get(`/clusterization/jobs/${jobState.job_id}`);
                const status = res.data.status;

                if (status === "done") {
                    const result = res.data.result;
                    setJobState({
                        job_id: jobState.job_id,
                        status: "processing",
                        progress: 98,
                        step: "Finalizando resultados...",
                    });
                    clearInterval(interval);
                    const elapsed = jobStartedAt ? Date.now() - jobStartedAt : MIN_PROGRESS_VISIBLE_MS;
                    const remaining = Math.max(250, MIN_PROGRESS_VISIBLE_MS - elapsed);
                    finishTimeout = setTimeout(() => concluirJob(result), remaining);
                    return;
                }

                if (status === "error") {
                    setJobState({
                        job_id: jobState.job_id,
                        status: "error",
                        progress: 100,
                        step: res.data.step || "Erro",
                        error: res.data.error,
                    });
                    setLoading(false);
                    toast.error(res.data.error || "Erro ao executar clusterização.");
                    clearInterval(interval);
                    return;
                }

                setJobState({
                    job_id: jobState.job_id,
                    status: "processing",
                    progress: Math.max(8, Math.min(95, res.data.progress ?? 0)),
                    step: res.data.step ?? "Processando",
                });
            } catch (err: any) {
                setJobState((prev) => prev ? { ...prev, step: "Aguardando atualização..." } : prev);
            }
        }, POLLING_INTERVAL_MS);

        return () => {
            clearInterval(interval);
            if (finishTimeout) {
                clearTimeout(finishTimeout);
            }
        };
    }, [jobState?.job_id, jobState?.status, jobStartedAt]);

    const buildExportUrl = (path: string) =>
        `${import.meta.env.VITE_API_URL}${path}`;

    return (
        <div className="px-6 py-4">
            <div className="bg-white shadow rounded-2xl p-5">
                <h2 className="text-lg font-bold mb-4 flex items-center gap-2">
                    <Network className="w-5 h-5 text-emerald-600" />
                    Clusterização
                </h2>
                <p className="mb-4 text-sm text-slate-500">
                    Defina como os clusters devem ser formados e qual hub central servirá como referência operacional.
                </p>

                {/* ── Layout 2 colunas ── */}
                <div className="grid grid-cols-1 lg:grid-cols-[1fr_360px] gap-4 items-start">

                    {/* Coluna esquerda — seletor de datas */}
                    <div className="rounded-lg border bg-slate-50 p-3">
                        <div className="flex items-center justify-between gap-2 mb-2">
                            <p className="text-sm font-medium flex items-center gap-1.5">
                                <CalendarDays className="w-4 h-4 text-emerald-600" />
                                Datas disponíveis
                            </p>
                            {data && (
                                <span className="text-xs text-slate-500 bg-emerald-50 border border-emerald-200 rounded px-2 py-0.5">
                                    {data}
                                </span>
                            )}
                        </div>

                        {/* Filtros de data */}
                        <div className="grid grid-cols-[1fr_1fr_auto_auto] gap-2 mb-2">
                            <input
                                type="date"
                                value={dataInicioFiltro}
                                onChange={(e) => setDataInicioFiltro(e.target.value)}
                                placeholder="De"
                                className="border rounded px-2 py-1.5 text-sm bg-white w-full"
                            />
                            <input
                                type="date"
                                value={dataFimFiltro}
                                onChange={(e) => setDataFimFiltro(e.target.value)}
                                placeholder="Até"
                                className="border rounded px-2 py-1.5 text-sm bg-white w-full"
                            />
                            <button
                                onClick={() => carregarDatasDisponiveis(0, true)}
                                disabled={datasLoading}
                                className="border rounded px-3 py-1.5 bg-white hover:bg-slate-100 disabled:opacity-60 flex items-center gap-1 text-sm"
                            >
                                {datasLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Search className="w-3.5 h-3.5" />}
                                Filtrar
                            </button>
                            <button
                                onClick={limparFiltrosDatas}
                                disabled={datasLoading || (!dataInicioFiltro && !dataFimFiltro && datasOffset === 0)}
                                className="border rounded px-2 py-1.5 bg-white hover:bg-slate-100 disabled:opacity-50"
                                title="Limpar filtros"
                            >
                                <X className="w-3.5 h-3.5" />
                            </button>
                        </div>

                        {/* Lista de datas — linhas compactas */}
                        <div className="grid gap-1">
                            {datasLoading && (
                                <p className="text-xs text-gray-400 flex items-center gap-1.5 py-2">
                                    <Loader2 className="w-3.5 h-3.5 animate-spin" /> Carregando...
                                </p>
                            )}
                            {!datasLoading && datasDisponiveis.length === 0 && (
                                <p className="text-sm text-gray-500 py-2">Nenhum input encontrado.</p>
                            )}
                            {!datasLoading && datasDisponiveis.map((item) => (
                                <button
                                    key={item.data}
                                    onClick={() => setData(item.data)}
                                    className={`flex items-center justify-between gap-2 rounded border px-3 py-2 text-left text-sm transition ${data === item.data
                                            ? "border-emerald-500 bg-emerald-50 text-emerald-800"
                                            : "bg-white hover:bg-slate-50 text-slate-700"
                                        }`}
                                >
                                    <span className="font-medium">{item.data}</span>
                                    <span className="text-xs text-slate-400">{item.quantidade_entregas} entregas</span>
                                </button>
                            ))}
                        </div>

                        {/* Paginação compacta */}
                        <div className="mt-2 flex items-center justify-between gap-2">
                            <button
                                onClick={() => carregarDatasDisponiveis(Math.max(0, datasOffset - DATAS_PAGE_SIZE), true)}
                                disabled={datasLoading || datasOffset === 0}
                                className="border rounded px-2 py-1 text-xs disabled:opacity-40 flex items-center gap-1 hover:bg-slate-100"
                            >
                                <ChevronLeft className="w-3.5 h-3.5" /> Anteriores
                            </button>
                            <span className="text-xs text-slate-400">
                                {datasOffset + 1}–{datasOffset + datasDisponiveis.length}
                            </span>
                            <button
                                onClick={() => carregarDatasDisponiveis(datasOffset + DATAS_PAGE_SIZE, true)}
                                disabled={datasLoading || !datasHasMore}
                                className="border rounded px-2 py-1 text-xs disabled:opacity-40 flex items-center gap-1 hover:bg-slate-100"
                            >
                                Próximas <ChevronRight className="w-3.5 h-3.5" />
                            </button>
                        </div>
                    </div>

                    {/* Coluna direita — parâmetros + botão */}
                    <div className="grid gap-3">

                        {/* Modo */}
                        <div className="rounded-lg border bg-slate-50 p-3">
                            <p className="text-xs font-medium text-slate-500 uppercase tracking-wide mb-2">Como definir os clusters</p>
                            <div className="grid grid-cols-2 gap-1.5">
                                {(["automatico", "predefinido"] as const).map((m) => (
                                    <button
                                        key={m}
                                        onClick={() => setModo(m)}
                                        className={`rounded-lg border py-2 text-sm font-medium transition ${modo === m
                                                ? "border-emerald-600 bg-emerald-50 text-emerald-700"
                                                : "bg-white hover:bg-slate-100 text-slate-600"
                                            }`}
                                    >
                                        {m === "automatico" ? "Automático" : "Centros pré-definidos"}
                                    </button>
                                ))}
                            </div>
                            <FieldHelpText>
                                Use o modo automático para deixar o sistema montar os clusters. Use centros pré-definidos quando você já souber quais bases deseja usar.
                            </FieldHelpText>
                        </div>

                        {/* Parâmetros do modo */}
                        {modo === "automatico" ? (
                            <div className="grid grid-cols-2 gap-2">
                                <label>
                                    <FieldLabel
                                        title="Entregas mínimas por cluster"
                                        hint="Quantidade mínima desejada de entregas em cada cluster formado automaticamente."
                                    />
                                    <input type="number" min={1} value={minEntregasClusterAlvo}
                                        onChange={(e) => setMinEntregasClusterAlvo(Number(e.target.value))}
                                        className="mt-1 border rounded px-2 py-1.5 text-sm w-full" />
                                    <FieldHelpText>Ajuda a evitar clusters muito pequenos.</FieldHelpText>
                                </label>
                                <label>
                                    <FieldLabel
                                        title="Entregas máximas por cluster"
                                        hint="Quantidade máxima desejada de entregas em cada cluster formado automaticamente."
                                    />
                                    <input type="number" min={minEntregasClusterAlvo} value={maxEntregasClusterAlvo}
                                        onChange={(e) => setMaxEntregasClusterAlvo(Number(e.target.value))}
                                        className="mt-1 border rounded px-2 py-1.5 text-sm w-full" />
                                    <FieldHelpText>Use esse limite para evitar clusters grandes demais para a operação.</FieldHelpText>
                                </label>
                            </div>
                        ) : (
                            <div>
                                <FieldLabel
                                    title="Centros pré-definidos"
                                    hint="Selecione os hubs que poderão atuar como centros dos clusters neste processamento."
                                />
                                <MultiSelectCentros
                                    options={hubsCentroCluster}
                                    selected={centrosSelecionados}
                                    onChange={setCentrosSelecionados}
                                    loading={hubsLoading}
                                />
                                <FieldHelpText>Esses centros serão usados como base fixa para distribuir as entregas.</FieldHelpText>
                                {!hubsLoading && hubsCentroCluster.length === 0 && (
                                    <p className="mt-1 text-xs text-red-600">
                                        Nenhum hub marcado como centro de cluster está ativo.
                                    </p>
                                )}
                            </div>
                        )}

                        {/* Hub Central */}
                        <label>
                            <FieldLabel
                                title="Hub central"
                                hint="Hub usado como referência principal para consolidar a operação e calcular o raio de influência."
                            />
                            <select value={hubCentralId} onChange={(e) => setHubCentralId(e.target.value)}
                                className="mt-1 border rounded px-2 py-1.5 text-sm w-full" disabled={hubsLoading}>
                                <option value="">{hubsLoading ? "Carregando..." : "Selecione o hub central"}</option>
                                {hubsCentrais.map((hub) => (
                                    <option key={hub.id} value={hub.id}>{hub.nome} — {hub.endereco}</option>
                                ))}
                            </select>
                            <FieldHelpText>Esse hub funciona como ponto principal de referência para a clusterização.</FieldHelpText>
                            {!hubsLoading && hubsCentrais.length === 0 && (
                                <p className="mt-1 text-xs text-red-600">Cadastre um hub como hub central.</p>
                            )}
                        </label>

                        {/* Raio */}
                        <label>
                            <FieldLabel
                                title="Raio de influência do hub (km)"
                                hint="Distância máxima para considerar entregas atendidas pelo agrupamento do hub central."
                            />
                            <input type="number" step="0.1" value={raioHub}
                                onChange={(e) => setRaioHub(Number(e.target.value))}
                                className="mt-1 border rounded px-2 py-1.5 text-sm w-full" />
                            <FieldHelpText>Quanto maior o raio, maior a área atendida diretamente pelo hub central.</FieldHelpText>
                        </label>

                        {/* Botão executar */}
                        <button
                            onClick={executar}
                            disabled={loading}
                            className="w-full bg-emerald-600 text-white rounded-lg px-4 py-2.5 hover:bg-emerald-700 disabled:opacity-60 flex items-center justify-center gap-2 font-medium"
                        >
                            {loading
                                ? <><Loader2 className="w-4 h-4 animate-spin" /> Processando...</>
                                : <><PlayCircle className="w-4 h-4" /> Iniciar clusterização</>}
                        </button>
                    </div>
                </div>

                {/* ── Stepper de progresso ── */}
                {jobState && (
                    <div className="mt-6 rounded-lg border bg-slate-50 p-4">
                        <div className="flex items-center justify-between">
                            <p className="font-medium text-sm">
                                {jobState.status === "error" ? "Erro na clusterização" : "Processando clusterização"}
                            </p>
                            <span className="text-xs text-slate-500">{jobState.progress}%</span>
                        </div>
                        <Stepper step={jobState.step} isError={jobState.status === "error"} />
                        {jobState.error && (
                            <p className="mt-3 text-sm text-red-600 bg-red-50 rounded p-2">{jobState.error}</p>
                        )}
                    </div>
                )}

                {/* ── Resultado inline: mapa + tabela ── */}
                {resultadoJSONLoading && (
                    <div className="mt-6 flex items-center gap-2 text-sm text-slate-500">
                        <Loader2 className="w-4 h-4 animate-spin" /> Carregando resultado...
                    </div>
                )}

                {resultadoJSON && (
                    <div className="mt-6 grid gap-4">
                        {/* KPIs */}
                        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                            {[
                                { label: "Data", value: resultadoJSON.data },
                                { label: "Clusters", value: resultadoJSON.total_clusters },
                                { label: "Entregas", value: resultadoJSON.total_entregas.toLocaleString("pt-BR") },
                                { label: "Média / cluster", value: Math.round(resultadoJSON.total_entregas / resultadoJSON.total_clusters).toLocaleString("pt-BR") },
                            ].map((kpi) => (
                                <div key={kpi.label} className="rounded-lg border bg-white p-3 text-center">
                                    <p className="text-xs text-slate-500 uppercase tracking-wide">{kpi.label}</p>
                                    <p className="text-xl font-bold text-slate-800 mt-0.5">{kpi.value}</p>
                                </div>
                            ))}
                        </div>

                        {/* Mapa */}
                        <div className="rounded-xl border overflow-hidden">
                            <ClusterMap resultado={resultadoJSON} />
                        </div>

                        {/* Tabela */}
                        <TabelaClusters clusters={resultadoJSON.clusters} />

                        {/* Downloads */}
                        {viz && (
                            <div className="rounded-lg border bg-slate-50 p-4">
                                <p className="text-sm font-medium mb-2 text-slate-700">Downloads</p>
                                <div className="flex flex-wrap gap-3">
                                    <a href={buildExportUrl(viz.arquivos.mapa_html)} target="_blank"
                                        className="flex items-center gap-1.5 text-sm text-emerald-700 hover:underline">
                                        <Map className="w-4 h-4" /> Mapa interativo
                                    </a>
                                    <a href={buildExportUrl(viz.arquivos.pdf)} target="_blank"
                                        className="flex items-center gap-1.5 text-sm text-emerald-700 hover:underline">
                                        <FileText className="w-4 h-4" /> Relatório PDF
                                    </a>
                                    {viz.arquivos.xlsx && (
                                        <a href={buildExportUrl(viz.arquivos.xlsx)} target="_blank"
                                            className="flex items-center gap-1.5 text-sm text-emerald-700 hover:underline">
                                            <FileText className="w-4 h-4" /> Planilha XLSX
                                        </a>
                                    )}
                                    {vizLoading && (
                                        <span className="text-xs text-slate-400 flex items-center gap-1">
                                            <Loader2 className="w-3 h-3 animate-spin" /> Gerando arquivos...
                                        </span>
                                    )}
                                </div>
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
