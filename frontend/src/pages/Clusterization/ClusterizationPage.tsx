// src/pages/Clusterization/ClusterizationPage.tsx
import { useEffect, useRef, useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import { CalendarDays, ChevronDown, ChevronLeft, ChevronRight, Loader2, Network, PlayCircle, FileText, Map, Search, X } from "lucide-react";
import { listClusterizationHubs, type ClusterizationHub } from "@/services/clusterizationApi";

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

export default function ClusterizationPage() {
    const [data, setData] = useState(""); // apenas uma data
    const [modo, setModo] = useState<"automatico" | "predefinido">("automatico");
    const [minEntregasClusterAlvo, setMinEntregasClusterAlvo] = useState(10);
    const [maxEntregasClusterAlvo, setMaxEntregasClusterAlvo] = useState(100);
    const [hubCentralId, setHubCentralId] = useState("");
    const [raioHub, setRaioHub] = useState(80.0);
    const [centrosSelecionados, setCentrosSelecionados] = useState<number[]>([]);

    const [loading, setLoading] = useState(false);
    const [resultado, setResultado] = useState<any>(null);
    const [vizData, setVizData] = useState("");
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
            toast.error("Selecione o Hub Central.");
            return;
        }
        setLoading(true);
        setResultado(null);
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
            setResultado(result);
            if (result?.datas?.length) {
                const primeiraData = result.datas[0];
                setVizData(primeiraData);
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
        <div className="p-6">
            <div className="max-w-4xl mx-auto bg-white shadow rounded-2xl p-6">
                <h2 className="text-xl font-bold mb-4 flex items-center gap-2">
                    <Network className="w-5 h-5 text-emerald-600" />
                    Clusterização
                </h2>

                {/* Formulário de parâmetros */}
                <div className="grid gap-3">
                    <div className="rounded-lg border bg-slate-50 p-4">
                        <div className="flex items-center justify-between gap-3">
                            <p className="font-medium flex items-center gap-2">
                                <CalendarDays className="w-4 h-4 text-emerald-600" />
                                Datas de input disponíveis
                            </p>
                            {data && (
                                <span className="text-sm text-slate-600">
                                    Selecionada: {data}
                                </span>
                            )}
                        </div>

                        <div className="mt-3 grid gap-3 md:grid-cols-[1fr_1fr_auto_auto]">
                            <label className="text-sm font-medium">
                                De:
                                <input
                                    type="date"
                                    value={dataInicioFiltro}
                                    onChange={(e) => setDataInicioFiltro(e.target.value)}
                                    className="border rounded px-3 py-2 w-full bg-white"
                                />
                            </label>
                            <label className="text-sm font-medium">
                                Até:
                                <input
                                    type="date"
                                    value={dataFimFiltro}
                                    onChange={(e) => setDataFimFiltro(e.target.value)}
                                    className="border rounded px-3 py-2 w-full bg-white"
                                />
                            </label>
                            <button
                                onClick={() => carregarDatasDisponiveis(0, true)}
                                disabled={datasLoading}
                                className="self-end bg-white border rounded-lg px-4 py-2 hover:bg-slate-100 disabled:opacity-60 flex items-center justify-center gap-2"
                            >
                                {datasLoading ? (
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                ) : (
                                    <Search className="w-4 h-4" />
                                )}
                                Filtrar
                            </button>
                            <button
                                onClick={limparFiltrosDatas}
                                disabled={datasLoading || (!dataInicioFiltro && !dataFimFiltro && datasOffset === 0)}
                                className="self-end bg-white border rounded-lg px-4 py-2 hover:bg-slate-100 disabled:opacity-50 flex items-center justify-center gap-2"
                            >
                                <X className="w-4 h-4" />
                                Limpar
                            </button>
                        </div>

                        <div className="mt-4 grid gap-2">
                            {datasLoading && (
                                <p className="text-sm text-gray-500 flex items-center gap-2">
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                    Carregando datas disponíveis...
                                </p>
                            )}
                            {!datasLoading && datasDisponiveis.length === 0 && (
                                <p className="text-sm text-gray-600">
                                    Nenhum input encontrado para os filtros informados.
                                </p>
                            )}
                            {!datasLoading && datasDisponiveis.map((item) => (
                                <button
                                    key={item.data}
                                    onClick={() => setData(item.data)}
                                    className={`flex items-center justify-between gap-3 rounded-lg border px-4 py-3 text-left transition ${data === item.data
                                        ? "border-emerald-600 bg-emerald-50"
                                        : "bg-white hover:bg-slate-50"
                                        }`}
                                >
                                    <span className="font-medium">{item.data}</span>
                                    <span className="text-sm text-slate-600">
                                        {item.quantidade_entregas} entregas
                                    </span>
                                </button>
                            ))}
                        </div>

                        <div className="mt-3 flex items-center justify-between gap-3">
                            <button
                                onClick={() => carregarDatasDisponiveis(Math.max(0, datasOffset - DATAS_PAGE_SIZE), true)}
                                disabled={datasLoading || datasOffset === 0}
                                className="border rounded-lg px-3 py-2 disabled:opacity-50 flex items-center gap-2"
                            >
                                <ChevronLeft className="w-4 h-4" />
                                Anteriores
                            </button>
                            <span className="text-sm text-slate-600">
                                Mostrando {datasOffset + 1} - {datasOffset + datasDisponiveis.length}
                            </span>
                            <button
                                onClick={() => carregarDatasDisponiveis(datasOffset + DATAS_PAGE_SIZE, true)}
                                disabled={datasLoading || !datasHasMore}
                                className="border rounded-lg px-3 py-2 disabled:opacity-50 flex items-center gap-2"
                            >
                                Próximas
                                <ChevronRight className="w-4 h-4" />
                            </button>
                        </div>
                    </div>

                    {/* Modo de clusterização */}
                    <div className="rounded-lg border bg-slate-50 p-4">
                        <p className="text-sm font-medium mb-2">Modo de clusterização:</p>
                        <div className="flex gap-2">
                            <button
                                onClick={() => setModo("automatico")}
                                className={`flex-1 rounded-lg border px-4 py-2 text-sm font-medium transition ${
                                    modo === "automatico"
                                        ? "border-emerald-600 bg-emerald-50 text-emerald-700"
                                        : "bg-white hover:bg-slate-100"
                                }`}
                            >
                                Centro automático
                            </button>
                            <button
                                onClick={() => setModo("predefinido")}
                                className={`flex-1 rounded-lg border px-4 py-2 text-sm font-medium transition ${
                                    modo === "predefinido"
                                        ? "border-emerald-600 bg-emerald-50 text-emerald-700"
                                        : "bg-white hover:bg-slate-100"
                                }`}
                            >
                                Centro pré-definido
                            </button>
                        </div>
                    </div>

                    {/* Parâmetros só para modo automático */}
                    {modo === "automatico" && (
                        <div className="flex gap-3">
                            <label className="flex-1 text-sm font-medium">
                                Mín. entregas por cluster alvo:
                                <input
                                    type="number"
                                    min={1}
                                    value={minEntregasClusterAlvo}
                                    onChange={(e) => setMinEntregasClusterAlvo(Number(e.target.value))}
                                    className="border rounded px-3 py-2 w-full"
                                />
                            </label>
                            <label className="flex-1 text-sm font-medium">
                                Máx. entregas por cluster alvo:
                                <input
                                    type="number"
                                    min={minEntregasClusterAlvo}
                                    value={maxEntregasClusterAlvo}
                                    onChange={(e) => setMaxEntregasClusterAlvo(Number(e.target.value))}
                                    className="border rounded px-3 py-2 w-full"
                                />
                            </label>
                        </div>
                    )}

                    {/* Seleção de centros pré-definidos */}
                    {modo === "predefinido" && (
                        <div>
                            <p className="text-sm font-medium mb-1">Centros pré-definidos:</p>
                            <MultiSelectCentros
                                options={hubsCentroCluster}
                                selected={centrosSelecionados}
                                onChange={setCentrosSelecionados}
                                loading={hubsLoading}
                            />
                            {!hubsLoading && hubsCentroCluster.length === 0 && (
                                <p className="mt-1 text-xs text-red-600">
                                    Nenhum hub marcado como Centro de Cluster ativo. Edite os hubs no Cadastro de Hubs.
                                </p>
                            )}
                        </div>
                    )}

                    <label className="text-sm font-medium">
                        Hub Central:
                        <select
                            value={hubCentralId}
                            onChange={(e) => setHubCentralId(e.target.value)}
                            className="border rounded px-3 py-2 w-full"
                            disabled={hubsLoading}
                        >
                            <option value="">
                                {hubsLoading ? "Carregando hubs..." : "Selecione o Hub Central"}
                            </option>
                            {hubsCentrais.map((hub) => (
                                <option key={hub.id} value={hub.id}>
                                    {hub.nome} - {hub.endereco}
                                </option>
                            ))}
                        </select>
                    </label>
                    {!hubsLoading && hubsCentrais.length === 0 && (
                        <p className="text-sm text-red-600">
                            Cadastre e marque um hub ativo como Hub Central antes de executar.
                        </p>
                    )}

                    <label className="text-sm font-medium">
                        Raio cluster Hub Central (km):
                        <input
                            type="number"
                            step="0.1"
                            value={raioHub}
                            onChange={(e) => setRaioHub(Number(e.target.value))}
                            className="border rounded px-3 py-2 w-full"
                        />
                    </label>

                    <button
                        onClick={executar}
                        disabled={loading}
                        className="bg-emerald-600 text-white rounded-lg px-4 py-2 hover:bg-emerald-700 disabled:opacity-60 flex items-center gap-2"
                    >
                        {loading ? (
                            <>
                                <Loader2 className="w-4 h-4 animate-spin" /> Processando...
                            </>
                        ) : (
                            <>
                                <PlayCircle className="w-4 h-4" /> Executar
                            </>
                        )}
                    </button>
                </div>

                {jobState && (
                    <div className="mt-6 bg-slate-50 border rounded-lg p-4">
                        <div className="flex items-center justify-between gap-3">
                            <p className="font-medium">
                                {jobState.status === "error" ? "Clusterização com erro" : "Processamento da clusterização"}
                            </p>
                            <span className="text-sm text-slate-600">{jobState.progress}%</span>
                        </div>
                        <div className="mt-3 h-3 w-full rounded bg-slate-200 overflow-hidden">
                            <div
                                className={`h-full ${jobState.status === "error" ? "bg-red-500" : "bg-emerald-600"}`}
                                style={{ width: `${Math.min(100, Math.max(0, jobState.progress))}%` }}
                            />
                        </div>
                        <p className="mt-2 text-sm text-slate-600">{jobState.step}</p>
                        {jobState.error && (
                            <p className="mt-2 text-sm text-red-600">{jobState.error}</p>
                        )}
                    </div>
                )}

                {/* Resultado da execução */}
                {resultado && (
                    <div className="mt-6 bg-gray-50 border rounded-lg p-4">
                        <p className="font-medium">{resultado.mensagem}</p>
                        {resultado.parametros && (
                            <p className="mt-2 text-sm text-gray-600">
                                Faixa alvo: {resultado.parametros.min_entregas_por_cluster_alvo} a{" "}
                                {resultado.parametros.max_entregas_por_cluster_alvo} entregas por cluster.
                            </p>
                        )}
                        {resultado.resumo?.length > 0 && (
                            <div className="mt-3 grid gap-2">
                                {resultado.resumo.map((item: any) => (
                                    <div key={item.data} className="rounded border bg-white p-3 text-sm">
                                        <p className="font-medium">{item.data}</p>
                                        <p className="text-gray-600">
                                            {item.total_entregas} entregas, {item.total_clusters} clusters, {item.total_outliers} outliers.
                                        </p>
                                        <p className="mt-1 text-gray-600">
                                            Distribuição: {Object.entries(item.distribuicao_clusters || {}).map(([cluster, qtd]) => `Cluster ${cluster}: ${qtd}`).join(" | ")}
                                        </p>
                                    </div>
                                ))}
                            </div>
                        )}
                        {resultado.datas?.length > 0 && (
                            <>
                                <label className="block mt-3 text-sm">
                                    Data para visualizar:
                                    <select
                                        value={vizData}
                                        onChange={(e) => setVizData(e.target.value)}
                                        className="border rounded px-3 py-2 w-full"
                                    >
                                        {resultado.datas.map((d: string) => (
                                            <option key={d} value={d}>
                                                {d}
                                            </option>
                                        ))}
                                    </select>
                                </label>
                                <button
                                    onClick={() => carregarVisualizacao(vizData)}
                                    disabled={vizLoading}
                                    className="mt-3 bg-emerald-600 text-white rounded-lg px-4 py-2 hover:bg-emerald-700 disabled:opacity-60 flex items-center gap-2"
                                >
                                    {vizLoading ? (
                                        <>
                                            <Loader2 className="w-4 h-4 animate-spin" /> Gerando...
                                        </>
                                    ) : (
                                        <>
                                            <FileText className="w-4 h-4" /> Atualizar visualização
                                        </>
                                    )}
                                </button>
                            </>
                        )}
                    </div>
                )}

                {vizLoading && (
                    <div className="mt-6 bg-slate-50 border rounded-lg p-4 text-sm text-slate-600 flex items-center gap-2">
                        <Loader2 className="w-4 h-4 animate-spin" />
                        Gerando mapa, gráficos e relatório...
                    </div>
                )}

                {/* Visualização */}
                {viz && (
                    <div className="mt-6 bg-white border rounded-lg p-4 shadow-sm">
                        <p className="font-semibold mb-2">
                            Arquivos de {viz.data}
                        </p>
                        <ul className="list-disc pl-5 text-emerald-700 text-sm">
                            <li>
                                <a
                                    href={buildExportUrl(viz.arquivos.mapa_html)}
                                    target="_blank"
                                    className="hover:underline flex items-center gap-1"
                                >
                                    <Map className="w-4 h-4" /> Baixar Mapa interativo
                                </a>
                            </li>
                            <li>
                                <a
                                    href={buildExportUrl(viz.arquivos.pdf)}
                                    target="_blank"
                                    className="hover:underline flex items-center gap-1"
                                >
                                    <FileText className="w-4 h-4" /> Baixar Relatório PDF
                                </a>
                            </li>
                            {viz.arquivos.xlsx && (
                                <li>
                                    <a
                                        href={buildExportUrl(viz.arquivos.xlsx)}
                                        target="_blank"
                                        className="hover:underline flex items-center gap-1"
                                    >
                                        <FileText className="w-4 h-4" /> Baixar Entregas Clusterizadas XLSX
                                    </a>
                                </li>
                            )}
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
