import { useEffect, useMemo, useState } from "react";
import api from "@/services/api";
import toast from "react-hot-toast";
import {
    CalendarDays,
    ChevronLeft,
    ChevronRight,
    FileText,
    Loader2,
    Map,
    RefreshCw,
    Route,
    Search,
    Truck,
    X,
} from "lucide-react";
import {
    listarClusterizacoesTransfer,
    processarTransferRouting,
    type ClusterizacaoTransferDisponivel,
} from "@/services/transferRouting";

type Artefatos = {
    tenant_id: string;
    data_inicial: string;
    data_final?: string | null;
    map_html_url?: string | null;
    map_png_url?: string | null;
    pdf_url?: string | null;
    resumo?: {
        totais: TransferResumoTotais;
        totais_transferencia?: TransferResumoTotais;
        hub_central?: TransferResumoTotais;
        rotas: Array<TransferResumoRota>;
    };
};

type TransferResumoTotais = {
    rotas: number;
    entregas: number;
    paradas: number;
    volumes: number;
    peso_total_kg: number;
    distancia_ida_km: number;
    distancia_total_km: number;
    tempo_ida_min: number;
    tempo_total_min: number;
};

type TransferResumoRota = {
    rota_transf: string;
    tipo_veiculo?: string | null;
    is_hub_central?: boolean;
    quantidade_entregas: number;
    clusters_qde: number;
    volumes_total: number;
    peso_total_kg: number;
    cte_peso?: number;
    distancia_ida_km: number;
    distancia_total_km: number;
    tempo_ida_min: number;
    tempo_total_min: number;
    aproveitamento_percentual?: number | null;
};

const PAGE_SIZE = 12;

const resolveUrl = (path: string | null | undefined) => {
    if (!path) return null;
    if (path.startsWith("http")) return path;
    return `${import.meta.env.VITE_API_URL}/${path.replace(/^\/+/, "")}`;
};

const formatNumber = (value: number, maximumFractionDigits = 0) =>
    Number(value || 0).toLocaleString("pt-BR", { maximumFractionDigits });

const formatDecimal = (value: number, digits = 1) =>
    Number(value || 0).toLocaleString("pt-BR", {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    });

function getErrorMessage(err: any) {
    const detail = err?.response?.data?.detail;
    if (typeof detail === "string") return detail;
    if (detail?.detail) return detail.detail;
    return err?.message || "Erro não identificado";
}

function ClusterizacaoCard({
    item,
    active,
    onClick,
}: {
    item: ClusterizacaoTransferDisponivel;
    active: boolean;
    onClick: () => void;
}) {
    return (
        <button
            onClick={onClick}
            className={`rounded-lg border px-3 py-2 text-left text-sm transition ${
                active
                    ? "border-emerald-500 bg-emerald-50 text-emerald-900"
                    : "bg-white hover:bg-slate-50 text-slate-700"
            }`}
        >
            <div className="flex items-center justify-between gap-3">
                <span className="font-medium">{item.data}</span>
                <span
                    className={`rounded px-2 py-0.5 text-[11px] ${
                        item.roteirizacao_existente
                            ? "bg-emerald-100 text-emerald-700"
                            : "bg-slate-100 text-slate-500"
                    }`}
                >
                    {item.roteirizacao_existente ? `${item.rotas_processadas} rotas` : "pendente"}
                </span>
            </div>
            <div className="mt-1 grid grid-cols-3 gap-2 text-xs text-slate-500">
                <span>{formatNumber(item.quantidade_entregas)} CTEs</span>
                <span>{formatNumber(item.clusters_transferiveis)} cargas</span>
                <span className="text-right">{formatNumber(item.peso_total_kg)} kg</span>
            </div>
        </button>
    );
}

export default function RoutingPage() {
    const [data, setData] = useState("");
    const [clusterizacoes, setClusterizacoes] = useState<ClusterizacaoTransferDisponivel[]>([]);
    const [clusterizacoesLoading, setClusterizacoesLoading] = useState(false);
    const [offset, setOffset] = useState(0);
    const [hasMore, setHasMore] = useState(false);
    const [dataInicioFiltro, setDataInicioFiltro] = useState("");
    const [dataFimFiltro, setDataFimFiltro] = useState("");

    const [params, setParams] = useState({
        modo_forcar: false,
        tempo_maximo: 1200,
        tempo_parada_leve: 10,
        peso_leve_max: 50,
        tempo_parada_pesada: 20,
        tempo_por_volume: 0.4,
    });
    const [loading, setLoading] = useState(false);
    const [artefatosLoading, setArtefatosLoading] = useState(false);
    const [artefatos, setArtefatos] = useState<Artefatos | null>(null);
    const [iframeKey, setIframeKey] = useState(0);

    const selecionada = useMemo(
        () => clusterizacoes.find((item) => item.data === data) || null,
        [clusterizacoes, data],
    );
    const canRun = Boolean(data);
    const rotasTransferencia = artefatos?.resumo?.rotas?.filter((rota) => !rota.is_hub_central && rota.rota_transf !== "HUB") || [];
    const validacaoHub = artefatos?.resumo?.hub_central;
    const totaisTransferencia = artefatos?.resumo?.totais_transferencia || artefatos?.resumo?.totais;

    async function carregarClusterizacoes(nextOffset = 0, usarFiltros = true) {
        setClusterizacoesLoading(true);
        try {
            const payload: Record<string, string | number> = {
                limit: PAGE_SIZE,
                offset: nextOffset,
            };
            if (usarFiltros && dataInicioFiltro) payload.data_inicio = dataInicioFiltro;
            if (usarFiltros && dataFimFiltro) payload.data_fim = dataFimFiltro;

            const resp = await listarClusterizacoesTransfer(payload);
            const itens = resp.clusterizacoes || [];
            setClusterizacoes(itens);
            setOffset(nextOffset);
            setHasMore(Boolean(resp.pagination?.has_more));

            if (itens.length > 0 && (!data || !itens.some((item) => item.data === data))) {
                setData(itens[0].data);
            }
            if (itens.length === 0) {
                setData("");
                setArtefatos(null);
            }
        } catch (err: any) {
            toast.error("Não foi possível carregar clusterizações: " + getErrorMessage(err));
        } finally {
            setClusterizacoesLoading(false);
        }
    }

    function limparFiltros() {
        setDataInicioFiltro("");
        setDataFimFiltro("");
        carregarClusterizacoes(0, false);
    }

    useEffect(() => {
        carregarClusterizacoes(0, true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    async function processar() {
        if (!canRun) {
            toast.error("Selecione uma clusterização.");
            return;
        }
        try {
            setLoading(true);
            const result = await processarTransferRouting({
                data_inicial: data,
                modo_forcar: params.modo_forcar,
                tempo_maximo: params.tempo_maximo,
                tempo_parada_leve: params.tempo_parada_leve,
                peso_leve_max: params.peso_leve_max,
                tempo_parada_pesada: params.tempo_parada_pesada,
                tempo_por_volume: params.tempo_por_volume,
            });
            if (result?.status === "skipped_existing") {
                toast(result.mensagem || "Roteirização já existente. Nenhum dado foi reprocessado.", {
                    icon: "ℹ️",
                });
                await buscarArtefatos({ silencioso: true });
            } else {
                toast.success(result?.mensagem || "Roteirização processada com sucesso.");
                await gerarArtefatos({ baixarPdf: false, silencioso: true });
            }
            await carregarClusterizacoes(offset, true);
        } catch (err: any) {
            toast.error("Erro ao processar roteirização: " + getErrorMessage(err));
        } finally {
            setLoading(false);
        }
    }

    async function gerarArtefatos(options: { baixarPdf?: boolean; silencioso?: boolean } = {}) {
        if (!canRun) {
            toast.error("Selecione uma clusterização.");
            return;
        }
        const { baixarPdf = false, silencioso = false } = options;
        try {
            setArtefatosLoading(true);
            const resp = await api.get("/transfer_routing/visualizacao", {
                params: { data_inicial: data },
                responseType: "blob",
            });

            if (baixarPdf) {
                const blob = new Blob([resp.data], { type: "application/pdf" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = `relatorio_transferencias_${data}.pdf`;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(url);
            }

            if (!silencioso) {
                toast.success(baixarPdf ? "PDF gerado e baixado." : "Artefatos gerados e carregados.");
            }
            await buscarArtefatos({ silencioso: true });
        } catch (err: any) {
            toast.error("Erro ao gerar artefatos: " + getErrorMessage(err));
        } finally {
            setArtefatosLoading(false);
        }
    }

    async function buscarArtefatos(options: { silencioso?: boolean } = {}) {
        if (!canRun) return;
        try {
            const { data: resp } = await api.get<Artefatos>(
                "/transfer_routing/artefatos",
                { params: { data_inicial: data } },
            );
            setArtefatos(resp);
            setIframeKey((k) => k + 1);
            if (!options.silencioso) {
                toast.success("Artefatos carregados.");
            }
        } catch {
            setArtefatos(null);
            if (!options.silencioso) {
                toast("Nenhum artefato encontrado para esta data.", { icon: "⚠️" });
            }
        }
    }

    useEffect(() => {
        setArtefatos(null);
    }, [data]);

    return (
        <div className="px-6 py-4">
            <div className="bg-white shadow rounded-2xl p-5">
                <h2 className="text-lg font-bold mb-4 flex items-center gap-2">
                    <Route className="w-5 h-5 text-emerald-600" />
                    Middle-Mile • Roteirização
                </h2>

                <div className="grid grid-cols-1 lg:grid-cols-[1fr_360px] gap-4 items-start">
                    <div className="rounded-lg border bg-slate-50 p-3">
                        <div className="flex items-center justify-between gap-2 mb-2">
                            <p className="text-sm font-medium flex items-center gap-1.5">
                                <CalendarDays className="w-4 h-4 text-emerald-600" />
                                Clusterizações disponíveis
                            </p>
                            {data && (
                                <span className="text-xs text-slate-500 bg-emerald-50 border border-emerald-200 rounded px-2 py-0.5">
                                    {data}
                                </span>
                            )}
                        </div>

                        <div className="grid grid-cols-[1fr_1fr_auto_auto] gap-2 mb-2">
                            <input
                                type="date"
                                value={dataInicioFiltro}
                                onChange={(e) => setDataInicioFiltro(e.target.value)}
                                className="border rounded px-2 py-1.5 text-sm bg-white w-full"
                            />
                            <input
                                type="date"
                                value={dataFimFiltro}
                                onChange={(e) => setDataFimFiltro(e.target.value)}
                                className="border rounded px-2 py-1.5 text-sm bg-white w-full"
                            />
                            <button
                                onClick={() => carregarClusterizacoes(0, true)}
                                disabled={clusterizacoesLoading}
                                className="border rounded px-3 py-1.5 bg-white hover:bg-slate-100 disabled:opacity-60 flex items-center gap-1 text-sm"
                            >
                                {clusterizacoesLoading ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Search className="w-3.5 h-3.5" />}
                                Filtrar
                            </button>
                            <button
                                onClick={limparFiltros}
                                disabled={clusterizacoesLoading || (!dataInicioFiltro && !dataFimFiltro && offset === 0)}
                                className="border rounded px-2 py-1.5 bg-white hover:bg-slate-100 disabled:opacity-50"
                                title="Limpar filtros"
                            >
                                <X className="w-3.5 h-3.5" />
                            </button>
                        </div>

                        <div className="grid gap-1">
                            {clusterizacoesLoading && (
                                <p className="text-xs text-gray-400 flex items-center gap-1.5 py-2">
                                    <Loader2 className="w-3.5 h-3.5 animate-spin" /> Carregando...
                                </p>
                            )}
                            {!clusterizacoesLoading && clusterizacoes.length === 0 && (
                                <p className="text-sm text-gray-500 py-2">Nenhuma clusterização encontrada.</p>
                            )}
                            {!clusterizacoesLoading && clusterizacoes.map((item) => (
                                <ClusterizacaoCard
                                    key={item.data}
                                    item={item}
                                    active={data === item.data}
                                    onClick={() => setData(item.data)}
                                />
                            ))}
                        </div>

                        <div className="mt-2 flex items-center justify-between gap-2">
                            <button
                                onClick={() => carregarClusterizacoes(Math.max(0, offset - PAGE_SIZE), true)}
                                disabled={clusterizacoesLoading || offset === 0}
                                className="border rounded px-2 py-1 text-xs disabled:opacity-40 flex items-center gap-1 hover:bg-slate-100"
                            >
                                <ChevronLeft className="w-3.5 h-3.5" /> Anteriores
                            </button>
                            <span className="text-xs text-slate-400">
                                {clusterizacoes.length ? `${offset + 1}-${offset + clusterizacoes.length}` : "0"}
                            </span>
                            <button
                                onClick={() => carregarClusterizacoes(offset + PAGE_SIZE, true)}
                                disabled={clusterizacoesLoading || !hasMore}
                                className="border rounded px-2 py-1 text-xs disabled:opacity-40 flex items-center gap-1 hover:bg-slate-100"
                            >
                                Próximas <ChevronRight className="w-3.5 h-3.5" />
                            </button>
                        </div>
                    </div>

                    <div className="grid gap-3">
                        <div className="rounded-lg border bg-slate-50 p-3">
                            <p className="text-xs font-medium text-slate-500 uppercase tracking-wide mb-2">Carga selecionada</p>
                            {selecionada ? (
                                <div className="grid grid-cols-2 gap-2 text-sm">
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">CTEs</p>
                                        <p className="font-semibold text-slate-800">{formatNumber(selecionada.quantidade_entregas)}</p>
                                    </div>
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">Cargas</p>
                                        <p className="font-semibold text-slate-800">{formatNumber(selecionada.clusters_transferiveis)}</p>
                                    </div>
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">Peso</p>
                                        <p className="font-semibold text-slate-800">{formatNumber(selecionada.peso_total_kg)} kg</p>
                                    </div>
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">Volumes</p>
                                        <p className="font-semibold text-slate-800">{formatNumber(selecionada.volumes_total)}</p>
                                    </div>
                                </div>
                            ) : (
                                <p className="text-sm text-slate-500">Selecione uma clusterização.</p>
                            )}
                        </div>

                        <div className="grid grid-cols-2 gap-2">
                            <label className="text-xs font-medium text-slate-600">
                                Tempo máx. rota
                                <input type="number" min={1} value={params.tempo_maximo}
                                    onChange={(e) => setParams((s) => ({ ...s, tempo_maximo: Number(e.target.value) }))}
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right" />
                            </label>
                            <label className="text-xs font-medium text-slate-600">
                                Peso leve máx.
                                <input type="number" min={0} value={params.peso_leve_max}
                                    onChange={(e) => setParams((s) => ({ ...s, peso_leve_max: Number(e.target.value) }))}
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right" />
                            </label>
                            <label className="text-xs font-medium text-slate-600">
                                Parada leve
                                <input type="number" min={0} value={params.tempo_parada_leve}
                                    onChange={(e) => setParams((s) => ({ ...s, tempo_parada_leve: Number(e.target.value) }))}
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right" />
                            </label>
                            <label className="text-xs font-medium text-slate-600">
                                Parada pesada
                                <input type="number" min={0} value={params.tempo_parada_pesada}
                                    onChange={(e) => setParams((s) => ({ ...s, tempo_parada_pesada: Number(e.target.value) }))}
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right" />
                            </label>
                        </div>

                        <label className="text-xs font-medium text-slate-600">
                            Tempo por volume
                            <input type="number" step="0.01" min={0} value={params.tempo_por_volume}
                                onChange={(e) => setParams((s) => ({ ...s, tempo_por_volume: Number(e.target.value) }))}
                                className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right" />
                        </label>

                        <label className="flex items-center gap-2 rounded-lg border bg-slate-50 p-3 text-sm text-slate-700">
                            <input
                                type="checkbox"
                                checked={params.modo_forcar}
                                onChange={(e) => setParams((s) => ({ ...s, modo_forcar: e.target.checked }))}
                                className="h-4 w-4 text-emerald-600 border-gray-300 rounded"
                            />
                            Forçar sobrescrita
                        </label>

                        <button
                            onClick={processar}
                            disabled={!canRun || loading}
                            className="w-full bg-emerald-600 text-white rounded-lg px-4 py-2.5 hover:bg-emerald-700 disabled:opacity-60 flex items-center justify-center gap-2 font-medium"
                        >
                            {loading
                                ? <><Loader2 className="w-4 h-4 animate-spin" /> Processando...</>
                                : <><Truck className="w-4 h-4" /> Processar roteirização</>}
                        </button>

                        <div className="grid grid-cols-2 gap-2">
                            <button
                                onClick={() => buscarArtefatos()}
                                disabled={!canRun || loading || artefatosLoading}
                                className="border rounded-lg px-3 py-2 bg-white hover:bg-slate-50 disabled:opacity-50 flex items-center justify-center gap-2 text-sm"
                            >
                                {artefatosLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <RefreshCw className="w-4 h-4" />}
                                Artefatos
                            </button>
                            <button
                                onClick={() => gerarArtefatos({ baixarPdf: true })}
                                disabled={!canRun || loading || artefatosLoading}
                                className="border rounded-lg px-3 py-2 bg-white hover:bg-slate-50 disabled:opacity-50 flex items-center justify-center gap-2 text-sm"
                            >
                                {artefatosLoading ? <Loader2 className="w-4 h-4 animate-spin" /> : <FileText className="w-4 h-4" />}
                                PDF
                            </button>
                        </div>
                    </div>
                </div>

                {artefatosLoading && !artefatos && (
                    <div className="mt-6 flex items-center gap-2 text-sm text-slate-500">
                        <Loader2 className="w-4 h-4 animate-spin" /> Gerando e carregando artefatos...
                    </div>
                )}

                {artefatos && (
                    <div className="mt-6 grid gap-4">
                        {totaisTransferencia && (
                            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                                {[
                                    { label: "Rotas transf.", value: formatNumber(totaisTransferencia.rotas) },
                                    { label: "Entregas transf.", value: formatNumber(totaisTransferencia.entregas) },
                                    { label: "Paradas transf.", value: formatNumber(totaisTransferencia.paradas) },
                                    { label: "Peso transf.", value: `${formatNumber(totaisTransferencia.peso_total_kg)} kg` },
                                    { label: "Volumes transf.", value: formatNumber(totaisTransferencia.volumes) },
                                    { label: "Dist. ida", value: `${formatDecimal(totaisTransferencia.distancia_ida_km)} km` },
                                    { label: "Dist. total", value: `${formatDecimal(totaisTransferencia.distancia_total_km)} km` },
                                    { label: "Tempo total", value: `${formatDecimal(totaisTransferencia.tempo_total_min)} min` },
                                ].map((kpi) => (
                                    <div key={kpi.label} className="rounded-lg border bg-white p-3 text-center">
                                        <p className="text-xs text-slate-500 uppercase tracking-wide">{kpi.label}</p>
                                        <p className="text-xl font-bold text-slate-800 mt-0.5">{kpi.value}</p>
                                    </div>
                                ))}
                            </div>
                        )}

                        {validacaoHub && validacaoHub.entregas > 0 && (
                            <div className="rounded-lg border border-amber-200 bg-amber-50 p-4">
                                <div className="flex flex-wrap items-start justify-between gap-3">
                                    <div>
                                        <p className="text-sm font-semibold text-amber-900">Hub Central 9999 - validação de somatório</p>
                                        <p className="text-xs text-amber-800 mt-1">
                                            Estas entregas já estão no Hub Central. Não geram rota de transferência, não aparecem no mapa e não têm veículo alocado.
                                        </p>
                                    </div>
                                    <div className="grid grid-cols-3 gap-2 text-right">
                                        <div>
                                            <p className="text-[11px] uppercase text-amber-700">CTEs</p>
                                            <p className="font-semibold text-amber-950">{formatNumber(validacaoHub.entregas)}</p>
                                        </div>
                                        <div>
                                            <p className="text-[11px] uppercase text-amber-700">Peso</p>
                                            <p className="font-semibold text-amber-950">{formatDecimal(validacaoHub.peso_total_kg, 2)} kg</p>
                                        </div>
                                        <div>
                                            <p className="text-[11px] uppercase text-amber-700">Volumes</p>
                                            <p className="font-semibold text-amber-950">{formatNumber(validacaoHub.volumes)}</p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        )}

                        <div className="rounded-lg border bg-slate-50 p-4">
                            <div className="flex flex-wrap items-center justify-between gap-3 mb-3">
                                <div>
                                    <p className="text-sm font-medium text-slate-700">Artefatos {artefatos.data_inicial}</p>
                                    <p className="text-xs text-slate-500">Mapa e legenda exibem apenas rotas de transferência. Hub Central 9999 fica fora da roteirização.</p>
                                </div>
                                <div className="flex flex-wrap gap-3">
                                    {artefatos.map_html_url && (
                                        <a href={resolveUrl(artefatos.map_html_url) || "#"} target="_blank" rel="noreferrer"
                                            className="flex items-center gap-1.5 text-sm text-emerald-700 hover:underline">
                                            <Map className="w-4 h-4" /> Mapa interativo
                                        </a>
                                    )}
                                    {artefatos.map_png_url && (
                                        <a href={resolveUrl(artefatos.map_png_url) || "#"} target="_blank" rel="noreferrer"
                                            className="flex items-center gap-1.5 text-sm text-emerald-700 hover:underline">
                                            <Map className="w-4 h-4" /> Mapa PNG
                                        </a>
                                    )}
                                    {artefatos.pdf_url && (
                                        <a href={resolveUrl(artefatos.pdf_url) || "#"} target="_blank" rel="noreferrer"
                                            className="flex items-center gap-1.5 text-sm text-emerald-700 hover:underline">
                                            <FileText className="w-4 h-4" /> Relatório PDF
                                        </a>
                                    )}
                                </div>
                            </div>

                            {artefatos.map_html_url && (
                                <div className="border rounded-xl overflow-hidden bg-white">
                                    <iframe
                                        key={iframeKey}
                                        src={`${resolveUrl(artefatos.map_html_url) || ""}?v=${iframeKey}`}
                                        title="Mapa Interativo - Transferências"
                                        className="w-full"
                                        style={{ height: "68vh" }}
                                    />
                                </div>
                            )}
                        </div>

                        {rotasTransferencia.length ? (
                            <div className="rounded-lg border bg-white overflow-hidden">
                                <div className="px-4 py-3 border-b bg-slate-50">
                                    <p className="text-sm font-medium text-slate-700">Resumo das rotas de transferência</p>
                                </div>
                                <div className="overflow-x-auto">
                                    <table className="w-full text-sm">
                                        <thead className="bg-slate-50 text-xs uppercase text-slate-500">
                                            <tr>
                                                <th className="px-3 py-2 text-left">Rota</th>
                                                <th className="px-3 py-2 text-left">Veículo</th>
                                                <th className="px-3 py-2 text-right">Entregas</th>
                                                <th className="px-3 py-2 text-right">Paradas</th>
                                                <th className="px-3 py-2 text-right">Peso kg</th>
                                                <th className="px-3 py-2 text-right">Dist. total</th>
                                                <th className="px-3 py-2 text-right">Tempo total</th>
                                            </tr>
                                        </thead>
                                        <tbody className="divide-y divide-slate-100">
                                            {rotasTransferencia.map((rota) => (
                                                <tr key={rota.rota_transf} className="hover:bg-slate-50">
                                                    <td className="px-3 py-2 font-medium text-slate-800">{rota.rota_transf}</td>
                                                    <td className="px-3 py-2 text-slate-600">{rota.tipo_veiculo || "-"}</td>
                                                    <td className="px-3 py-2 text-right">{formatNumber(rota.quantidade_entregas)}</td>
                                                    <td className="px-3 py-2 text-right">{formatNumber(rota.clusters_qde)}</td>
                                                    <td className="px-3 py-2 text-right">{formatDecimal(rota.peso_total_kg || rota.cte_peso || 0, 2)}</td>
                                                    <td className="px-3 py-2 text-right">{formatDecimal(rota.distancia_total_km, 1)} km</td>
                                                    <td className="px-3 py-2 text-right">{formatDecimal(rota.tempo_total_min, 1)} min</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        ) : null}
                    </div>
                )}
            </div>
        </div>
    );
}
