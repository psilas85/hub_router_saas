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
import { MapContainer, TileLayer, CircleMarker, Marker, Polyline, Popup, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import {
    listarClusterizacoesTransfer,
    processarTransferRouting,
    type ClusterizacaoTransferDisponivel,
} from "@/services/transferRouting";

// ─── Types ────────────────────────────────────────────────────────────────────

type GeoFeatureProp = {
    feature_type: "hub" | "route" | "stop";
    rota_id?: string;
    color?: string;
    is_hub_central?: boolean;
    tipo_veiculo?: string | null;
    quantidade_entregas?: number | null;
    clusters_qde?: number | null;
    volumes_total?: number | null;
    peso_total_kg?: number | null;
    distancia_total_km?: number | null;
    tempo_total_min?: number | null;
    aproveitamento_percentual?: number | null;
    stop_index?: number;
    label?: string;
};

type GeoFeature = {
    type: "Feature";
    geometry:
        | { type: "Point"; coordinates: [number, number] }
        | { type: "LineString"; coordinates: [number, number][] };
    properties: GeoFeatureProp;
};

type GeoCollection = {
    type: "FeatureCollection";
    features: GeoFeature[];
    metadata?: {
        tenant_id: string;
        data_inicial: string;
        data_final: string;
        total_routes: number;
    };
};

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

// ─── Constants / helpers ──────────────────────────────────────────────────────

const PAGE_SIZE = 5;

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

function getErrorMessage(err: unknown) {
    const e = err as { response?: { data?: { detail?: unknown } }; message?: string };
    const detail = e?.response?.data?.detail;
    if (typeof detail === "string") return detail;
    if (detail && typeof detail === "object" && "detail" in detail) return String((detail as { detail: unknown }).detail);
    return e?.message || "Erro não identificado";
}

// ─── Map sub-components ───────────────────────────────────────────────────────

function FitBounds({ coords }: { coords: [number, number][] }) {
    const map = useMap();
    useEffect(() => {
        if (coords.length > 0) {
            map.fitBounds(coords, { padding: [40, 40] });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);
    return null;
}

function TransferMap({
    geoData,
    selectedRoute,
    onSelectRoute,
}: {
    geoData: GeoCollection;
    selectedRoute: string | null;
    onSelectRoute: (id: string | null) => void;
}) {
    const hub = geoData.features.find((f) => f.properties.feature_type === "hub");
    const routes = geoData.features.filter((f) => f.properties.feature_type === "route");
    const stops = geoData.features.filter((f) => f.properties.feature_type === "stop");

    const allCoords = useMemo<[number, number][]>(() => {
        const pts: [number, number][] = [];
        routes.forEach((f) => {
            if (f.geometry.type === "LineString") {
                (f.geometry.coordinates as [number, number][]).forEach(([lng, lat]) =>
                    pts.push([lat, lng])
                );
            }
        });
        return pts;
    }, [routes]);

    const center: [number, number] = hub
        ? [
              (hub.geometry.coordinates as [number, number])[1],
              (hub.geometry.coordinates as [number, number])[0],
          ]
        : [-15, -47];

    const hubIcon = L.divIcon({
        html: `<div style="background:#ef4444;width:16px;height:16px;border-radius:50%;border:3px solid white;box-shadow:0 2px 6px rgba(0,0,0,.5)"></div>`,
        className: "",
        iconSize: [16, 16],
        iconAnchor: [8, 8],
    });

    return (
        <MapContainer
            center={center}
            zoom={7}
            style={{ height: "62vh", width: "100%", borderRadius: "8px" }}
            scrollWheelZoom
        >
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />

            {hub && (
                <Marker
                    position={[
                        (hub.geometry.coordinates as [number, number])[1],
                        (hub.geometry.coordinates as [number, number])[0],
                    ]}
                    icon={hubIcon}
                >
                    <Popup>
                        <strong>Hub Central</strong>
                    </Popup>
                </Marker>
            )}

            {routes.map((f) => {
                const p = f.properties;
                const isSelected = selectedRoute === p.rota_id;
                const isHub = p.is_hub_central;
                const coords = (f.geometry.coordinates as [number, number][]).map(
                    ([lng, lat]) => [lat, lng] as [number, number]
                );
                const color = isSelected ? "#f97316" : p.color || "#2563eb";
                return (
                    <Polyline
                        key={`route-${p.rota_id}`}
                        positions={coords}
                        pathOptions={{
                            color,
                            weight: isSelected ? 5 : isHub ? 1.5 : 2.5,
                            opacity: isHub ? 0.35 : isSelected ? 1 : 0.75,
                            dashArray: isHub ? "6 4" : undefined,
                        }}
                        eventHandlers={{
                            click: () => onSelectRoute(isSelected ? null : p.rota_id || null),
                        }}
                    >
                        <Popup>
                            <div style={{ fontSize: "12px", lineHeight: "1.6" }}>
                                <strong>Rota {p.rota_id}</strong>
                                {p.tipo_veiculo && <div>Veículo: {p.tipo_veiculo}</div>}
                                {p.quantidade_entregas != null && <div>Entregas: {p.quantidade_entregas}</div>}
                                {p.peso_total_kg != null && (
                                    <div>Peso: {Number(p.peso_total_kg).toFixed(1)} kg</div>
                                )}
                                {p.distancia_total_km != null && (
                                    <div>Dist. total: {Number(p.distancia_total_km).toFixed(1)} km</div>
                                )}
                                {p.aproveitamento_percentual != null && (
                                    <div>Aproveitamento: {Number(p.aproveitamento_percentual).toFixed(1)}%</div>
                                )}
                            </div>
                        </Popup>
                    </Polyline>
                );
            })}

            {stops.map((f, i) => {
                const p = f.properties;
                const [lng, lat] = f.geometry.coordinates as [number, number];
                const dimmed = selectedRoute !== null && selectedRoute !== p.rota_id;
                return (
                    <CircleMarker
                        key={`stop-${i}`}
                        center={[lat, lng]}
                        radius={dimmed ? 3 : 5}
                        pathOptions={{
                            color: "white",
                            fillColor: p.color || "#2563eb",
                            fillOpacity: dimmed ? 0.25 : 0.9,
                            weight: 1.5,
                        }}
                    >
                        <Popup>
                            Rota {p.rota_id} — parada {p.stop_index}
                        </Popup>
                    </CircleMarker>
                );
            })}

            {allCoords.length > 0 && <FitBounds coords={allCoords} />}
        </MapContainer>
    );
}

// ─── ClusterizacaoCard ────────────────────────────────────────────────────────

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

// ─── Main page ────────────────────────────────────────────────────────────────

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
    const [geoData, setGeoData] = useState<GeoCollection | null>(null);
    const [geoLoading, setGeoLoading] = useState(false);
    const [activeTab, setActiveTab] = useState<"mapa" | "rotas" | "downloads">("mapa");
    const [selectedRoute, setSelectedRoute] = useState<string | null>(null);

    const selecionada = useMemo(
        () => clusterizacoes.find((item) => item.data === data) || null,
        [clusterizacoes, data],
    );
    const canRun = Boolean(data);
    const rotasTransferencia =
        artefatos?.resumo?.rotas?.filter((r) => !r.is_hub_central && r.rota_transf !== "HUB") || [];
    const validacaoHub = artefatos?.resumo?.hub_central;
    const totaisTransferencia = artefatos?.resumo?.totais_transferencia || artefatos?.resumo?.totais;

    // Map from rota_id → color (from GeoJSON, so colors are always in sync with map)
    const routeColorMap = useMemo(() => {
        if (!geoData) return {} as Record<string, string>;
        const m: Record<string, string> = {};
        geoData.features
            .filter((f) => f.properties.feature_type === "route")
            .forEach((f) => {
                if (f.properties.rota_id && f.properties.color) {
                    m[f.properties.rota_id] = f.properties.color;
                }
            });
        return m;
    }, [geoData]);

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
                setGeoData(null);
            }
        } catch (err: unknown) {
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

    async function buscarGeoJSON() {
        if (!canRun) return;
        setGeoLoading(true);
        try {
            const { data: resp } = await api.get<GeoCollection>("/transfer_routing/geojson", {
                params: { data_inicial: data },
            });
            setGeoData(resp);
        } catch {
            setGeoData(null);
        } finally {
            setGeoLoading(false);
        }
    }

    async function buscarArtefatos(options: { silencioso?: boolean } = {}) {
        if (!canRun) return;
        try {
            const { data: resp } = await api.get<Artefatos>("/transfer_routing/artefatos", {
                params: { data_inicial: data },
            });
            setArtefatos(resp);
            if (!options.silencioso) {
                toast.success("Artefatos carregados.");
            }
        } catch {
            setArtefatos(null);
            if (!options.silencioso) {
                toast("Nenhum artefato encontrado para esta data.", { icon: "⚠️" });
            }
        }
        await buscarGeoJSON();
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
        } catch (err: unknown) {
            toast.error("Erro ao gerar artefatos: " + getErrorMessage(err));
        } finally {
            setArtefatosLoading(false);
        }
    }

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
        } catch (err: unknown) {
            toast.error("Erro ao processar roteirização: " + getErrorMessage(err));
        } finally {
            setLoading(false);
        }
    }

    useEffect(() => {
        setArtefatos(null);
        setGeoData(null);
        setSelectedRoute(null);
        setActiveTab("mapa");
    }, [data]);

    // ── Tab helper ────────────────────────────────────────────
    const tabCls = (tab: string) =>
        `flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition ${
            activeTab === tab
                ? "border-emerald-600 text-emerald-700"
                : "border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300"
        }`;

    return (
        <div className="px-6 py-4">
            <div className="bg-white shadow rounded-2xl p-5">
                <h2 className="text-lg font-bold mb-4 flex items-center gap-2">
                    <Route className="w-5 h-5 text-emerald-600" />
                    Middle-Mile • Roteirização
                </h2>

                <div className="grid grid-cols-1 lg:grid-cols-[1fr_360px] gap-4 items-start">
                    {/* ── Left: date list ── */}
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
                                {clusterizacoesLoading ? (
                                    <Loader2 className="w-3.5 h-3.5 animate-spin" />
                                ) : (
                                    <Search className="w-3.5 h-3.5" />
                                )}
                                Filtrar
                            </button>
                            <button
                                onClick={limparFiltros}
                                disabled={
                                    clusterizacoesLoading ||
                                    (!dataInicioFiltro && !dataFimFiltro && offset === 0)
                                }
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
                                <p className="text-sm text-gray-500 py-2">
                                    Nenhuma clusterização encontrada.
                                </p>
                            )}
                            {!clusterizacoesLoading &&
                                clusterizacoes.map((item) => (
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
                                onClick={() =>
                                    carregarClusterizacoes(Math.max(0, offset - PAGE_SIZE), true)
                                }
                                disabled={clusterizacoesLoading || offset === 0}
                                className="border rounded px-2 py-1 text-xs disabled:opacity-40 flex items-center gap-1 hover:bg-slate-100"
                            >
                                <ChevronLeft className="w-3.5 h-3.5" /> Anteriores
                            </button>
                            <span className="text-xs text-slate-400">
                                {clusterizacoes.length
                                    ? `${offset + 1}–${offset + clusterizacoes.length}`
                                    : "—"}
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

                    {/* ── Right: params + actions ── */}
                    <div className="grid gap-3">
                        <div className="rounded-lg border bg-slate-50 p-3">
                            <p className="text-xs font-medium text-slate-500 uppercase tracking-wide mb-2">
                                Carga selecionada
                            </p>
                            {selecionada ? (
                                <div className="grid grid-cols-2 gap-2 text-sm">
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">CTEs</p>
                                        <p className="font-semibold text-slate-800">
                                            {formatNumber(selecionada.quantidade_entregas)}
                                        </p>
                                    </div>
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">Cargas</p>
                                        <p className="font-semibold text-slate-800">
                                            {formatNumber(selecionada.clusters_transferiveis)}
                                        </p>
                                    </div>
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">Peso</p>
                                        <p className="font-semibold text-slate-800">
                                            {formatNumber(selecionada.peso_total_kg)} kg
                                        </p>
                                    </div>
                                    <div className="rounded border bg-white p-2">
                                        <p className="text-xs text-slate-500">Volumes</p>
                                        <p className="font-semibold text-slate-800">
                                            {formatNumber(selecionada.volumes_total)}
                                        </p>
                                    </div>
                                </div>
                            ) : (
                                <p className="text-sm text-slate-500">Selecione uma clusterização.</p>
                            )}
                        </div>

                        <div className="grid grid-cols-2 gap-2">
                            <label className="text-xs font-medium text-slate-600">
                                Tempo máx. rota
                                <input
                                    type="number"
                                    min={1}
                                    value={params.tempo_maximo}
                                    onChange={(e) =>
                                        setParams((s) => ({ ...s, tempo_maximo: Number(e.target.value) }))
                                    }
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right"
                                />
                            </label>
                            <label className="text-xs font-medium text-slate-600">
                                Peso leve máx.
                                <input
                                    type="number"
                                    min={0}
                                    value={params.peso_leve_max}
                                    onChange={(e) =>
                                        setParams((s) => ({ ...s, peso_leve_max: Number(e.target.value) }))
                                    }
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right"
                                />
                            </label>
                            <label className="text-xs font-medium text-slate-600">
                                Parada leve
                                <input
                                    type="number"
                                    min={0}
                                    value={params.tempo_parada_leve}
                                    onChange={(e) =>
                                        setParams((s) => ({
                                            ...s,
                                            tempo_parada_leve: Number(e.target.value),
                                        }))
                                    }
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right"
                                />
                            </label>
                            <label className="text-xs font-medium text-slate-600">
                                Parada pesada
                                <input
                                    type="number"
                                    min={0}
                                    value={params.tempo_parada_pesada}
                                    onChange={(e) =>
                                        setParams((s) => ({
                                            ...s,
                                            tempo_parada_pesada: Number(e.target.value),
                                        }))
                                    }
                                    className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right"
                                />
                            </label>
                        </div>

                        <label className="text-xs font-medium text-slate-600">
                            Tempo por volume
                            <input
                                type="number"
                                step="0.01"
                                min={0}
                                value={params.tempo_por_volume}
                                onChange={(e) =>
                                    setParams((s) => ({ ...s, tempo_por_volume: Number(e.target.value) }))
                                }
                                className="mt-1 border rounded px-2 py-1.5 text-sm w-full text-right"
                            />
                        </label>

                        <label className="flex items-center gap-2 rounded-lg border bg-slate-50 p-3 text-sm text-slate-700">
                            <input
                                type="checkbox"
                                checked={params.modo_forcar}
                                onChange={(e) =>
                                    setParams((s) => ({ ...s, modo_forcar: e.target.checked }))
                                }
                                className="h-4 w-4 text-emerald-600 border-gray-300 rounded"
                            />
                            Forçar sobrescrita
                        </label>

                        <button
                            onClick={processar}
                            disabled={!canRun || loading}
                            className="w-full bg-emerald-600 text-white rounded-lg px-4 py-2.5 hover:bg-emerald-700 disabled:opacity-60 flex items-center justify-center gap-2 font-medium"
                        >
                            {loading ? (
                                <>
                                    <Loader2 className="w-4 h-4 animate-spin" /> Processando...
                                </>
                            ) : (
                                <>
                                    <Truck className="w-4 h-4" /> Processar roteirização
                                </>
                            )}
                        </button>

                        <div className="grid grid-cols-2 gap-2">
                            <button
                                onClick={() => buscarArtefatos()}
                                disabled={!canRun || loading || artefatosLoading}
                                className="border rounded-lg px-3 py-2 bg-white hover:bg-slate-50 disabled:opacity-50 flex items-center justify-center gap-2 text-sm"
                            >
                                {artefatosLoading ? (
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                ) : (
                                    <RefreshCw className="w-4 h-4" />
                                )}
                                Artefatos
                            </button>
                            <button
                                onClick={() => gerarArtefatos({ baixarPdf: true })}
                                disabled={!canRun || loading || artefatosLoading}
                                className="border rounded-lg px-3 py-2 bg-white hover:bg-slate-50 disabled:opacity-50 flex items-center justify-center gap-2 text-sm"
                            >
                                {artefatosLoading ? (
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                ) : (
                                    <FileText className="w-4 h-4" />
                                )}
                                PDF
                            </button>
                        </div>
                    </div>
                </div>

                {/* ── Loading overlay ── */}
                {artefatosLoading && !artefatos && (
                    <div className="mt-6 flex items-center gap-2 text-sm text-slate-500">
                        <Loader2 className="w-4 h-4 animate-spin" /> Gerando e carregando artefatos...
                    </div>
                )}

                {/* ── Results ── */}
                {(artefatos || geoData) && (
                    <div className="mt-6 grid gap-4">
                        {/* KPI cards */}
                        {totaisTransferencia && (
                            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                                {[
                                    {
                                        label: "Rotas transf.",
                                        value: formatNumber(totaisTransferencia.rotas),
                                    },
                                    {
                                        label: "Entregas transf.",
                                        value: formatNumber(totaisTransferencia.entregas),
                                    },
                                    {
                                        label: "Paradas transf.",
                                        value: formatNumber(totaisTransferencia.paradas),
                                    },
                                    {
                                        label: "Peso transf.",
                                        value: `${formatNumber(totaisTransferencia.peso_total_kg)} kg`,
                                    },
                                    {
                                        label: "Volumes transf.",
                                        value: formatNumber(totaisTransferencia.volumes),
                                    },
                                    {
                                        label: "Dist. ida",
                                        value: `${formatDecimal(totaisTransferencia.distancia_ida_km)} km`,
                                    },
                                    {
                                        label: "Dist. total",
                                        value: `${formatDecimal(totaisTransferencia.distancia_total_km)} km`,
                                    },
                                    {
                                        label: "Tempo total",
                                        value: `${formatDecimal(totaisTransferencia.tempo_total_min)} min`,
                                    },
                                ].map((kpi) => (
                                    <div
                                        key={kpi.label}
                                        className="rounded-lg border bg-white p-3 text-center"
                                    >
                                        <p className="text-xs text-slate-500 uppercase tracking-wide">
                                            {kpi.label}
                                        </p>
                                        <p className="text-xl font-bold text-slate-800 mt-0.5">
                                            {kpi.value}
                                        </p>
                                    </div>
                                ))}
                            </div>
                        )}

                        {/* Hub Central info */}
                        {validacaoHub && validacaoHub.entregas > 0 && (
                            <div className="rounded-lg border border-amber-200 bg-amber-50 p-4">
                                <div className="flex flex-wrap items-start justify-between gap-3">
                                    <div>
                                        <p className="text-sm font-semibold text-amber-900">
                                            Hub Central 9999 — validação de somatório
                                        </p>
                                        <p className="text-xs text-amber-800 mt-1">
                                            Entregas já no Hub Central. Não geram rota de transferência e não
                                            aparecem no mapa.
                                        </p>
                                    </div>
                                    <div className="grid grid-cols-3 gap-2 text-right">
                                        <div>
                                            <p className="text-[11px] uppercase text-amber-700">CTEs</p>
                                            <p className="font-semibold text-amber-950">
                                                {formatNumber(validacaoHub.entregas)}
                                            </p>
                                        </div>
                                        <div>
                                            <p className="text-[11px] uppercase text-amber-700">Peso</p>
                                            <p className="font-semibold text-amber-950">
                                                {formatDecimal(validacaoHub.peso_total_kg, 2)} kg
                                            </p>
                                        </div>
                                        <div>
                                            <p className="text-[11px] uppercase text-amber-700">Volumes</p>
                                            <p className="font-semibold text-amber-950">
                                                {formatNumber(validacaoHub.volumes)}
                                            </p>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        )}

                        {/* Tab bar */}
                        <div className="flex border-b border-slate-200 -mb-px">
                            <button onClick={() => setActiveTab("mapa")} className={tabCls("mapa")}>
                                <Map className="w-4 h-4" /> Mapa
                            </button>
                            <button onClick={() => setActiveTab("rotas")} className={tabCls("rotas")}>
                                <Route className="w-4 h-4" /> Rotas
                                {rotasTransferencia.length > 0 && (
                                    <span className="ml-1 rounded-full bg-slate-100 px-1.5 py-0.5 text-[11px] text-slate-600">
                                        {rotasTransferencia.length}
                                    </span>
                                )}
                            </button>
                            <button
                                onClick={() => setActiveTab("downloads")}
                                className={tabCls("downloads")}
                            >
                                <FileText className="w-4 h-4" /> Downloads
                            </button>
                        </div>

                        {/* ── Mapa tab ── */}
                        {activeTab === "mapa" && (
                            <div className="rounded-xl border overflow-hidden bg-white">
                                {geoLoading && (
                                    <div className="flex items-center justify-center h-40 text-slate-400 gap-2 text-sm">
                                        <Loader2 className="w-5 h-5 animate-spin" /> Carregando mapa...
                                    </div>
                                )}
                                {!geoLoading && geoData && geoData.features.length > 0 && (
                                    <TransferMap
                                        geoData={geoData}
                                        selectedRoute={selectedRoute}
                                        onSelectRoute={setSelectedRoute}
                                    />
                                )}
                                {!geoLoading && (!geoData || geoData.features.length === 0) && (
                                    <div className="flex items-center justify-center h-40 text-slate-400 text-sm">
                                        Nenhuma rota disponível para visualização.
                                    </div>
                                )}
                            </div>
                        )}

                        {/* ── Rotas tab ── */}
                        {activeTab === "rotas" && (
                            <div className="rounded-lg border bg-white overflow-hidden">
                                {rotasTransferencia.length > 0 ? (
                                    <>
                                        <div className="px-4 py-2 border-b bg-slate-50">
                                            <p className="text-xs text-slate-500">
                                                Clique em uma rota para destacá-la no mapa
                                            </p>
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
                                                        <th className="px-3 py-2 text-right">Aproveit.</th>
                                                    </tr>
                                                </thead>
                                                <tbody className="divide-y divide-slate-100">
                                                    {rotasTransferencia.map((rota) => {
                                                        const color =
                                                            routeColorMap[rota.rota_transf] || "#94a3b8";
                                                        const isSelected =
                                                            selectedRoute === rota.rota_transf;
                                                        return (
                                                            <tr
                                                                key={rota.rota_transf}
                                                                onClick={() => {
                                                                    setSelectedRoute(
                                                                        isSelected
                                                                            ? null
                                                                            : rota.rota_transf,
                                                                    );
                                                                    setActiveTab("mapa");
                                                                }}
                                                                className={`cursor-pointer transition hover:bg-slate-50 ${isSelected ? "bg-orange-50" : ""}`}
                                                            >
                                                                <td className="px-3 py-2">
                                                                    <div className="flex items-center gap-2">
                                                                        <span
                                                                            style={{
                                                                                background: color,
                                                                                width: 10,
                                                                                height: 10,
                                                                                borderRadius: "50%",
                                                                                display: "inline-block",
                                                                                flexShrink: 0,
                                                                            }}
                                                                        />
                                                                        <span className="font-medium text-slate-800">
                                                                            {rota.rota_transf}
                                                                        </span>
                                                                    </div>
                                                                </td>
                                                                <td className="px-3 py-2 text-slate-600">
                                                                    {rota.tipo_veiculo || "-"}
                                                                </td>
                                                                <td className="px-3 py-2 text-right">
                                                                    {formatNumber(rota.quantidade_entregas)}
                                                                </td>
                                                                <td className="px-3 py-2 text-right">
                                                                    {formatNumber(rota.clusters_qde)}
                                                                </td>
                                                                <td className="px-3 py-2 text-right">
                                                                    {formatDecimal(
                                                                        rota.peso_total_kg ||
                                                                            rota.cte_peso ||
                                                                            0,
                                                                        2,
                                                                    )}
                                                                </td>
                                                                <td className="px-3 py-2 text-right">
                                                                    {formatDecimal(
                                                                        rota.distancia_total_km,
                                                                        1,
                                                                    )}{" "}
                                                                    km
                                                                </td>
                                                                <td className="px-3 py-2 text-right">
                                                                    {formatDecimal(
                                                                        rota.tempo_total_min,
                                                                        1,
                                                                    )}{" "}
                                                                    min
                                                                </td>
                                                                <td className="px-3 py-2 text-right text-slate-500">
                                                                    {rota.aproveitamento_percentual != null
                                                                        ? `${formatDecimal(rota.aproveitamento_percentual, 1)}%`
                                                                        : "-"}
                                                                </td>
                                                            </tr>
                                                        );
                                                    })}
                                                </tbody>
                                            </table>
                                        </div>
                                    </>
                                ) : (
                                    <p className="p-6 text-sm text-slate-500 text-center">
                                        Nenhuma rota de transferência disponível.
                                    </p>
                                )}
                            </div>
                        )}

                        {/* ── Downloads tab ── */}
                        {activeTab === "downloads" && (
                            <div className="rounded-lg border bg-slate-50 p-4 grid gap-2">
                                {artefatos?.map_html_url && (
                                    <a
                                        href={resolveUrl(artefatos.map_html_url) || "#"}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="flex items-center gap-2 rounded-lg border bg-white px-4 py-3 text-sm text-emerald-700 hover:bg-emerald-50 transition"
                                    >
                                        <Map className="w-4 h-4 shrink-0" />
                                        <span>Mapa interativo (HTML) — rota via malha viária</span>
                                    </a>
                                )}
                                {artefatos?.map_png_url && (
                                    <a
                                        href={resolveUrl(artefatos.map_png_url) || "#"}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="flex items-center gap-2 rounded-lg border bg-white px-4 py-3 text-sm text-emerald-700 hover:bg-emerald-50 transition"
                                    >
                                        <Map className="w-4 h-4 shrink-0" />
                                        <span>Mapa estático (PNG)</span>
                                    </a>
                                )}
                                {artefatos?.pdf_url && (
                                    <a
                                        href={resolveUrl(artefatos.pdf_url) || "#"}
                                        target="_blank"
                                        rel="noreferrer"
                                        className="flex items-center gap-2 rounded-lg border bg-white px-4 py-3 text-sm text-emerald-700 hover:bg-emerald-50 transition"
                                    >
                                        <FileText className="w-4 h-4 shrink-0" />
                                        <span>Relatório consolidado (PDF)</span>
                                    </a>
                                )}
                                {!artefatos?.map_html_url &&
                                    !artefatos?.map_png_url &&
                                    !artefatos?.pdf_url && (
                                        <p className="text-sm text-slate-500">
                                            Nenhum artefato gerado ainda. Use o botão{" "}
                                            <strong>PDF</strong> para gerar os arquivos.
                                        </p>
                                    )}
                            </div>
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}
