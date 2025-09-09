// hub_router_1.0.1/frontend/src/pages/Home/HomePage.tsx

import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuthStore } from "@/store/authStore";
import MarkerClusterGroup from "react-leaflet-cluster";

import {
    LayoutGrid,
    Upload,
    Sparkles,
    Network,
    Package,
} from "lucide-react";
import {
    ResponsiveContainer,
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
} from "recharts";
import {
    MapContainer,
    TileLayer,
    CircleMarker,
    Popup,
    useMap,
} from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { fmtCompact } from "@/utils/format";
import L from "leaflet";

// 🔧 Força API Gateway (porta 8010)
let apiBase = import.meta.env.VITE_API_URL || window.location.origin;
try {
    const u = new URL(apiBase);
    u.port = "8010"; // sempre usar porta 8010 do gateway
    apiBase = u.toString().replace(/\/+$/, "");
} catch {
    apiBase = "http://localhost:8010";
}
const API_URL = apiBase;
console.log("🌍 Home API_URL:", API_URL);

export default function HomePage() {
    const navigate = useNavigate();
    const usuario = useAuthStore((s) => (s as any).user);
    const token = useAuthStore((s) => (s as any).token);

    const [loading, setLoading] = useState(true);

    const [entregas30d, setEntregas30d] = useState<any[]>([]);
    const [entregas12m, setEntregas12m] = useState<any[]>([]);
    const [entregasMapa, setEntregasMapa] = useState<any[]>([]);

    useEffect(() => {
        const headers = { Authorization: `Bearer ${token}` };

        async function safeGet<T>(url: string): Promise<T | []> {
            try {
                const res = await fetch(url, { headers });
                if (!res.ok) return [];
                const txt = await res.text();
                if (!txt) return [];
                return JSON.parse(txt);
            } catch {
                return [];
            }
        }

        async function carregar() {
            setLoading(true);
            setEntregas30d(
                await safeGet<any[]>(`${API_URL}/data_input/dashboard/ultimos-30-dias`)
            );
            setEntregas12m(
                await safeGet<any[]>(`${API_URL}/data_input/dashboard/mensal`)
            );
            setEntregasMapa(
                await safeGet<any[]>(`${API_URL}/data_input/dashboard/mapa`)
            );
            setLoading(false);
        }

        carregar();
    }, [token]);

    return (
        <div className="p-6 md:p-8 space-y-6">
            {/* HEADER */}
            <div className="flex items-center justify-between flex-wrap gap-4">
                <div>
                    <h1 className="text-2xl md:text-3xl font-semibold tracking-tight flex items-center gap-2">
                        <LayoutGrid className="h-7 w-7" /> HubRouter — Visão Geral
                    </h1>
                    <p className="text-sm text-gray-500 mt-1">
                        {usuario?.name ? (
                            <>
                                Olá, <span className="font-medium">{usuario.name}</span>
                            </>
                        ) : (
                            "Bem-vindo ao HubRouter"
                        )}
                    </p>
                </div>
                <span className="inline-flex items-center rounded-md border border-gray-300 bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-700">
                    v{import.meta.env.VITE_APP_BUILD ?? "local"}
                </span>
            </div>

            {/* AÇÕES RÁPIDAS */}
            <section className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <ActionCard
                    title="Upload de Dados"
                    description="Envie o CSV e valide colunas."
                    icon={<Upload className="h-5 w-5 text-emerald-600" />}
                    onClick={() => navigate("/data-input")}
                />
                <ActionCard
                    title="Explorar Entregas (EDA)"
                    description="Boxplots, rankings e mapas."
                    icon={<Sparkles className="h-5 w-5 text-purple-600" />}
                    onClick={() => navigate("/eda")}
                />
                <ActionCard
                    title="Simulação"
                    description="Compare cenários de custos."
                    icon={<Network className="h-5 w-5 text-blue-600" />}
                    onClick={() => navigate("/simulation")}
                />
            </section>

            {/* GRÁFICOS + MAPA */}
            <section className="grid grid-cols-1 xl:grid-cols-2 gap-4 h-[600px]">
                {/* Coluna esquerda: gráficos */}
                <div className="flex flex-col space-y-4">
                    <Card>
                        <CardHeader>
                            <CardTitle>Entregas (últimos 30 dias)</CardTitle>
                        </CardHeader>
                        <CardContent className="h-64">
                            {loading ? (
                                <Skeleton />
                            ) : entregas30d.length > 0 ? (
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={entregas30d}>
                                        <XAxis dataKey="dia" />
                                        <YAxis />
                                        <Tooltip formatter={(v: number) => fmtCompact(v)} />
                                        <Bar dataKey="entregas" fill="#10b981" />
                                    </BarChart>
                                </ResponsiveContainer>
                            ) : (
                                <EmptyMessage />
                            )}
                        </CardContent>
                    </Card>

                    <Card>
                        <CardHeader>
                            <CardTitle>Entregas Mensais (12 meses)</CardTitle>
                        </CardHeader>
                        <CardContent className="h-64">
                            {loading ? (
                                <Skeleton />
                            ) : entregas12m.length > 0 ? (
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={entregas12m}>
                                        <XAxis dataKey="mes" />
                                        <YAxis
                                            label={{
                                                value: "Qtd Entregas",
                                                angle: -90,
                                                position: "insideLeft",
                                            }}
                                        />
                                        <Tooltip formatter={(v: number) => fmtCompact(v)} />
                                        <Bar dataKey="entregas" fill="#3b82f6" />
                                    </BarChart>
                                </ResponsiveContainer>
                            ) : (
                                <EmptyMessage />
                            )}
                        </CardContent>
                    </Card>
                </div>

                {/* Coluna direita: mapa */}
                <Card>
                    <CardHeader>
                        <CardTitle>Mapa — Entregas últimos 30 dias</CardTitle>
                    </CardHeader>
                    <CardContent className="h-full">
                        {loading ? (
                            <Skeleton />
                        ) : entregasMapa.length > 0 ? (
                            <MapContainer
                                style={{ height: "100%", width: "100%" }}
                                zoom={6}
                                center={[-15, -47]} // fallback Brasil
                            >
                                <TileLayer
                                    attribution='&copy; <a href="https://carto.com/">CARTO</a>'
                                    url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
                                />
                                <AutoZoom pontos={entregasMapa} />

                                <MarkerClusterGroup
                                    chunkedLoading
                                    iconCreateFunction={(cluster: any) => {
                                        const count = cluster.getChildCount();
                                        let color = "bg-blue-500";
                                        if (count > 1000) color = "bg-red-500";
                                        else if (count > 500) color = "bg-orange-500";
                                        else if (count > 100) color = "bg-green-500";

                                        return L.divIcon({
                                            html: `<div class="flex items-center justify-center rounded-full ${color} text-white text-xs font-bold w-10 h-10">${count}</div>`,
                                            className: "custom-cluster",
                                            iconSize: [40, 40],
                                        });
                                    }}
                                >
                                    {entregasMapa.map((p, i) => (
                                        <CircleMarker
                                            key={i}
                                            center={[p.lat, p.lon]}
                                            radius={Math.sqrt(Number(p.cte_valor_nf)) / 500}
                                            fillOpacity={0.6}
                                            stroke={false}
                                            color="blue"
                                        >
                                            <Popup>
                                                <b>Data:</b> {p.envio_data}
                                                <br />
                                                <b>Valor NF:</b>{" "}
                                                R$ {Number(p.cte_valor_nf).toLocaleString("pt-BR")}
                                            </Popup>
                                        </CircleMarker>
                                    ))}
                                </MarkerClusterGroup>
                            </MapContainer>
                        ) : (
                            <EmptyMessage icon={<Package className="w-5 h-5 mr-2" />} />
                        )}
                    </CardContent>
                </Card>
            </section>

            {/* FOOTER */}
            <div className="text-xs text-muted-foreground pt-2 border-t">
                HubRouter · © {new Date().getFullYear()} ·{" "}
                <span className="font-medium">
                    Build {import.meta.env.VITE_APP_BUILD ?? "local"}
                </span>
            </div>
        </div>
    );
}

/* === Auxiliares === */

function AutoZoom({ pontos }: { pontos: any[] }) {
    const map = useMap();
    useEffect(() => {
        if (pontos.length > 0) {
            map.fitBounds(pontos.map((p) => [p.lat, p.lon]));
        }
    }, [pontos, map]);
    return null;
}

function Card({ children, className = "" }: { children: React.ReactNode; className?: string }) {
    return <div className={`card ${className}`}>{children}</div>;
}
function CardHeader({ children }: { children: React.ReactNode }) {
    return <div className="mb-2">{children}</div>;
}
function CardContent({ children, className = "" }: { children: React.ReactNode; className?: string }) {
    return <div className={className}>{children}</div>;
}
function CardTitle({ children, className = "" }: { children: React.ReactNode; className?: string }) {
    return <h3 className={`font-semibold ${className}`}>{children}</h3>;
}
function ActionCard({
    title,
    description,
    icon,
    onClick,
}: {
    title: string;
    description: string;
    icon: React.ReactNode;
    onClick: () => void;
}) {
    return (
        <Card className="hover:shadow-md transition-shadow">
            <CardHeader>
                <CardTitle className="text-base flex items-center gap-2">
                    {icon} {title}
                </CardTitle>
            </CardHeader>
            <CardContent className="flex items-center justify-between">
                <p className="text-sm text-gray-500">{description}</p>
                <button
                    onClick={onClick}
                    className="border border-gray-300 text-gray-700 hover:bg-gray-100 px-2 py-1 rounded"
                >
                    Abrir
                </button>
            </CardContent>
        </Card>
    );
}
function Skeleton() {
    return <div className="animate-pulse bg-gray-200 rounded h-full w-full" />;
}
function EmptyMessage({ icon }: { icon?: React.ReactNode }) {
    return (
        <div className="flex items-center justify-center h-full text-gray-400">
            {icon} Sem dados disponíveis
        </div>
    );
}
