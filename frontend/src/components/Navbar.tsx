// hub_router_1.0.1/frontend/src/components/Navbar.tsx
import { Link, NavLink, useNavigate } from "react-router-dom";
import { useAuthStore } from "@/store/authStore";
import { useEffect, useRef, useState, type ReactNode } from "react";
import {
    Upload,
    Network,
    Truck,
    Bike,
    BarChart3,
    Settings,
    LogOut,
    ChevronDown,
    X,
    Menu,
} from "lucide-react";
import { useProcessing } from "@/context/ProcessingContext";  // ✅ novo

type Role =
    | "hub_admin"
    | "hub_operacional"
    | "cliente_admin"
    | "cliente_operacional";

const labelRole = (r: string | undefined) =>
    r === "hub_admin"
        ? "Hub Admin"
        : r === "hub_operacional"
            ? "Hub Operacional"
            : r === "cliente_admin"
                ? "Cliente Admin"
                : r === "cliente_operacional"
                    ? "Cliente Operacional"
                    : "-";

/** Hook simples para fechar dropdown/drawer ao clicar fora */
function useClickOutside(
    ref: React.RefObject<HTMLElement | null>,
    onOutside: () => void
) {
    useEffect(() => {
        function handler(e: MouseEvent) {
            const el = ref.current;
            if (!el) return;
            if (!el.contains(e.target as Node)) onOutside();
        }
        document.addEventListener("mousedown", handler);
        return () => document.removeEventListener("mousedown", handler);
    }, [ref, onOutside]);
}

export default function Navbar() {
    const { usuario, logout } = useAuthStore();
    const navigate = useNavigate();
    const { processing } = useProcessing(); // ✅ pega status global

    // Drawer mobile
    const [drawerOpen, setDrawerOpen] = useState(false);

    // Submenus
    const [mmOpen, setMmOpen] = useState(false); // Middle-Mile
    const [lmOpen, setLmOpen] = useState(false); // Last-Mile
    const [simOpen, setSimOpen] = useState(false); // Simulação
    const [adminOpen, setAdminOpen] = useState(false); // Admin

    const drawerRef = useRef<HTMLElement | null>(null);
    useClickOutside(drawerRef, () => setDrawerOpen(false));

    const handleLogout = () => {
        logout();
        navigate("/login", { replace: true });
    };

    const podeGerenciarUsuarios =
        usuario?.role === "hub_admin" || usuario?.role === "cliente_admin";
    const podeGerenciarTenants = usuario?.role === "hub_admin";

    const linkBase =
        "flex items-center gap-2 px-3 py-2 rounded-lg text-emerald-50/90 hover:text-white hover:bg-emerald-700 focus:outline-none focus:ring-2 focus:ring-white/60 transition-colors";
    const active = "bg-emerald-800 text-white shadow-sm";

    // Componente de link padrão com active state
    const Item = ({
        to,
        icon,
        children,
        onClick,
    }: {
        to: string;
        icon: ReactNode;
        children: ReactNode;
        onClick?: () => void;
    }) => (
        <NavLink
            to={to}
            onClick={onClick}
            className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
        >
            {icon}
            <span className="truncate">{children}</span>
        </NavLink>
    );

    // Submenu reutilizável
    const SubMenu = ({
        title,
        icon,
        open,
        setOpen,
        children,
    }: {
        title: string;
        icon: ReactNode;
        open: boolean;
        setOpen: (v: boolean | ((p: boolean) => boolean)) => void;
        children: ReactNode;
    }) => (
        <div className="mb-1">
            <button
                onClick={() => setOpen((v) => !v)}
                className={`${linkBase} w-full justify-between`}
                aria-expanded={open}
            >
                <span className="flex items-center gap-2">
                    {icon}
                    <span>{title}</span>
                </span>
                <ChevronDown
                    size={16}
                    className={`transition-transform ${open ? "rotate-180" : ""}`}
                />
            </button>
            {open && <div className="mt-1 ml-2 flex flex-col gap-1">{children}</div>}
        </div>
    );

    const SIDEBAR_W = "w-64";

    // Sidebar (desktop e drawer mobile)
    const SidebarContent = (
        <aside
            ref={drawerRef as React.RefObject<HTMLElement>}
            className={`flex h-full flex-col bg-emerald-600 text-white ${SIDEBAR_W}`}
        >
            {/* Cabeçalho / Logo */}
            <div className="flex items-center gap-2 px-4 py-3 border-b border-white/10">
                <img src="/hubrouter_icone.png" alt="HubRouter" className="h-7 w-7" />
                <Link
                    to="/"
                    className="font-semibold tracking-tight hover:opacity-90"
                    onClick={() => setDrawerOpen(false)}
                >
                    HubRouter
                </Link>
                {/* Fechar (mobile) */}
                <button
                    className="ml-auto md:hidden p-1 rounded hover:bg-emerald-700"
                    onClick={() => setDrawerOpen(false)}
                    aria-label="Fechar menu"
                >
                    <X size={18} />
                </button>
            </div>

            {/* Navegação */}
            <nav className="flex-1 overflow-y-auto px-3 py-3 space-y-1">
                {/* Input de Dados (com badge de processamento) */}
                <Item
                    to="/data-input"
                    icon={<Upload size={16} />}
                    onClick={() => setDrawerOpen(false)}
                >
                    <span className="flex items-center gap-2">
                        Input de Dados
                        {processing && (
                            <span className="ml-2 text-xs text-amber-300 animate-pulse">
                                ⏳
                            </span>
                        )}
                    </span>
                </Item>

                {/* EDA */}
                <Item
                    to="/exploratory_analysis_ui"
                    icon={<BarChart3 size={16} />}
                    onClick={() => setDrawerOpen(false)}
                >
                    EDA
                </Item>

                <Item
                    to="/clusterization"
                    icon={<Network size={16} />}
                    onClick={() => setDrawerOpen(false)}
                >
                    Clusterização
                </Item>

                {/* Middle-Mile */}
                <SubMenu title="Middle-Mile" icon={<Truck size={16} />} open={mmOpen} setOpen={setMmOpen}>
                    <Item
                        to="/middle-mile/vehicles"
                        icon={<span className="text-base leading-none">🚗</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Cadastro Veículos
                    </Item>
                    <Item
                        to="/middle-mile/routing"
                        icon={<span className="text-base leading-none">🛣️</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Roteirização
                    </Item>
                    <Item
                        to="/middle-mile/costs"
                        icon={<span className="text-base leading-none">💰</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Custeio
                    </Item>
                </SubMenu>

                {/* Last-Mile */}
                <SubMenu title="Last-Mile" icon={<Bike size={16} />} open={lmOpen} setOpen={setLmOpen}>
                    <Item
                        to="/last-mile/vehicles"
                        icon={<span className="text-base leading-none">🚚</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Cadastro Veículos
                    </Item>
                    <Item
                        to="/last-mile/routing"
                        icon={<span className="text-base leading-none">🛵</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Roteirização
                    </Item>
                    <Item
                        to="/last-mile/costs"
                        icon={<span className="text-base leading-none">💲</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Custeio
                    </Item>
                </SubMenu>

                {/* Simulação */}
                <SubMenu title="Simulação" icon={<BarChart3 size={16} />} open={simOpen} setOpen={setSimOpen}>
                    <Item
                        to="/simulation"
                        icon={<span className="text-base leading-none">▶️</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Processamento
                    </Item>
                    <Item
                        to="/simulation/hubs"
                        icon={<span className="text-base leading-none">📍</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Cadastro de Hubs
                    </Item>
                    <Item
                        to="/simulation/cluster_costs"
                        icon={<span className="text-base leading-none">💰</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Custos de Centros
                    </Item>
                    <Item
                        to="/simulation/lastmile_vehicles"
                        icon={<span className="text-base leading-none">🚐</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Veículos Last-Mile
                    </Item>
                    <Item
                        to="/simulation/transfer_vehicles"
                        icon={<span className="text-base leading-none">🚛</span>}
                        onClick={() => setDrawerOpen(false)}
                    >
                        Veículos Transferência
                    </Item>
                </SubMenu>

                {/* Admin */}
                {(podeGerenciarUsuarios || podeGerenciarTenants) && (
                    <SubMenu title="Admin" icon={<Settings size={16} />} open={adminOpen} setOpen={setAdminOpen}>
                        {podeGerenciarUsuarios && (
                            <Item
                                to="/users"
                                icon={<span className="text-base leading-none">👥</span>}
                                onClick={() => setDrawerOpen(false)}
                            >
                                Usuários
                            </Item>
                        )}
                        {podeGerenciarTenants && (
                            <Item
                                to="/tenants"
                                icon={<span className="text-base leading-none">🏢</span>}
                                onClick={() => setDrawerOpen(false)}
                            >
                                Tenants
                            </Item>
                        )}
                        <Item
                            to="/profile"
                            icon={<span className="text-base leading-none">👤</span>}
                            onClick={() => setDrawerOpen(false)}
                        >
                            Perfil
                        </Item>
                    </SubMenu>
                )}
            </nav>

            {/* Rodapé com usuário e logout */}
            <div className="mt-auto border-t border-white/10 p-3">
                <div className="flex items-center justify-between rounded-lg bg-emerald-700 px-3 py-2">
                    <div className="flex min-w-0 items-center gap-2">
                        <span className="shrink-0">👤</span>
                        <div className="min-w-0">
                            <p className="text-sm leading-tight truncate">{usuario?.nome ?? "Usuário"}</p>
                            <p className="text-xs opacity-90">
                                <span className="bg-emerald-500/50 px-2 py-0.5 rounded">
                                    {labelRole(usuario?.role as Role)}
                                </span>
                            </p>
                        </div>
                    </div>
                    <button
                        onClick={handleLogout}
                        className="ml-2 bg-red-500 hover:bg-red-600 text-white px-3 py-2 rounded-md flex items-center gap-2"
                    >
                        <LogOut size={16} />
                        <span className="hidden lg:inline">Sair</span>
                    </button>
                </div>
            </div>
        </aside>
    );

    return (
        <>
            {/* Topbar (mobile) */}
            <div className="md:hidden sticky top-0 z-50 bg-emerald-600 text-white px-4 py-2 shadow-md">
                <div className="flex items-center justify-between">
                    <button
                        onClick={() => setDrawerOpen(true)}
                        className="inline-flex items-center gap-2 rounded px-2 py-1 hover:bg-emerald-700"
                        aria-label="Abrir menu"
                    >
                        <Menu size={20} />
                        <span>Menu</span>
                    </button>
                    <Link to="/" className="flex items-center gap-2 hover:opacity-90">
                        <img src="/hubrouter_icone.png" alt="HubRouter" className="h-7 w-7" />
                        <span className="font-semibold tracking-tight">HubRouter</span>
                    </Link>
                </div>
            </div>

            {/* Sidebar fixa no desktop */}
            <div className="hidden md:block fixed inset-y-0 left-0">{SidebarContent}</div>

            {/* Drawer mobile */}
            {drawerOpen && (
                <div className="md:hidden">
                    <div className="fixed inset-0 bg-black/40 z-40" />
                    <div className="fixed inset-y-0 left-0 z-50">{SidebarContent}</div>
                </div>
            )}
        </>
    );
}
