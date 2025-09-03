//hub_router_1.0.1/frontend/src/components/Navbar.tsx

import { Link, NavLink, useNavigate } from "react-router-dom";
import { useAuthStore } from "@/store/authStore";
import { useEffect, useRef, useState } from "react";

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

/** Hook simples para fechar um menu ao clicar fora */
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

    const [menuOpen, setMenuOpen] = useState(false); // menu mobile
    const [mmOpen, setMmOpen] = useState(false); // dropdown Middle-Mile
    const [lmOpen, setLmOpen] = useState(false); // dropdown Last-Mile

    const mmRef = useRef<HTMLElement | null>(null);
    const lmRef = useRef<HTMLElement | null>(null);
    useClickOutside(mmRef, () => setMmOpen(false));
    useClickOutside(lmRef, () => setLmOpen(false));

    const handleLogout = () => {
        logout();
        navigate("/login", { replace: true });
    };

    const podeGerenciarUsuarios =
        usuario?.role === "hub_admin" || usuario?.role === "cliente_admin";
    const podeGerenciarTenants = usuario?.role === "hub_admin";

    const linkBase =
        "px-3 py-2 rounded-md text-emerald-50/90 hover:text-white hover:bg-emerald-700 focus:outline-none focus:ring-2 focus:ring-white/60";
    const active = "bg-emerald-700 text-white shadow-sm";

    return (
        <nav className="bg-emerald-600 text-white px-6 py-3 shadow-md z-50 relative">
            <div className="mx-auto max-w-7xl flex justify-between items-center">
                {/* Logo + nome → link para Home */}
                <Link
                    to="/"
                    className="flex items-center space-x-2 transition-opacity hover:opacity-80"
                >
                    <img src="/hubrouter_icone.png" alt="HubRouter" className="h-7 w-7" />
                    <span className="font-semibold tracking-tight text-white">
                        HubRouter
                    </span>
                </Link>

                {/* Links desktop */}
                <div className="hidden md:flex items-center gap-2">
                    <NavLink
                        to="/data-input"
                        className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
                    >
                        Data Input
                    </NavLink>
                    <NavLink
                        to="/clusterization"
                        className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
                    >
                        Clusterização
                    </NavLink>

                    {/* Middle-Mile (dropdown) */}
                    <div
                        className="relative"
                        ref={mmRef as React.RefObject<HTMLDivElement>}
                    >
                        <button
                            onClick={() => setMmOpen((v) => !v)}
                            className={`${linkBase} flex items-center gap-1`}
                            aria-haspopup="true"
                            aria-expanded={mmOpen}
                        >
                            Middle-Mile <span className="text-xs">▾</span>
                        </button>

                        {mmOpen && (
                            <div
                                className="absolute mt-2 left-0 bg-white text-gray-800 rounded-xl shadow-lg w-56 py-2 z-[60]"
                                role="menu"
                            >
                                <NavLink
                                    to="/middle-mile/vehicles"
                                    className={({ isActive }) =>
                                        `block px-4 py-2 rounded-lg hover:bg-gray-100 ${isActive ? "bg-gray-100" : ""
                                        }`
                                    }
                                    onClick={() => setMmOpen(false)}
                                >
                                    Cadastro Veículos
                                </NavLink>
                                <NavLink
                                    to="/middle-mile/routing"
                                    className={({ isActive }) =>
                                        `block px-4 py-2 rounded-lg hover:bg-gray-100 ${isActive ? "bg-gray-100" : ""
                                        }`
                                    }
                                    onClick={() => setMmOpen(false)}
                                >
                                    Roteirização
                                </NavLink>
                                <NavLink
                                    to="/middle-mile/costs"
                                    className={({ isActive }) =>
                                        `block px-4 py-2 rounded-lg hover:bg-gray-100 ${isActive ? "bg-gray-100" : ""
                                        }`
                                    }
                                    onClick={() => setMmOpen(false)}
                                >
                                    Custeio
                                </NavLink>
                            </div>
                        )}
                    </div>

                    {/* Last-Mile (dropdown) */}
                    <div
                        className="relative"
                        ref={lmRef as React.RefObject<HTMLDivElement>}
                    >
                        <button
                            onClick={() => setLmOpen((v) => !v)}
                            className={`${linkBase} flex items-center gap-1`}
                            aria-haspopup="true"
                            aria-expanded={lmOpen}
                        >
                            Last-Mile <span className="text-xs">▾</span>
                        </button>

                        {lmOpen && (
                            <div
                                className="absolute mt-2 left-0 bg-white text-gray-800 rounded-xl shadow-lg w-56 py-2 z-[60]"
                                role="menu"
                            >
                                <NavLink
                                    to="/last-mile/vehicles"
                                    className={({ isActive }) =>
                                        `block px-4 py-2 rounded-lg hover:bg-gray-100 ${isActive ? "bg-gray-100" : ""
                                        }`
                                    }
                                    onClick={() => setLmOpen(false)}
                                >
                                    Cadastro Veículos
                                </NavLink>
                                <NavLink
                                    to="/last-mile/routing"
                                    className={({ isActive }) =>
                                        `block px-4 py-2 rounded-lg hover:bg-gray-100 ${isActive ? "bg-gray-100" : ""
                                        }`
                                    }
                                    onClick={() => setLmOpen(false)}
                                >
                                    Roteirização
                                </NavLink>
                                <NavLink
                                    to="/last-mile/costs"
                                    className={({ isActive }) =>
                                        `block px-4 py-2 rounded-lg hover:bg-gray-100 ${isActive ? "bg-gray-100" : ""
                                        }`
                                    }
                                    onClick={() => setLmOpen(false)}
                                >
                                    Custeio
                                </NavLink>
                            </div>
                        )}
                    </div>
                    <NavLink
                        to="/simulation"
                        className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
                    >
                        Simulação
                    </NavLink>
                    <NavLink
                        to="/planner"
                        className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
                    >
                        Planner
                    </NavLink>
                    {podeGerenciarUsuarios && (
                        <NavLink
                            to="/users"
                            className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
                        >
                            Usuários
                        </NavLink>
                    )}

                    {podeGerenciarTenants && (
                        <NavLink
                            to="/tenants"
                            className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
                        >
                            Tenants
                        </NavLink>
                    )}

                    <NavLink
                        to="/profile"
                        className={({ isActive }) => `${linkBase} ${isActive ? active : ""}`}
                    >
                        Perfil
                    </NavLink>

                    {/* Badge usuário + role */}
                    <span className="ml-3 text-sm flex items-center gap-2 bg-emerald-700 px-2 py-1 rounded-lg">
                        <span>👤 {usuario?.nome ?? "Usuário"}</span>
                        <span className="text-xs bg-emerald-500/50 px-2 py-0.5 rounded">
                            {labelRole(usuario?.role as Role)}
                        </span>
                    </span>

                    <button
                        onClick={handleLogout}
                        className="ml-3 bg-red-500 hover:bg-red-600 text-white px-3 py-2 rounded-md focus:outline-none focus:ring-2 focus:ring-white/60"
                    >
                        Sair
                    </button>
                </div>

                {/* Botão hambúrguer mobile */}
                <button
                    onClick={() => setMenuOpen(!menuOpen)}
                    className="md:hidden inline-flex flex-col gap-1 focus:outline-none focus:ring-2 focus:ring-white/60"
                >
                    <span className="w-6 h-0.5 bg-white" />
                    <span className="w-6 h-0.5 bg-white" />
                    <span className="w-6 h-0.5 bg-white" />
                </button>
            </div>

            {/* Menu mobile */}
            {menuOpen && (
                <div className="md:hidden bg-emerald-700/95 backdrop-blur">
                    <div className="px-4 py-3 flex flex-col gap-2">
                        <Link to="/data-input" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Data Input
                        </Link>
                        <Link to="/clusterization" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Clusterização
                        </Link>

                        {/* Middle-Mile mobile */}
                        <Link to="/middle-mile/vehicles" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Middle-Mile – Cadastro Veículos
                        </Link>
                        <Link to="/middle-mile/routing" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Middle-Mile – Roteirização
                        </Link>
                        <Link to="/middle-mile/costs" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Middle-Mile – Custeio
                        </Link>

                        {/* Last-Mile mobile */}
                        <Link to="/last-mile/vehicles" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Last-Mile – Cadastro Veículos
                        </Link>
                        <Link to="/last-mile/routing" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Last-Mile – Roteirização
                        </Link>
                        <Link to="/last-mile/costs" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Last-Mile – Custeio
                        </Link>
                        <Link
                            to="/simulation"
                            onClick={() => setMenuOpen(false)}
                            className={linkBase}
                        >
                            Simulação
                        </Link>

                        <Link
                            to="/planner"
                            onClick={() => setMenuOpen(false)}
                            className={linkBase}
                        >
                            Planner
                        </Link>


                        {podeGerenciarUsuarios && (
                            <Link to="/users" onClick={() => setMenuOpen(false)} className={linkBase}>
                                Usuários
                            </Link>
                        )}
                        {podeGerenciarTenants && (
                            <Link to="/tenants" onClick={() => setMenuOpen(false)} className={linkBase}>
                                Tenants
                            </Link>
                        )}
                        <Link to="/profile" onClick={() => setMenuOpen(false)} className={linkBase}>
                            Perfil
                        </Link>

                        <div className="text-sm text-emerald-50/90 mt-1 flex items-center gap-2">
                            <span>👤 {usuario?.nome ?? "Usuário"}</span>
                            <span className="text-xs bg-emerald-500/50 px-2 py-0.5 rounded">
                                {labelRole(usuario?.role as Role)}
                            </span>
                        </div>

                        <button
                            onClick={handleLogout}
                            className="bg-red-500 hover:bg-red-600 text-white px-3 py-2 rounded-md text-left"
                        >
                            Sair
                        </button>
                    </div>
                </div>
            )}
        </nav>
    );
}
