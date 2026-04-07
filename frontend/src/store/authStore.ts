// hub_router_1.0.1/frontend/src/store/authStore.ts
import { create } from "zustand";
import { persist } from "zustand/middleware";
import api from "@/services/api";

interface Usuario {
    id: string;
    nome: string;
    email: string;
    role: string;
    tenant_id?: string | null;
}

interface Tenant {
    id: string;
    nome: string;
    plano: string;
}

interface AuthState {
    token: string | null;
    usuario: Usuario | null;
    tenant: Tenant | null;
    login: (email: string, senha: string) => Promise<void>;
    logout: () => void;
    isTokenExpired: () => boolean;
}

function decodeJwt(token: string): any {
    try {
        const payload = token.split(".")[1];
        return JSON.parse(atob(payload));
    } catch (e) {
        console.error("❌ Erro ao decodificar token:", e);
        return null;
    }
}

// 🔑 Função de normalização de role
function normalizeRole(role: string): string {
    if (!role) return "";
    const r = role.trim().toLowerCase();
    if (r === "admin") return "hub_admin";
    if (r === "client_admin") return "cliente_admin";
    return r;
}

export const useAuthStore = create<AuthState>()(
    persist(
        (set, get) => ({
            token: null,
            usuario: null,
            tenant: null,

            login: async (email, senha) => {
                try {
                    const { data } = await api.post("/auth/login", { email, senha });

                    const jwt = data.access_token || data.token;

                    if (!jwt) {
                        console.error("❌ Login sem token na resposta:", data);
                        throw new Error("Token não retornado pelo backend");
                    }

                    const tenant =
                        data.tenant && Object.keys(data.tenant).length > 0
                            ? data.tenant
                            : null;

                    const usuario: Usuario = {
                        ...data.usuario,
                        role: normalizeRole(data.usuario.role),
                    };

                    set({
                        token: jwt,
                        usuario,
                        tenant,
                    });

                    console.log("✅ Login bem-sucedido:", { usuario, tenant });
                    console.log("📌 Role normalizado:", usuario.role);

                    const decoded = decodeJwt(jwt);
                    if (decoded?.exp) {
                        const expiresIn = decoded.exp * 1000 - Date.now();
                        console.log("⏳ Token expira em:", expiresIn / 1000, "segundos");

                        setTimeout(() => {
                            if (get().token) {
                                console.warn("⚠️ Token expirou, deslogando...");
                                get().logout();
                            }
                        }, Math.max(expiresIn, 0));
                    }
                } catch (error) {
                    console.error("❌ Erro no login:", error);
                    throw error;
                }
            },

            logout: () => {
                console.log("👋 Logout executado, limpando store");
                set({ token: null, usuario: null, tenant: null });
                localStorage.removeItem("auth-storage"); // garante limpeza
            },

            isTokenExpired: () => {
                const token = get().token;
                if (!token) return true;

                const decoded = decodeJwt(token);
                if (!decoded?.exp) return true;

                return Date.now() >= decoded.exp * 1000;
            },
        }),
        {
            name: "auth-storage",
            version: 3, // 👈 incrementa versão por causa da mudança de role
            migrate: (persistedState: any, version: number) => {
                if (version < 3) {
                    console.log("🧹 Limpando storage antigo por mudança de role");
                    return { token: null, usuario: null, tenant: null };
                }
                return persistedState as AuthState;
            },
        }
    )
);
