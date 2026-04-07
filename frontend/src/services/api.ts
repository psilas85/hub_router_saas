//hub_router_1.0.1/frontend/src/services/api.ts
import axios from "axios";
import { useAuthStore } from "@/store/authStore";

const baseURL = import.meta.env.VITE_API_URL;

// 👉 log para checar no console do navegador
console.log("🌍 API baseURL configurado:", baseURL);

const api = axios.create({
    baseURL,
    timeout: 1_200_000, // 1200s = 20 minutos
});

// request → injeta token automaticamente
api.interceptors.request.use((config) => {
    const token = useAuthStore.getState().token;

    console.log("🔐 Token no interceptor:", token);

    if (token) {
        config.headers = config.headers ?? {};
        config.headers.Authorization = `Bearer ${token}`;
    }

    return config;
});

// response → trata erros globalmente
api.interceptors.response.use(
    (res) => res,
    (err) => {
        const status = err.response?.status;

        if (status === 401) {
            console.warn("⚠️ 401 recebido:", err.config?.url);

            // 🔥 só desloga se for rota de autenticação
            if (err.config?.url?.includes("/auth")) {
                useAuthStore.getState().logout();
                window.location.href = "/login";
            }
        } else if (status === 403) {
            alert("🚫 Você não tem permissão para acessar este recurso.");
        } else if (status === 404) {
            alert("❌ Recurso não encontrado.");
        } else if (status >= 500) {
            alert("💥 Erro interno no servidor. Tente novamente mais tarde.");
        } else if (err.code === "ECONNABORTED") {
            alert("⏳ A requisição demorou demais e foi cancelada.");
        } else if (!err.response) {
            alert("🌐 Não foi possível conectar ao servidor. Verifique sua rede.");
        }

        return Promise.reject(err);
    }
);

export default api;
