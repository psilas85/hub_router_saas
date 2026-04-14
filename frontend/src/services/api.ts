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
        const requestUrl = err.config?.url ?? "";
        const isSimulationStatusPolling = requestUrl.includes("/simulation/status/");

        if (status === 401) {
            console.warn("⚠️ 401 recebido:", err.config?.url);

            if (err.config?.url?.includes("/auth")) {
                useAuthStore.getState().logout();
                window.location.href = "/login";
            }

        } else if (status === 403) {
            alert("🚫 Você não tem permissão.");

        } else if (status === 404) {
            if (!isSimulationStatusPolling) {
                alert("❌ Recurso não encontrado.");
            }

        } else if (status === 422) {
            console.error("🔥 422 RAW:", err.response?.data);
            console.error("🔥 422 FORMATADO:", JSON.stringify(err.response?.data, null, 2));

            alert("❌ Erro de validação (422). Veja console.");
        } else if (status >= 500) {
            alert("💥 Erro interno no servidor.");

        } else if (err.code === "ECONNABORTED") {
            alert("⏳ Timeout.");

        } else if (!err.response) {
            alert("🌐 Sem conexão.");
        }

        return Promise.reject(err);
    }
);

export default api;
