import { useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { useAuthStore } from "@/store/authStore";

export function useAuthGuard() {
    const navigate = useNavigate();
    const { token, usuario, tenant, isTokenExpired, logout } = useAuthStore();

    useEffect(() => {
        if (!token || isTokenExpired()) {
            logout();
            navigate("/login", { replace: true });
        }
    }, [token, isTokenExpired, logout, navigate]);

    return { token, usuario, tenant };
}
