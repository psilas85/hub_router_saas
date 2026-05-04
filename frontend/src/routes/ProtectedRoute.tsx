// src/routes/ProtectedRoute.tsx
import type { ReactNode } from "react";
import { Navigate } from "react-router-dom";
import { useAuthStore } from "@/store/authStore";

interface ProtectedRouteProps {
    children: ReactNode;
}

export default function ProtectedRoute({ children }: ProtectedRouteProps) {
    const token = useAuthStore((s) => s.token);
    const isTokenExpired = useAuthStore((s) => s.isTokenExpired);

    if (!token || isTokenExpired()) {
        return <Navigate to="/login" replace />;
    }

    return <>{children}</>;
}
