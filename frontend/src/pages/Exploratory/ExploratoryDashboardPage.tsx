// hub_router_1.0.1/frontend/src/pages/Exploratory/ExploratoryDashboardPage.tsx
// hub_router_1.0.1/frontend/src/pages/Exploratory/ExploratoryDashboardPage.tsx
import { useEffect, useState } from "react";
import { useAuthStore } from "@/store/authStore";

export default function ExploratoryPage() {
    const [height, setHeight] = useState<number>(
        typeof window !== "undefined" ? window.innerHeight - 64 : 800
    );

    const { token } = useAuthStore();  // 🔑 pega o JWT

    useEffect(() => {
        const onResize = () => setHeight(window.innerHeight - 64);
        window.addEventListener("resize", onResize);
        return () => window.removeEventListener("resize", onResize);
    }, []);

    if (!token) {
        // 🔒 evita renderizar sem token
        return (
            <div className="flex items-center justify-center h-full">
                <p className="text-red-600 font-semibold">
                    ⚠️ É necessário estar autenticado para acessar a análise exploratória.
                </p>
            </div>
        );
    }

    const url = `/exploratory_analysis_ui/?token=${token}`;

    return (
        <div className="flex flex-col h-full w-full">
            <iframe
                src={url}
                title="EDA Dashboard"
                className="flex-1 border-0 w-full"
                style={{ minHeight: height }}
                sandbox="allow-same-origin allow-scripts allow-popups allow-forms allow-modals allow-downloads"
                allow="clipboard-read; clipboard-write"
            />

        </div>
    );
}
