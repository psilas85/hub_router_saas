// src/components/Layout.tsx

import type { ReactNode } from "react";
import Navbar from "./Navbar";

export default function Layout({ children }: { children: ReactNode }) {
    return (
        <div className="min-h-screen bg-gray-100 flex">
            {/* 🔹 Sidebar fixa */}
            <Navbar />

            {/* 🔹 Conteúdo principal */}
            <div className="flex-1 flex flex-col md:pl-64">
                <main className="flex-1 p-6 md:p-8 lg:p-10 max-w-7xl mx-auto w-full">
                    {children}
                </main>

                {/* 🔹 Rodapé */}
                <footer className="py-4 text-center text-xs text-gray-500 border-t">
                    HubRouter © {new Date().getFullYear()} – Todos os direitos reservados
                </footer>
            </div>
        </div>
    );
}
