import { NavLink, Outlet } from "react-router-dom";
import { Database, Upload, BarChart3 } from "lucide-react";

const tabBaseClassName =
    "flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-medium transition-colors";

export default function DataWorkspacePage() {
    return (
        <>
            <div className="mx-auto max-w-6xl px-6 pt-6">
                <div className="mb-5 flex items-center gap-3">
                    <div className="rounded-2xl bg-emerald-50 p-3 text-emerald-600">
                        <Database className="h-6 w-6" />
                    </div>
                    <div>
                        <h1 className="text-2xl font-semibold text-gray-900">Entrada de Dados</h1>
                        <p className="text-sm text-gray-500">
                            Centralize o upload das planilhas e a análise exploratória da operação.
                        </p>
                    </div>
                </div>

                <div className="mb-6 rounded-2xl border border-slate-200 bg-white p-2 shadow-sm">
                    <div className="flex flex-wrap gap-2">
                        <NavLink
                            to="/entrada-dados/upload"
                            className={({ isActive }) =>
                                `${tabBaseClassName} ${isActive ? "bg-emerald-600 text-white shadow-sm" : "text-slate-600 hover:bg-slate-50"}`
                            }
                        >
                            <Upload className="h-4 w-4" />
                            Upload
                        </NavLink>
                        <NavLink
                            to="/entrada-dados/explorador"
                            className={({ isActive }) =>
                                `${tabBaseClassName} ${isActive ? "bg-emerald-600 text-white shadow-sm" : "text-slate-600 hover:bg-slate-50"}`
                            }
                        >
                            <BarChart3 className="h-4 w-4" />
                            Explorador de Dados
                        </NavLink>
                    </div>
                </div>
            </div>

            <Outlet />
        </>
    );
}
