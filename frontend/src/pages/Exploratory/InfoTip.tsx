// frontend/src/pages/Exploratory/InfoTip.tsx

import { useState } from "react";
import type { TipContent } from "./labels";

interface InfoTipProps extends TipContent {
    align?: "left" | "right";
}

export function InfoTip({ comercial, tecnico, align = "left" }: InfoTipProps) {
    const [open, setOpen] = useState(false);
    const [showTecnico, setShowTecnico] = useState(false);

    function toggle() {
        setOpen((v) => !v);
        if (open) setShowTecnico(false);
    }

    return (
        <span className="relative inline-block align-middle ml-1.5">
            <button
                onClick={toggle}
                className="w-4 h-4 rounded-full border border-gray-300 text-gray-400 hover:border-blue-400 hover:text-blue-500 transition-colors text-[10px] font-bold leading-none inline-flex items-center justify-center"
                title="Mais informações"
                aria-label="Mais informações"
            >
                i
            </button>

            {open && (
                <>
                    {/* backdrop to close on outside click */}
                    <div
                        className="fixed inset-0 z-40"
                        onClick={() => { setOpen(false); setShowTecnico(false); }}
                    />

                    <div
                        className={`absolute z-50 top-6 w-72 bg-white border border-gray-200 rounded-lg shadow-xl p-3 text-left ${
                            align === "right" ? "right-0" : "left-0"
                        }`}
                    >
                        <button
                            onClick={() => { setOpen(false); setShowTecnico(false); }}
                            className="absolute top-2 right-2 text-gray-300 hover:text-gray-500 text-xs leading-none"
                            aria-label="Fechar"
                        >
                            ✕
                        </button>

                        <p className="text-sm text-gray-700 leading-snug pr-4">{comercial}</p>

                        {tecnico && (
                            <>
                                {!showTecnico ? (
                                    <button
                                        onClick={(e) => { e.stopPropagation(); setShowTecnico(true); }}
                                        className="mt-2 text-xs text-blue-500 hover:text-blue-700 hover:underline"
                                    >
                                        Ver detalhe técnico ↓
                                    </button>
                                ) : (
                                    <p className="mt-2 text-xs text-gray-500 bg-gray-50 rounded p-2 border-t border-gray-100 leading-snug">
                                        {tecnico}
                                    </p>
                                )}
                            </>
                        )}
                    </div>
                </>
            )}
        </span>
    );
}
