// frontend/src/context/PlannerContext.tsx
import { createContext, useContext, useState } from "react";
import type { ReactNode } from "react";

type ScenarioResult = {
    cenario: string;
    mes: string;
    custo_transferencia: number;
    custo_last_mile: number;
    custo_total: number;
    frota: Record<string, number>;
    hubs: [string, string][];
};

type PlannerContextType = {
    results: ScenarioResult[];
    setResults: (r: ScenarioResult[]) => void;
    status: string | null;
    setStatus: (s: string | null) => void;
};

const PlannerContext = createContext<PlannerContextType | undefined>(undefined);

export function PlannerProvider({ children }: { children: ReactNode }) {
    const [results, setResults] = useState<ScenarioResult[]>([]);
    const [status, setStatus] = useState<string | null>(null);

    return (
        <PlannerContext.Provider value={{ results, setResults, status, setStatus }}>
            {children}
        </PlannerContext.Provider>
    );
}

export function usePlanner() {
    const ctx = useContext(PlannerContext);
    if (!ctx) throw new Error("usePlanner deve ser usado dentro de <PlannerProvider>");
    return ctx;
}
