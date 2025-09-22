//hub_router_1.0.1/frontend/src/context/ProcessingContext.tsx

import { createContext, useContext, useState } from "react";

type ProcessingContextType = {
    count: number;                           // número de processamentos ativos
    processing: boolean;                     // true se count > 0
    startProcessing: () => void;             // adiciona +1
    stopProcessing: () => void;              // remove -1
    resetProcessing: () => void;             // zera (seguro)
};

const ProcessingContext = createContext<ProcessingContextType | undefined>(undefined);

export const ProcessingProvider = ({ children }: { children: React.ReactNode }) => {
    const [count, setCount] = useState(0);

    const startProcessing = () => setCount((c) => c + 1);
    const stopProcessing = () => setCount((c) => Math.max(0, c - 1));
    const resetProcessing = () => setCount(0);

    return (
        <ProcessingContext.Provider
            value={{
                count,
                processing: count > 0,
                startProcessing,
                stopProcessing,
                resetProcessing,
            }}
        >
            {children}
        </ProcessingContext.Provider>
    );
};

export const useProcessing = () => {
    const context = useContext(ProcessingContext);
    if (!context) {
        throw new Error("useProcessing deve ser usado dentro de ProcessingProvider");
    }
    return context;
};
