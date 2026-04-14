//hub_router_1.0.1/frontend/src/hooks/useSimulationJob.ts

import { useEffect, useRef } from "react";
import { getSimulationStatus, getHistorico } from "@/services/simulationApi";

type Options = {
    jobId: string | null;
    onUpdate: (msg: string, tone: "info" | "success" | "error") => void;
    onFinish: () => void;
    onError: () => void;
};

export function useSimulationJob({
    jobId,
    onUpdate,
    onFinish,
    onError,
}: Options) {
    const tentativasRef = useRef(0);

    useEffect(() => {
        if (!jobId) return;

        tentativasRef.current = 0;

        const run = async () => {
            try {
                const status = await getSimulationStatus(jobId);

                const mensagem =
                    status.mensagem ||
                    status.result?.mensagem ||
                    status.error ||
                    null;

                if (status.status === "done" || status.status === "finished") {
                    onUpdate(mensagem || "✅ Simulação concluída.", "success");
                    onFinish();
                    return true;
                }

                if (status.status === "error" || status.status === "failed") {
                    onUpdate(mensagem || "❌ Simulação falhou.", "error");
                    onError();
                    return true;
                }

                onUpdate(mensagem || "⏳ Processando...", "info");
                return false;
            } catch (err: any) {
                // 🔥 fallback histórico
                if (err?.response?.status === 404) {
                    try {
                        const hist = await getHistorico(10);
                        const job = hist.historico?.find(
                            (h) => h.job_id === jobId
                        );

                        if (job?.status === "finished") {
                            onUpdate(
                                job.mensagem || "✅ Simulação concluída.",
                                "success"
                            );
                            onFinish();
                            return true;
                        }

                        if (job?.status === "failed") {
                            onUpdate(
                                job.mensagem || "❌ Simulação falhou.",
                                "error"
                            );
                            onError();
                            return true;
                        }

                        onUpdate(
                            job?.mensagem || "⏳ Inicializando...",
                            "info"
                        );
                        return false;
                    } catch {
                        onUpdate("⏳ Inicializando...", "info");
                        return false;
                    }
                }

                onUpdate("⏳ Aguardando atualização...", "info");
                return false;
            }
        };

        const interval = setInterval(async () => {
            tentativasRef.current++;

            const finished = await run();

            // 🔥 para polling se terminou
            if (finished) {
                clearInterval(interval);
                return;
            }

            // 🔥 timeout (10 min)
            if (tentativasRef.current > 60) {
                onUpdate("⚠️ Timeout da simulação.", "error");
                onError();
                clearInterval(interval);
            }
        }, 10000);

        // 🔥 primeira execução rápida
        const timeout = setTimeout(run, 2000);

        return () => {
            clearInterval(interval);
            clearTimeout(timeout);
        };
    }, [jobId]);
}