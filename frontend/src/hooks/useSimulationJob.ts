//hub_router_1.0.1/frontend/src/hooks/useSimulationJob.ts

import { useEffect, useRef } from "react";
import { getSimulationStatus, getHistorico } from "@/services/simulationApi";

type Options = {
    jobId: string | null;
    onUpdate: (msg: string, tone: "info" | "success" | "error") => void;
    onProgress?: (progress: number, step: string) => void;
    onFinish: () => void;
    onError: () => void;
};

export function useSimulationJob({
    jobId,
    onUpdate,
    onProgress,
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
                const rawStatus = String(status.status);
                const normalizedStatus = (rawStatus === "ok" ? "done" : rawStatus) as
                    | "queued"
                    | "processing"
                    | "done"
                    | "error"
                    | "finished"
                    | "failed";

                const mensagem =
                    status.mensagem ||
                    status.result?.mensagem ||
                    status.error ||
                    null;

                if (normalizedStatus === "done" || normalizedStatus === "finished") {
                    onProgress?.(100, "Concluído");
                    onUpdate(mensagem || "✅ Simulação concluída.", "success");
                    onFinish();
                    return true;
                }

                if (normalizedStatus === "error" || normalizedStatus === "failed") {
                    onUpdate(mensagem || "❌ Simulação falhou.", "error");
                    onError();
                    return true;
                }

                onProgress?.(status.progress ?? 0, status.step ?? "Processando...");
                onUpdate(mensagem || "⏳ Processando...", "info");
                return false;
            } catch (err: any) {
                // fallback histórico quando job expira do Redis
                if (err?.response?.status === 404) {
                    try {
                        const hist = await getHistorico(25);
                        const job = hist.historico?.find(
                            (h) => h.job_id === jobId
                        );

                        if (job?.status === "finished") {
                            onProgress?.(100, "Concluído");
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

            if (finished) {
                clearInterval(interval);
                return;
            }

            // timeout: 240 tentativas × 10s = 40 minutos
            if (tentativasRef.current > 240) {
                onUpdate("⚠️ Tempo limite atingido. Verifique o histórico.", "error");
                onError();
                clearInterval(interval);
            }
        }, 10000);

        // primeira execução rápida
        const timeout = setTimeout(run, 2000);

        return () => {
            clearInterval(interval);
            clearTimeout(timeout);
        };
    }, [jobId]);
}