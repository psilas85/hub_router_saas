// hub_router_1.0.1/frontend/src/hooks/useTransferRoutingJob.ts

import { useEffect, useRef } from "react";
import { getTransferJobStatus } from "@/services/transferRouting";

type Options = {
    jobId: string | null;
    onUpdate: (msg: string, tone: "info" | "success" | "error") => void;
    onProgress?: (progress: number, step: string) => void;
    onFinish: () => void;
    onError: () => void;
};

export function useTransferRoutingJob({
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
                const status = await getTransferJobStatus(jobId);

                const mensagem = status.mensagem || status.error || null;

                if (status.status === "finished") {
                    onProgress?.(100, "Concluído");
                    onUpdate(mensagem || "✅ Roteirização concluída.", "success");
                    onFinish();
                    return true;
                }

                if (status.status === "failed") {
                    onUpdate(mensagem || "❌ Roteirização falhou.", "error");
                    onError();
                    return true;
                }

                onProgress?.(status.progress ?? 0, status.step ?? "Processando...");
                onUpdate(mensagem || "⏳ Processando...", "info");
                return false;
            } catch (err: any) {
                if (err?.response?.status === 404) {
                    onUpdate("⏳ Aguardando worker...", "info");
                    return false;
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
