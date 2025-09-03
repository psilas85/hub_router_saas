//frontend/src/services/ml.ts

import api from "@/services/api";

export type TrainTarget = "custo_total" | "is_ponto_otimo";
export type AlgoKey = "linear" | "random_forest" | "logistic";

export async function trainCompare(params: {
    dataset_name: string;
    target_column: TrainTarget;
    start_date: string;
    end_date: string;
    fast?: boolean;
}) {
    const { data } = await api.post("/ml/train_compare", params, {
        params: { fast: params.fast ?? false },
    });

    return data.content as {
        detail: string;
        target_column: TrainTarget;
        start_date: string;
        end_date: string;
        fast: boolean;
        results: {
            algorithm: string;
            algorithm_label: string;
            metrics: Record<string, number>;
        }[];
        csv: string;
    };
}

export async function plan(params: {
    start_date: string;
    months: number;
    scenarios?: string; // "base,baixo,alto"
    fast?: boolean;
}) {
    const { data } = await api.get("/ml/plan", {
        params: {
            start_date: params.start_date,
            months: params.months,
            scenarios: params.scenarios ?? "base,baixo,alto",
            fast: params.fast ?? false,
        },
    });
    return data.content as Record<
        string,
        Array<{
            mes: string;
            k_clusters: number;
            hubs: [string, string][];
            frota: Record<string, number>;
            custo_transferencia: number;
            custo_last_mile: number;
            custo_total: number;
        }>
    >;
}

export async function predict(params: {
    dataset_name: string;
    target_column: TrainTarget;
    features: Record<string, number>;
    algorithm?: AlgoKey | null;
}) {
    const { data } = await api.post("/ml/predict", {
        dataset_name: params.dataset_name,
        target_column: params.target_column,
        features: params.features,
        algorithm: params.algorithm ?? null,
    });

    // se vier em "predictions" (batch), pega o primeiro
    const content = data.content;
    if (content?.predictions?.length) {
        return content.predictions[0] as {
            prediction: number;
            probability: number | null;
            algorithm: string | null;
        };
    }
    return content as {
        prediction: number;
        probability: number | null;
        algorithm: string | null;
    };
}
