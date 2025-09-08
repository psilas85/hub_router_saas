//frontend/src/services/ml.ts

import api from "@/services/api";

export type TrainTarget = "custo_total" | "is_ponto_otimo";
export type AlgoKey = "linear" | "random_forest" | "logistic";

// helpers
function asArray<T>(v: unknown): T[] {
    if (Array.isArray(v)) return v as T[];
    if (v && typeof v === "object") return [v as T];
    return [];
}
function unwrapResults(obj: any) {
    // aceita {content: {results: {...}}} ou {content: {...}} ou direto {...}
    const content = obj?.content ?? obj;
    return content?.results ?? content ?? {};
}

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
    return (data?.content ?? data) as {
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

// ---------- PLAN (Histórico)
export type PlanV1Item = {
    mes: string;
    k_clusters: number;
    hubs: [string, string][];
    frota: Record<string, number>;
    custo_transferencia: number;
    custo_last_mile: number;
    custo_total: number;
};

export async function plan(params: {
    start_date: string;
    months: number;
    scenarios?: string;
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

    const raw = unwrapResults(data) as Record<string, unknown>;
    const normalized: Record<string, PlanV1Item[]> = {};
    Object.entries(raw).forEach(([k, v]) => {
        normalized[k] = asArray<PlanV1Item>(v);
    });
    return normalized;
}

// ---------- PLAN V2 (ML)
export type PlanV2Item = {
    mes: string;
    entregas: number;
    peso: number;
    volumes: number;
    valor_nf: number;
    custo_last_mile: number;
    custo_transfer_total: number;
    custo_total: number;
    mae?: number;
    mae_interval?: string;
    mse?: number;
    r2?: number | null;
    r2_interval?: string;
    accuracy?: number;
    f1?: number;
    roc_auc?: number;
};

export async function planV2(params: {
    start_date: string;
    months: number;
    scenarios?: string;
    fast?: boolean;
}) {
    const { data } = await api.get("/ml/plan_v2", {
        params: {
            start_date: params.start_date,
            months: params.months,
            scenarios: params.scenarios ?? "base,baixo,alto",
            fast: params.fast ?? false,
        },
    });

    const raw = unwrapResults(data) as Record<string, unknown>;
    const normalized: Record<string, PlanV2Item[]> = {};
    Object.entries(raw).forEach(([k, v]) => {
        normalized[k] = asArray<PlanV2Item>(v);
    });
    return normalized;
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

    const content = data?.content ?? data;
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
