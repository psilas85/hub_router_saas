// hub_router_1.0.1/frontend/src/services/transferRouting.ts

import api from "@/services/api";

export type ClusterizacaoTransferDisponivel = {
    data: string;
    quantidade_entregas: number;
    total_clusters: number;
    clusters_transferiveis: number;
    peso_total_kg: number;
    volumes_total: number;
    roteirizacao_existente: boolean;
    rotas_processadas: number;
};

export type ClusterizacoesTransferResponse = {
    status: string;
    tenant_id: string;
    clusterizacoes: ClusterizacaoTransferDisponivel[];
    pagination?: {
        limit: number;
        offset: number;
        has_more: boolean;
        next_offset?: number | null;
    };
};

export async function listarClusterizacoesTransfer(params: {
    limit?: number;
    offset?: number;
    data_inicio?: string;
    data_fim?: string;
}): Promise<ClusterizacoesTransferResponse> {
    const { data } = await api.get("/transfer_routing/clusterizacoes-disponiveis", { params });
    return data;
}

// ===== Transfer Routing (processamento + PDF)
export type ProcessarParams = {
    data_inicial: string;      // YYYY-MM-DD
    modo_forcar?: boolean;
    tempo_maximo?: number;
    tempo_parada_leve?: number;
    peso_leve_max?: number;
    tempo_parada_pesada?: number;
    tempo_por_volume?: number;
};

export async function processarTransferRouting(params: ProcessarParams) {
    const res = await api.post("/transfer_routing/processar", null, { params });
    return res.data;
}

export type EnqueueTransferParams = ProcessarParams;

export type TransferJobStatus = {
    job_id: string;
    status: "queued" | "running" | "finished" | "failed" | string;
    progress: number;
    step: string;
    mensagem?: string | null;
    result?: unknown;
    error?: string | null;
};

export async function enqueueTransferJob(params: EnqueueTransferParams): Promise<{ job_id: string; status: string }> {
    const res = await api.post("/transfer_routing/jobs", null, {
        params: {
            data_inicial: params.data_inicial,
            modo_forcar: params.modo_forcar,
            tempo_maximo: params.tempo_maximo,
            tempo_parada_leve: params.tempo_parada_leve,
            peso_leve_max: params.peso_leve_max,
            tempo_parada_pesada: params.tempo_parada_pesada,
            tempo_por_volume: params.tempo_por_volume,
        },
    });
    return res.data;
}

export async function getTransferJobStatus(jobId: string): Promise<TransferJobStatus> {
    const res = await api.get(`/transfer_routing/jobs/${jobId}`);
    return res.data;
}

export async function baixarRelatorioPDF(data_inicial: string, data_final?: string) {
    const res = await api.get("/transfer_routing/visualizacao", {
        params: { data_inicial, data_final },
        responseType: "blob",
    });
    return res.data as Blob;
}

// ===== Costs Transfer (tarifas por tipo de veículo)
export type Tarifa = {
    tipo_veiculo: string;
    custo_km: number;
    capacidade_min: number;
    capacidade_max: number;
};

export async function listarTarifas(): Promise<Tarifa[]> {
    const { data } = await api.get("/costs_transfer/tarifas");
    return data;
}

export async function criarTarifa(body: Tarifa): Promise<Tarifa> {
    const { data } = await api.post("/costs_transfer/tarifas", body);
    return data;
}

export async function atualizarTarifa(tipo_veiculo: string, body: Tarifa): Promise<Tarifa> {
    const { data } = await api.put(`/costs_transfer/tarifas/${tipo_veiculo}`, body);
    return data;
}

export async function removerTarifa(tipo_veiculo: string): Promise<void> {
    await api.delete(`/costs_transfer/tarifas/${tipo_veiculo}`);
}
