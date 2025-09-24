// hub_router_1.0.1/frontend/src/services/simulationApi.ts

import api from "@/services/api";

// ===== Execução de simulação =====
export type RunSimulationParams = {
    data_inicial: string;
    data_final?: string;
    hub_id: number;

    // Clusterização
    k_min?: number;
    k_max?: number;
    k_inicial_transferencia?: number;
    min_entregas_cluster?: number;
    fundir_clusters_pequenos?: boolean;

    // Cluster hub
    desativar_cluster_hub?: boolean;
    raio_hub_km?: number;

    // Tempos
    parada_leve?: number;
    parada_pesada?: number;
    tempo_volume?: number;

    // Operações
    velocidade?: number;
    limite_peso?: number;

    // Restrições
    restricao_veiculo_leve_municipio?: boolean;
    peso_leve_max?: number;

    // Transferências
    tempo_max_transferencia?: number;
    peso_max_transferencia?: number;

    // Last-mile
    entregas_por_subcluster?: number;
    tempo_max_roteirizacao?: number;
    tempo_max_k1?: number;

    // Rotas excedentes
    permitir_rotas_excedentes?: boolean;

    // Forçar sobrescrita
    modo_forcar?: boolean;
};

export async function runSimulation(params: RunSimulationParams) {
    const { data_inicial, data_final, ...rest } = params;
    const df = data_final && data_final.trim() !== "" ? data_final : data_inicial;

    const resp = await api.post("/simulation/executar", null, {
        params: {
            data_inicial,
            data_final: df,
            ...rest,
        },
    });

    return resp.data as {
        status: "ok" | "queued" | "processing";
        job_id?: string;   // 👈 para acompanhar no frontend
        tenant_id?: string;
        mensagem: string;
        datas_processadas?: string[];
        datas_ignoradas?: string[];
        parametros?: Record<string, unknown>;
    };
}

// ===== Status de execução da simulação =====
export type SimulationJobStatus = {
    status: "queued" | "processing" | "done" | "error";
    job_id: string;
    tenant_id: string;
    progress?: number;
    step?: string;
    mensagem?: string;
    error?: string;
    datas_processadas?: string[];
    ended_at?: string;
};

export async function getSimulationStatus(job_id: string) {
    const resp = await api.get(`/simulation/status/${job_id}`);
    return resp.data as SimulationJobStatus;
}

// ===== Visualização de simulação =====
export type VisualizeSimulationResponse = {
    data: string;
    relatorio_pdf?: string;
    graficos?: string[];
    cenarios: Record<
        string,
        {
            graficos?: string[];
            mapas?: string[];
            tabelas_lastmile?: string[];
            tabelas_transferencias?: string[];
            tabelas_resumo?: string[];
            tabelas_detalhes?: string[];
            otimo?: boolean;
        }
    >;
};

export async function visualizeSimulation(data: string) {
    const resp = await api.get("/simulation/visualizar", {
        params: { data },
    });
    return resp.data as VisualizeSimulationResponse;
}

// ===== Distribuição de k_clusters =====
export type DistribuicaoKResponse = {
    status: string;
    data_inicial: string;
    data_final: string;
    grafico: string;
    dados: { k_clusters: number; qtd: number }[];
};

export async function getDistribuicaoK(params: {
    data_inicial: string;
    data_final: string;
}) {
    const resp = await api.get("/simulation/distribuicao_k", { params });
    return resp.data as DistribuicaoKResponse;
}

// ===== Frequência de cidades =====
export type FrequenciaCidadesResponse = {
    status: string;
    data_inicial: string;
    data_final: string;
    grafico: string;
    csv: string;
    dados: { cluster_cidade: string; qtd: number }[];
};

export async function getFrequenciaCidades(params: {
    data_inicial: string;
    data_final: string;
}) {
    const resp = await api.get("/simulation/frequencia_cidades", { params });
    return resp.data as FrequenciaCidadesResponse;
}

// ===== Custos consolidados para k fixo =====
export type KFixoResponse = {
    status: string;
    tenant_id: string;
    data_inicial: string;
    data_final: string;
    grafico: string | null;
    csv: string | null;
    cenarios: {
        k_clusters: number;
        dias_presentes: number;
        total_dias: number;
        cobertura_pct: number;
        custo_alvo: number;
        regret_absoluto: number;
        regret_relativo: number;
    }[];
};

export async function getKFixo(params: {
    data_inicial: string;
    data_final: string;
    min_cobertura_parcial?: number;
}) {
    const resp = await api.get("/simulation/k_fixo", { params });
    return resp.data as KFixoResponse;
}

// ===== Frota sugerida para k fixo =====
export type FrotaKFixoResponse = {
    status: string;
    tenant_id: string;
    data_inicial: string;
    data_final: string;
    csv_lastmile?: string | null;
    csv_transfer?: string | null;
    lastmile: {
        k_clusters: number;
        tipo_veiculo: string;
        frota_sugerida: number;
        dias_presentes: number;
        total_dias: number;
        cobertura_pct: number;
        modo: string;
    }[];
    transfer: {
        k_clusters: number;
        tipo_veiculo: string;
        frota_sugerida: number;
        dias_presentes: number;
        total_dias: number;
        cobertura_pct: number;
        modo: string;
    }[];
};

export async function getFrotaKFixo(params: {
    data_inicial: string;
    data_final: string;
    k: number;   // ✅ agora apenas um k por vez
}) {
    const resp = await api.get("/simulation/frota_k_fixo", { params });
    return resp.data as FrotaKFixoResponse;
}

// ===== CRUD de Hubs =====
export type Hub = {
    hub_id: number;
    nome: string;
    cidade: string;
    latitude: number;
    longitude: number;
};

export type HubCreate = Omit<Hub, "hub_id">;

export async function listHubs() {
    const resp = await api.get("/simulation/hubs");
    return resp.data as Hub[];
}

export async function createHub(hub: HubCreate) {
    const resp = await api.post("/simulation/hubs", hub);
    return resp.data as Hub;
}

export async function updateHub(id: number, hub: HubCreate) {
    const resp = await api.put(`/simulation/hubs/${id}`, hub);
    return resp.data as Hub;
}

export async function deleteHub(id: number) {
    const resp = await api.delete(`/simulation/hubs/${id}`);
    return resp.data;
}

// ===== CRUD de Custos de Centros =====
export type ClusterCost = {
    id?: number;
    limite_qtd_entregas: number;
    custo_fixo_diario: number;
    custo_variavel_por_entrega: number;
};

export async function listClusterCosts() {
    const resp = await api.get("/simulation/cluster_costs/list");
    return resp.data as ClusterCost[];
}

export async function getClusterCosts() {
    const resp = await api.get("/simulation/cluster_costs");
    return resp.data as ClusterCost;
}

export async function saveClusterCosts(costs: ClusterCost) {
    const resp = await api.post("/simulation/cluster_costs", costs);
    return resp.data as ClusterCost;
}

export async function deleteClusterCost(id: number) {
    const resp = await api.delete(`/simulation/cluster_costs/${id}`);
    return resp.data;
}

// ===== Histórico de Simulações =====
export type HistoricoSimulation = {
    id: number;
    tenant_id: string;
    job_id: string;
    status: "processing" | "finished" | "failed";
    mensagem: string;
    datas?: any;
    parametros?: any;
    criado_em: string;
    updated_at: string;
};

export async function getHistorico(limit: number = 10) {
    const resp = await api.get("/simulation/historico", { params: { limit } });
    return resp.data as {
        status: string;
        tenant_id: string;
        historico: HistoricoSimulation[];
    };
}
