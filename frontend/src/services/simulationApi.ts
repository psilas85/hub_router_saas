// hub_router_1.0.1/frontend/src/services/simulationApi.ts
import api from "@/services/api";

function removeEmptyQueryParams<T extends Record<string, unknown>>(params: T) {
    return Object.fromEntries(
        Object.entries(params).filter(([, value]) => {
            if (value === undefined || value === null) return false;
            if (typeof value === "string" && value.trim() === "") return false;
            if (typeof value === "number" && Number.isNaN(value)) return false;
            return true;
        })
    ) as Partial<T>;
}

// ===== Execução de simulação =====
export type RunSimulationParams = {
    data_inicial: string;
    data_final?: string;
    hub_id: number;

    // Estratégia
    modo_simulacao?: "padrao" | "balanceado" | "time_windows";
    algoritmo_clusterizacao?: "kmeans" | "balanced_kmeans";
    algoritmo_roteirizacao?: "heuristico" | "time_windows";

    // HUB
    desativar_cluster_hub?: boolean;
    raio_hub_km?: number;

    // OUTLIER
    usar_outlier?: boolean;
    distancia_outlier_km?: number;

    // CLUSTER
    min_entregas_por_cluster_alvo?: number;
    max_entregas_por_cluster_alvo?: number;

    // TEMPO OPERACIONAL
    tempo_parada_leve?: number;
    tempo_parada_pesada?: number;
    tempo_por_volume?: number;
    limite_peso_parada?: number;

    // VELOCIDADE
    velocidade_kmh?: number;

    // PESO
    limite_peso_veiculo?: number;
    peso_max_transferencia?: number;

    // TEMPO
    tempo_max_roteirizacao?: number;
    tempo_max_transferencia?: number;
    tempo_max_k0?: number;

    // LAST-MILE
    entregas_por_rota?: number;

    // REGRAS
    permitir_rotas_excedentes?: boolean;
    permitir_veiculo_leve_intermunicipal?: boolean;

    // EXECUÇÃO
    modo_forcar?: boolean;

    // TIME WINDOWS
    tempo_especial_min?: number;
    tempo_especial_max?: number;
    max_especiais_por_rota?: number;
};

export async function runSimulation(params: RunSimulationParams) {
    const { data_inicial, data_final, ...rest } = params;

    const payload = removeEmptyQueryParams({
        data_inicial,
        ...(data_final && data_final.trim() !== "" ? { data_final } : {}),
        ...rest,
    });

    console.log("🚀 PAYLOAD FINAL:", payload);

    const resp = await api.post("/simulation/executar", payload, {
        headers: {
            "Content-Type": "application/json",
        },
    });

    return resp.data;
}

// ===== Status de execução da simulação =====
export type SimulationJobStatus = {
    status: "queued" | "processing" | "done" | "error" | "finished" | "failed";
    job_id: string;
    tenant_id: string;
    progress?: number;
    step?: string;
    mensagem?: string;
    error?: string;
    result?: {
        mensagem?: string;
        status?: string;
    };
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
    excel_entregas_rotas?: string;
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
    k: number;
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

// ===== Veículos Simulation - Last-Mile =====
export type SimulationLastMileVehicle = {
    veiculo: string;  // 🔄 corrigido (antes era tipo_veiculo)
    capacidade_kg_min: number;
    capacidade_kg_max: number;
    tarifa_km: number;
    tarifa_entrega: number;
};

export async function simListLastMileVehicles() {
    const resp = await api.get("/simulation/tarifas/lastmile");
    return resp.data as SimulationLastMileVehicle[];
}

export async function simAddLastMileVehicle(v: SimulationLastMileVehicle) {
    const resp = await api.post("/simulation/tarifas/lastmile", v);
    return resp.data;
}

export async function simDeleteLastMileVehicle(veiculo: string) {
    const resp = await api.delete(
        `/simulation/tarifas/lastmile/${encodeURIComponent(veiculo)}`
    );
    return resp.data;
}


// ===== Veículos Simulation - Transferência =====
export type SimulationTransferVehicle = {
    veiculo: string;  // 🔄 corrigido
    capacidade_kg_min: number;
    capacidade_kg_max: number;
    tarifa_km: number;
    tarifa_fixa: number;
};

export async function simListTransferVehicles() {
    const resp = await api.get("/simulation/tarifas/transferencia");
    return resp.data as SimulationTransferVehicle[];
}

export async function simAddTransferVehicle(v: SimulationTransferVehicle) {
    const resp = await api.post("/simulation/tarifas/transferencia", v);
    return resp.data;
}

export async function simDeleteTransferVehicle(veiculo: string) {
    const resp = await api.delete(
        `/simulation/tarifas/transferencia/${encodeURIComponent(veiculo)}`
    );
    return resp.data;
}
