// hub_router_1.0.1/frontend/src/services/simulationApi.ts

import api from "@/services/api";

// ===== Execução de simulação =====
export type RunSimulationParams = {
    data_inicial: string;        // yyyy-mm-dd
    data_final?: string;         // opcional no front; se não vier, usamos a mesma do inicial

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
};

export async function runSimulation(params: RunSimulationParams) {
    const { data_inicial, data_final, ...rest } = params;
    const df = data_final && data_final.trim() !== "" ? data_final : data_inicial;

    const resp = await api.post("/simulation/executar", null, {
        params: {
            data_inicial,
            data_final: df,
            ...rest,
            // 🔒 não inclui modo_forcar → Gateway já injeta sempre como True
        },
    });

    return resp.data as {
        status: "ok";
        mensagem: string;
        datas_processadas: string[];
        datas_ignoradas: string[];
        parametros: Record<string, unknown>;
    };
}

// ===== Visualização de simulação (artefatos por data) =====
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
    const resp = await api.get("/simulation/simulacao/visualizar", {
        params: { data },
    });
    return resp.data as VisualizeSimulationResponse;
}

// ===== Distribuição de k_clusters =====
export type DistribuicaoKResponse = {
    status: string;
    data_inicial: string;
    data_final: string;
    grafico: string; // caminho relativo do backend
    dados: { k_clusters: number; qtd: number }[];
};

export async function getDistribuicaoK(params: {
    data_inicial: string;
    data_final: string;
}) {
    const resp = await api.get("/simulation/distribuicao_k", {
        params,
    });

    return resp.data as DistribuicaoKResponse;
}

// ===== Frequência de cidades em pontos ótimos =====
export type FrequenciaCidadesResponse = {
    status: string;
    data_inicial: string;
    data_final: string;
    grafico: string; // caminho relativo do backend
    dados: { cluster_cidade: string; qtd: number }[];
};

export async function getFrequenciaCidades(params: {
    data_inicial: string;
    data_final: string;
}) {
    const resp = await api.get("/simulation/frequencia_cidades", {
        params,
    });

    return resp.data as FrequenciaCidadesResponse;
}
