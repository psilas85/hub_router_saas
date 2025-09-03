// hub_router_1.0.1/frontend/src/services/transferRouting.ts

import api from "@/services/api";

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
