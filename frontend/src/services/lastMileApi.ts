// src/services/lastMileApi.ts
import api from "@/services/api";

// ⚠️ Endpoints alinhados ao GATEWAY fornecido:
// - Roteirização:   /last_mile_routing/roteirizar (POST)
// - Visualização:   /last_mile_routing/visualizar (GET)
// - Artefatos:      /last_mile_routing/artefatos (GET)
// - Custos process: /costs_last_mile/processar (POST)
// - Custos visual.: /costs_last_mile/visualizar (GET)
// - Artefatos cost: /costs_last_mile/artefatos (GET)
// - Veículos CRUD:  /costs_last_mile/vehicles (GET/POST/PUT/DELETE)

// ——— Tipos ———
export type RoutingParams = {
    data_inicial: string;             // YYYY-MM-DD
    data_final?: string;              // opcional — se não vier, backend usa inicial
    entregas_por_subcluster?: number;
    tempo_maximo_rota?: number;
    tempo_parada_leve?: number;
    tempo_parada_pesada?: number;
    tempo_descarga_por_volume?: number;
    peso_leve_max?: number;
    restricao_veiculo_leve_municipio?: boolean;
    modo_forcar?: boolean;
};

export type Artefato = {
    data: string;
    map_html_url: string;
    pdf_url: string;
};

// Artefato de custos só tem PDF
export type ArtefatoCustos = {
    data: string;
    pdf_url: string;
};

// 🔹 Tipo de retorno do endpoint de visualização de custos Last-Mile
export type VisualizeCostsResponse = {
    tenant_id: string;
    envio_data: string;
    detalhes_url: string;
    resumo_url: string;
    pdf_url: string;
    json_dados: any[];
};

// Tipo para custos de veículos
export type VehicleCost = {
    veiculo: string;
    peso_minimo_kg: number;
    peso_maximo_kg: number;
    custo_por_km: number;
    custo_por_entrega: number;
};

// ——— Routing ———
export async function lmProcessRouting(params: RoutingParams) {
    const { data } = await api.post("/last_mile_routing/roteirizar", null, {
        params: {
            ...params,
            data_final: params.data_final ?? params.data_inicial, // 🔑 garante consistência
        },
    });
    return data; // {status, mensagem, datas}
}

// 🔹 Buscar artefatos (HTML/PDF disponíveis da roteirização)
export async function lmBuscarArtefatos(data_inicial: string) {
    const { data } = await api.get("/last_mile_routing/artefatos", {
        params: { data_inicial },
    });
    return data as { status: string; artefatos: Artefato[] };
}

// ——— Costs ———
export async function lmProcessCosts(
    data_inicial: string,
    data_final?: string,
    modo_forcar = false
) {
    const { data } = await api.post("/costs_last_mile/processar", null, {
        params: { data_inicial, data_final: data_final ?? data_inicial, modo_forcar },
    });
    return data as { status: string; mensagem: string; datas?: string[] };
}

// 🔹 Visualizar custos (retorna CSV/PDF + JSON para tabela)
export async function lmVisualizeCosts(data: string): Promise<VisualizeCostsResponse> {
    const { data: resp } = await api.get("/costs_last_mile/visualizar", { params: { data } });
    return resp as VisualizeCostsResponse;
}

// 🔹 Buscar apenas artefatos de custos Last Mile (PDF fixo)
export async function lmBuscarArtefatosCustos(data: string) {
    const { data: resp } = await api.get("/costs_last_mile/artefatos", { params: { data } });
    return resp as { status: string; artefatos: ArtefatoCustos[] };
}

// ——— Vehicles CRUD ———
export async function lmListVehicles(): Promise<VehicleCost[]> {
    const { data } = await api.get("/costs_last_mile/vehicles");
    return data;
}

export async function lmAddVehicle(vehicle: VehicleCost): Promise<VehicleCost> {
    const { data } = await api.post("/costs_last_mile/vehicles", vehicle);
    return { ...vehicle, ...data };
}

export async function lmUpdateVehicle(
    veiculo: string,
    vehicle: VehicleCost
): Promise<VehicleCost> {
    const { data } = await api.put(
        `/costs_last_mile/vehicles/${encodeURIComponent(veiculo)}`,
        vehicle
    );
    return { ...vehicle, ...data };
}

export async function lmDeleteVehicle(
    veiculo: string
): Promise<{ status: string; mensagem: string }> {
    const { data } = await api.delete(
        `/costs_last_mile/vehicles/${encodeURIComponent(veiculo)}`
    );
    return data;
}
