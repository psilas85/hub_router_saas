// src/services/costsTransfer.ts
import api from "@/services/api";

export type ArtefatosCosts = {
    tenant_id: string;
    envio_data: string;
    pdf_url?: string | null;
    json_dados?: any[];
};

// Processar custos
export async function processarCosts(data: string, modo_forcar = true) {
    const { data: resp } = await api.post("/costs_transfer/processar", null, {
        params: { data_inicial: data, data_final: data, modo_forcar },
    });
    return resp;
}

// Buscar artefatos (PDF + JSON já estruturado)
export async function buscarArtefatosCosts(data: string): Promise<ArtefatosCosts> {
    const { data: resp } = await api.get("/costs_transfer/artefatos", {
        params: { data },
    });
    return resp;
}
