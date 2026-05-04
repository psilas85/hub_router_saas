import api from "@/services/api";

export type ClusterizationHub = {
    id: number;
    nome: string;
    endereco: string;
    latitude: number;
    longitude: number;
    hub_central: boolean;
    centro_cluster: boolean;
    ativo: boolean;
};

export type ClusterizationHubCreate = Omit<ClusterizationHub, "id">;

export async function listClusterizationHubs() {
    const resp = await api.get("/clusterization/hubs-cadastro");
    return resp.data as ClusterizationHub[];
}

export async function createClusterizationHub(hub: ClusterizationHubCreate) {
    const resp = await api.post("/clusterization/hubs-cadastro", hub);
    return resp.data as ClusterizationHub;
}

export async function updateClusterizationHub(id: number, hub: ClusterizationHubCreate) {
    const resp = await api.put(`/clusterization/hubs-cadastro/${id}`, hub);
    return resp.data as ClusterizationHub;
}

export async function deleteClusterizationHub(id: number) {
    const resp = await api.delete(`/clusterization/hubs-cadastro/${id}`);
    return resp.data;
}
