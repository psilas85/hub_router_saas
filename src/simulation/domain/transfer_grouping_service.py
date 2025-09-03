# domain/transfer_grouping_service.py

from typing import List
from simulation.utils.google_api import buscar_rota_google
from domain.entities import ClusterTransferencia

def agrupar_clusters_em_rotas(clusters: List[ClusterTransferencia], hub_coord: tuple, tempo_max: float, peso_max: float, tenant_id: str, db_conn, logger=None):
    rotas = []
    rota_atual = []
    peso_atual = 0
    tempo_atual = 0

    for cluster in clusters:
        destino = (cluster.centro_lat, cluster.centro_lon)

        # Rota ida
        dist_ida, tempo_ida, _ = buscar_rota_google(hub_coord, destino)
        # Rota volta
        dist_volta, tempo_volta, _ = buscar_rota_google(destino, hub_coord)

        tempo_estimado = tempo_ida + tempo_volta  # simplificado

        if (
            peso_atual + cluster.peso_total <= peso_max
            and tempo_atual + tempo_estimado <= tempo_max
        ):
            rota_atual.append(cluster)
            peso_atual += cluster.peso_total
            tempo_atual += tempo_estimado
        else:
            if rota_atual:
                rotas.append(rota_atual)
            rota_atual = [cluster]
            peso_atual = cluster.peso_total
            tempo_atual = tempo_estimado

    if rota_atual:
        rotas.append(rota_atual)

    return rotas
