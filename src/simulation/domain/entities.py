from datetime import date
from typing import Optional


class TransferenciaResumo:
    def __init__(
        self,
        rota_id: str,
        tenant_id: str,
        envio_data: date,
        simulation_id: str,
        k_clusters: int,
        is_ponto_otimo: bool,
        tipo_veiculo: str,
        distancia_total_km: float,
        distancia_parcial_km: float,
        tempo_total_min: float,
        peso_total_kg: float,
        volumes_total: int,
        valor_total_nf: float,
        aproveitamento_percentual: Optional[float],
        qde_entregas: int,
        qde_clusters_rota: int,
        coordenadas_seq: str,
        hub_id: Optional[int] = None,
        hub_nome: Optional[str] = None,
        hub_latitude: Optional[float] = None,
        hub_longitude: Optional[float] = None,
        tempo_parcial_min: Optional[float] = None  # 🔧 novo campo
    ):
        self.rota_id = rota_id
        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.simulation_id = simulation_id
        self.k_clusters = k_clusters
        self.is_ponto_otimo = is_ponto_otimo
        self.tipo_veiculo = tipo_veiculo
        self.distancia_total_km = distancia_total_km
        self.distancia_parcial_km = distancia_parcial_km
        self.tempo_total_min = tempo_total_min
        self.peso_total_kg = peso_total_kg
        self.volumes_total = volumes_total
        self.valor_total_nf = valor_total_nf
        self.aproveitamento_percentual = aproveitamento_percentual
        self.qde_entregas = qde_entregas
        self.qde_clusters_rota = qde_clusters_rota
        self.coordenadas_seq = coordenadas_seq
        self.hub_id = hub_id
        self.hub_nome = hub_nome
        self.hub_latitude = hub_latitude
        self.hub_longitude = hub_longitude
        self.tempo_parcial_min = tempo_parcial_min  # ✅ atribuição nova


# domain/entities.py

from dataclasses import dataclass
from typing import List

@dataclass
class ClusterTransferencia:
    cluster_id: int
    centro_lat: float
    centro_lon: float
    peso_total: float
    valor_nf_total: float
    valor_frete_total: float
