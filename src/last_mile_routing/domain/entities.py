from dataclasses import dataclass
from typing import List


@dataclass
class Entrega:
    cte_numero: str
    latitude: float
    longitude: float
    peso: float
    volumes: int
    valor_nf: float
    valor_frete: float
    endereco: str


@dataclass
class SubCluster:
    id: int
    entregas: List[Entrega]
    centro_lat: float
    centro_lon: float
    peso_total: float
    volumes_total: int


@dataclass
class Rota:
    rota_id: str
    tenant_id: str
    envio_data: str
    veiculo: str
    distancia_parcial_km: float
    distancia_total_km: float
    tempo_parcial_min: float
    tempo_total_min: float
    entregas: List[Entrega]
    polyline: str
