#hub_router_1.0.1/src/simulation/domain/entities.py

from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, model_validator

from dataclasses import dataclass

from pydantic import BaseModel, Field
from typing import Literal, Optional


from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class TransferenciaResumo:
    rota_id: str
    tenant_id: str
    envio_data: date
    simulation_id: str
    k_clusters: int
    is_ponto_otimo: bool
    tipo_veiculo: str
    distancia_total_km: float
    distancia_parcial_km: float
    tempo_total_min: float
    peso_total_kg: float
    volumes_total: int
    valor_total_nf: float
    aproveitamento_percentual: Optional[float]
    qde_entregas: int
    qde_clusters_rota: int
    coordenadas_seq: Optional[str]
    hub_id: Optional[int] = None
    hub_nome: Optional[str] = None
    hub_latitude: Optional[float] = None
    hub_longitude: Optional[float] = None
    tempo_parcial_min: Optional[float] = None
    fonte_metricas: Optional[str] = None


@dataclass
class ClusterTransferencia:
    cluster_id: int
    centro_lat: float
    centro_lon: float
    peso_total: float
    valor_nf_total: float
    valor_frete_total: float

# ==========================================================
# 🔥 NOVO: PARAMS DE SIMULAÇÃO (PADRÃO ÚNICO)
# ==========================================================

ModoSimulacao = Literal["padrao", "balanceado", "time_windows"]


class SimulationParams(BaseModel):

    # ==========================================================
    # 🔥 MODO (FONTE ÚNICA)
    # ==========================================================

    data_inicial: date
    data_final: Optional[date] = None
    hub_id: int
    modo_forcar: bool = False

    modo_simulacao: ModoSimulacao = "padrao"

    algoritmo_clusterizacao: Optional[Literal["kmeans", "balanced_kmeans"]] = None
    algoritmo_roteirizacao: Optional[Literal["heuristico", "time_windows"]] = None
    modo_forcar: bool = False

    # ==========================================================
    # HUB
    # ==========================================================
    desativar_cluster_hub: bool = False
    raio_hub_km: float = Field(80.0, gt=0)

    # 🔥 NOVO
    fator_ocupacao_hub: float = Field(0.85, gt=0, le=1)

    # ==========================================================
    # OUTLIERS
    # ==========================================================
    usar_outlier: bool = False
    distancia_outlier_km: Optional[float] = Field(None, gt=0)


    # ==========================================================
    # CLUSTER
    # ==========================================================
    min_entregas_por_cluster_alvo: int = Field(10, gt=0)
    max_entregas_por_cluster_alvo: int = Field(100, gt=0)

    # ==========================================================
    # 🔥 CAPACIDADE (PADRÃO ÚNICO)
    # ==========================================================
    entregas_por_rota: int = Field(25, gt=0)

    # ==========================================================
    # TEMPO OPERACIONAL
    # ==========================================================
    tempo_parada_leve: float = Field(10.0, ge=0)
    tempo_parada_pesada: float = Field(20.0, ge=0)
    tempo_por_volume: float = Field(0.4, ge=0)
    limite_peso_parada: float = Field(200.0, gt=0)

    # ==========================================================
    # VELOCIDADE
    # ==========================================================
    velocidade_kmh: float = Field(45.0, gt=0)

    # ==========================================================
    # CAPACIDADE DE CARGA
    # ==========================================================
    limite_peso_veiculo: float = Field(50.0, gt=0)
    peso_max_transferencia: float = Field(18000.0, gt=0)

    # ==========================================================
    # TEMPO LIMITE
    # ==========================================================
    tempo_max_roteirizacao: int = Field(600, gt=0)
    tempo_max_k0: int = Field(600, gt=0)
    tempo_max_transferencia: int = Field(600, gt=0)

    # ==========================================================
    # REGRAS OPERACIONAIS
    # ==========================================================
    permitir_rotas_excedentes: bool = True
    permitir_veiculo_leve_intermunicipal: bool = False

    # ==========================================================
    # TIME WINDOWS
    # ==========================================================
    tempo_especial_min: int = Field(180, ge=0)
    tempo_especial_max: int = Field(300, ge=0)
    max_especiais_por_rota: int = Field(1, ge=0)

    # ==========================================================
    # REFINAMENTO
    # ==========================================================
    max_refinamentos_subcluster: int = Field(4, ge=1)
    max_particoes_subcluster_local: int = Field(4, ge=2)
    max_tentativas_refino_cluster: int = Field(0, ge=0)
    max_tentativas_refino_subcluster: int = Field(4, ge=1)
    penalty_drop_node: int = 10000
    penalty_excedente: int = 1000000

    cobertura_minima_tempo: float = Field(0.9, ge=0, le=1)
    cobertura_minima_peso: float = Field(0.95, ge=0, le=1)
    cobertura_minima_volumes: float = Field(0.9, ge=0, le=1)
    cobertura_minima_prazo: float = Field(0.9, ge=0, le=1)

    multiplicador_iqr: float = Field(1.5, gt=0)
    quantil_referencia: float = Field(0.98, gt=0, lt=1)
    distancia_minima_km: float = Field(25.0, gt=0)
    percentual_maximo: float = Field(0.08, gt=0, lt=1)

    fator_correcao_distancia: float = Field(default=1.3)

    # ==========================================================
    # CONFIG
    # ==========================================================
    class Config:
        extra = "ignore"

    # ==========================================================
    # 🔥 VALIDAÇÕES CRÍTICAS
    # ==========================================================


    @model_validator(mode="after")
    def validar_params(self):
        if self.entregas_por_rota is None or self.entregas_por_rota <= 0:
            self.entregas_por_rota = 25
        return self