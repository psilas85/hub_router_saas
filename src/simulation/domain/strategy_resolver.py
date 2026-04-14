#hub_router_1.0.1/src/simulation/domain/strategy_resolver.py

from dataclasses import dataclass
from simulation.domain.entities import SimulationParams


@dataclass
class EstrategiaResolvida:
    algoritmo_clusterizacao: str
    algoritmo_roteirizacao: str


def resolver_estrategia(params: SimulationParams) -> EstrategiaResolvida:

    # 🔥 modo principal
    if params.modo_simulacao == "padrao":
        cluster = "kmeans"
        routing = "heuristico"

    elif params.modo_simulacao == "balanceado":
        cluster = "balanced_kmeans"
        routing = "heuristico"

    elif params.modo_simulacao == "time_windows":
        cluster = "balanced_kmeans"
        routing = "time_windows"

    else:
        raise ValueError(f"Modo inválido: {params.modo_simulacao}")

    # 🔥 override (se vier do front)
    if params.algoritmo_clusterizacao:
        cluster = params.algoritmo_clusterizacao

    if params.algoritmo_roteirizacao:
        routing = params.algoritmo_roteirizacao

    # 🔥 validação crítica
    if routing == "time_windows" and cluster == "kmeans":
        raise ValueError("Time Windows exige balanced_kmeans")

    return EstrategiaResolvida(cluster, routing)