# hub_router_1.0.1/src/machine_learning/k_cluster/domain/entities.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class KClusterTrainingMetrics:
    # métricas de classificação (classe rara)
    accuracy: float
    balanced_accuracy: float
    f1: float
    pr_ap: float  # Average Precision (PR-AUC)
    # métricas de negócio
    regret_mean: float
    regret_p50: float
    regret_p90: float
    hit_at_3: float
    # housekeeping
    n_train: int
    n_test: int
    n_features: int


@dataclass
class KAlternative:
    k_clusters: int
    prob_k_otimo: float
    tenant_id: str
    envio_data: str


@dataclass
class KClusterPrediction:
    k_sugerido: int
    alternativas: List[KAlternative]
    distribuicao: List[Dict[str, Any]]
