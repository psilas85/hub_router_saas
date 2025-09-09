# hub_router_1.0.1/src/machine_learning/k_cluster/domain/services.py

from __future__ import annotations
from typing import Protocol, Optional, List, Dict
import pandas as pd
from .entities import KClusterTrainingMetrics, KClusterPrediction


class ResultsRepository(Protocol):
    def load_resultados_simulacao(
        self, tenant_id: str, start: Optional[str] = None, end: Optional[str] = None
    ) -> pd.DataFrame: ...
    def load_resumo_clusters(
        self, tenant_id: str, start: Optional[str] = None, end: Optional[str] = None
    ) -> pd.DataFrame: ...
    def load_entregas_clusterizadas(
        self, tenant_id: str, start: Optional[str] = None, end: Optional[str] = None
    ) -> pd.DataFrame: ...


class KModelStore(Protocol):
    def save(self, tenant_id: str, payload: Dict) -> str: ...
    def load(self, tenant_id: str) -> Dict: ...


class KClusterForecasterSvc(Protocol):
    def fit(
        self,
        resultados_simulacao: pd.DataFrame,
        resumo_clusters: Optional[pd.DataFrame] = None,
        entregas_clusterizadas: Optional[pd.DataFrame] = None,
        test_size: float = 0.2,
    ) -> KClusterTrainingMetrics: ...

    def predict_k(
        self,
        demand_forecast: pd.DataFrame,
        tenant_id: str,
        envio_data: str,
        candidate_ks: Optional[List[int]] = None,
        top_n: int = 3,
    ) -> KClusterPrediction: ...
