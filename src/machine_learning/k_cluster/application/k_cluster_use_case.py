# hub_router_1.0.1/src/machine_learning/k_cluster/application/k_cluster_use_case.py
from __future__ import annotations
from dataclasses import asdict
from typing import Optional, List, Dict
import pandas as pd

from ..domain.entities import KClusterTrainingMetrics, KClusterPrediction
from ..domain.services import ResultsRepository, KModelStore
from ..models.k_cluster_forecaster import KClusterForecaster
from ..utils.feature_builder import FeatureBuilder, FeatureConfig


class KClusterUseCase:
    def __init__(self, repo: ResultsRepository, model_store: KModelStore, cfg: FeatureConfig = FeatureConfig()):
        self.repo = repo
        self.store = model_store
        self.fb = FeatureBuilder(cfg)
        self.model = KClusterForecaster(self.fb)

    def train(self, tenant_id: str, start: Optional[str] = None, end: Optional[str] = None, test_size: float = 0.2) -> Dict:
        resultados = self.repo.load_resultados_simulacao(tenant_id, start, end)
        resumo = self.repo.load_resumo_clusters(tenant_id, start, end)
        entregas = self.repo.load_entregas_clusterizadas(tenant_id, start, end)

        metrics: KClusterTrainingMetrics = self.model.fit(resultados, resumo, entregas, test_size=test_size)

        payload = {"model": self.model.model, "feature_cols": self.fb._feature_cols_, "cfg": self.fb.cfg}
        path = self.store.save(tenant_id, payload)

        # retorna métricas completas (inclui regret/hit@3 e PR-AP)
        return {"detail": "Treino concluído", "metrics": asdict(metrics), "model_path": path}

    def predict(
        self,
        tenant_id: str,
        envio_data: str,
        demand_forecast: pd.DataFrame,
        candidate_ks: Optional[List[int]] = None,
        top_n: int = 3,
    ) -> Dict:
        payload = self.store.load(tenant_id)
        self.model.model = payload["model"]
        self.fb._feature_cols_ = payload.get("feature_cols", None)
        self.fb.cfg = payload.get("cfg", self.fb.cfg)

        pred: KClusterPrediction = self.model.predict_k(
            demand_forecast=demand_forecast,
            tenant_id=tenant_id,
            envio_data=envio_data,
            candidate_ks=candidate_ks,
            top_n=top_n,
        )

        return {
            "k_sugerido": pred.k_sugerido,
            "alternativas": [asdict(a) for a in pred.alternativas],
            "distribuicao": pred.distribuicao,
        }
