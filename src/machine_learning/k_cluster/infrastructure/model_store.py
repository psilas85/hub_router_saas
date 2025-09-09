# hub_router_1.0.1/src/machine_learning/k_cluster/infrastructure/model_store.py

from __future__ import annotations
import os
from typing import Dict
import joblib


class LocalKModelStore:
    """Persistência local (disco) para o modelo de k."""

    def __init__(self, base_dir: str = "/app/exports/machine_learning/models/k_cluster"):
        self.base_dir = base_dir

    def _path(self, tenant_id: str) -> str:
        d = os.path.join(self.base_dir, tenant_id)
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, "k_cluster_model.joblib")

    def save(self, tenant_id: str, payload: Dict) -> str:
        path = self._path(tenant_id)
        joblib.dump(payload, path)
        return path

    def load(self, tenant_id: str) -> Dict:
        path = self._path(tenant_id)
        return joblib.load(path)
