#hub_router_1.0.1/src/ml_pipeline/infrastructure/model_repository.py

import os
import joblib

class ModelRepository:
    def __init__(self, base_path="models"):
        self.base_path = base_path
        os.makedirs(self.base_path, exist_ok=True)

    def get_model_path(self, tenant_id: str, dataset_name: str, target_column: str) -> str:
        path = os.path.join(self.base_path, tenant_id, dataset_name)
        os.makedirs(path, exist_ok=True)
        return os.path.join(path, f"{target_column}.pkl")

    def save_model(self, tenant_id: str, dataset_name: str, target_column: str, model_package: dict):
        path = self.get_model_path(tenant_id, dataset_name, target_column)
        joblib.dump(model_package, path)
        return path

    def load_model(self, tenant_id: str, dataset_name: str, target_column: str) -> dict:
        path = self.get_model_path(tenant_id, dataset_name, target_column)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Modelo não encontrado para {tenant_id}/{dataset_name}/{target_column}")
        return joblib.load(path)
