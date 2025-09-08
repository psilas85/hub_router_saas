import os
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from typing import Tuple

from ml_pipeline.infrastructure.dataset_repository import DatasetRepository
from ml_pipeline.models.trainer_factory import TrainerFactory


class TrainUseCase:
    """
    Caso de uso para treinar modelos de planejamento:
    - custo_last_mile
    - custo_transfer
    """

    def __init__(self, repository: DatasetRepository, logger=None):
        self.repo = repository
        self.logger = logger

    def _evaluate_model(self, model, X, y) -> dict:
        """Avalia modelo com métricas clássicas"""
        preds = model.predict(X)
        return {
            "mae": mean_absolute_error(y, preds),
            "rmse": mean_squared_error(y, preds, squared=False),
            "r2": r2_score(y, preds),
        }

    def train_lastmile(self, start_date: str, end_date: str, tenant_id: str) -> Tuple[object, dict]:
        """
        Treina modelo de custo_last_mile usando histórico de simulações.
        """
        df = self.repo.load_simulation_dataset(start_date, end_date, tenant_id)
        if df.empty:
            raise RuntimeError("⚠️ Nenhum dado disponível para treino last-mile")

        features = df[["total_entregas", "total_peso", "total_volumes", "valor_total_nf"]]
        target = df["custo_total"] - df["custo_transfer_total"]

        model = RandomForestRegressor(n_estimators=200, random_state=42)
        model.fit(features, target)

        metrics = self._evaluate_model(model, features, target)

        TrainerFactory.save_trained(model, "lastmile", tenant_id)

        if self.logger:
            self.logger.info(f"✅ Modelo last-mile treinado e salvo | {metrics}")

        return model, metrics

    def train_transfer(self, start_date: str, end_date: str, tenant_id: str) -> Tuple[object, dict]:
        """
        Treina modelo de custo_transfer_total usando histórico de simulações.
        """
        df = self.repo.load_simulation_dataset(start_date, end_date, tenant_id)
        if df.empty:
            raise RuntimeError("⚠️ Nenhum dado disponível para treino transfer")

        features = df[["total_entregas", "total_peso", "total_volumes", "valor_total_nf"]]
        target = df["custo_transfer_total"]

        model = RandomForestRegressor(n_estimators=200, random_state=42)
        model.fit(features, target)

        metrics = self._evaluate_model(model, features, target)

        TrainerFactory.save_trained(model, "transfer", tenant_id)

        if self.logger:
            self.logger.info(f"✅ Modelo transfer treinado e salvo | {metrics}")

        return model, metrics
