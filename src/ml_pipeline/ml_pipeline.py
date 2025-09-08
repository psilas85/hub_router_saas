# hub_router_1.0.1/src/ml_pipeline/ml_pipeline.py

import os
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple

# ✅ métricas seguras para classificação (AUC/F1/threshold/intervalos sem NaN)
from utils.metrics_safe import compute_classif_metrics
# ✅ sanitizador JSON-safe (NaN/Inf -> None) para qualquer payload/metrics
from utils.json_sanitize import clean_for_json

from ml_pipeline.infrastructure.dataset_repository import DatasetRepository
from ml_pipeline.application.ml_use_case import DatasetUseCase
from ml_pipeline.models.trainer_factory import TrainerFactory
from ml_pipeline.infrastructure.model_repository import ModelRepository
from ml_pipeline.preprocessing.feature_preprocessor import FeaturePreprocessor
from ml_pipeline.planning.planning_use_case import PlanningUseCase
from ml_pipeline.interface.costs_clients import CostsTransferClient, CostsLastMileClient
from ml_pipeline.infrastructure.geolocation_adapter import GeolocationAdapter


def _json_safe(obj: Any) -> Any:
    """Normaliza tipos numpy e elimina NaN/Inf (usando clean_for_json)."""
    if isinstance(obj, (np.floating, np.float32, np.float64)):
        obj = float(obj)
    if isinstance(obj, (np.integer,)):
        obj = int(obj)
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return clean_for_json(obj)


class MLPipeline:
    def __init__(self):
        """
        Inicializa pipeline carregando configurações do banco via .env
        """
        self.db_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "dbname": os.getenv("POSTGRES_DB", "postgres"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }
        self.repository = DatasetRepository(self.db_config)
        self.dataset_use_case = DatasetUseCase(self.repository)
        self.model = None
        self.model_repo = ModelRepository()
        self.preprocessor = None

        # 🔁 cache leve in-process para evitar I/O repetido em predict
        # chave = (tenant_id, dataset_name, target_column)
        self._model_cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def load_data(self, start_date: str, end_date: str, tenant_id: str) -> pd.DataFrame:
        """
        Busca dados de simulação direto do banco
        """
        self.logger.info(f"📥 Carregando dados de {start_date} até {end_date} | Tenant={tenant_id}")
        df = self.dataset_use_case.get_simulation_data(start_date, end_date, tenant_id)
        self.logger.info(f"✅ Dataset carregado: {len(df)} registros")
        return df

    def preprocess_data(self, df: pd.DataFrame, target_column: str):
        """
        Pré-processamento mínimo: coerção/seleção numérica + nomes de features.
        **Imputação e escala ficam a cargo dos Pipelines dos Trainers** (evita leakage/duplicidade).
        """
        self.preprocessor = FeaturePreprocessor(
            scale_numeric=False,   # <<< desativado aqui; pipelines do Trainer tratam isso
            logger=self.logger,
        )
        X, y = self.preprocessor.fit_transform(df, target_column)

        # Assert final (defensivo) — não preencher NaN aqui (SimpleImputer do pipeline cuida)
        if X.isna().any().any():
            null_cols = X.columns[X.isna().any()].tolist()
            self.logger.info(f"ℹ️ Após preprocess, há NaN em {null_cols}. Pipelines tratarão via Imputer.")

        return X, y

    def _evaluate_classification_safely(
        self,
        trainer,
        model,
        X_eval: pd.DataFrame,
        y_eval: pd.Series
    ) -> Dict[str, Any]:
        """
        Usa compute_classif_metrics com y_prob quando possível.
        Fallback para evaluate() do trainer se necessário.
        """
        try:
            if hasattr(model, "predict_proba"):
                y_prob = model.predict_proba(X_eval)[:, 1]
                metrics = compute_classif_metrics(y_true=y_eval, y_prob=y_prob)
            else:
                metrics = trainer.evaluate(model, X_eval, y_eval)
        except Exception as e:
            self.logger.warning(f"⚠️ compute_classif_metrics falhou, fallback para trainer.evaluate: {e}")
            metrics = trainer.evaluate(model, X_eval, y_eval)

        return metrics

    def _with_intervals(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Constrói dict com *_interval quando existirem pares *_cv_mean/_cv_std.
        Mantém demais chaves e normaliza números.
        """
        out = {}
        for k, v in metrics.items():
            if k.endswith("_cv_mean"):
                base = k.replace("_cv_mean", "")
                std_key = f"{base}_cv_std"
                if std_key in metrics:
                    out[base] = round(float(metrics[k]), 4)
                    out[f"{base}_interval"] = f"± {float(metrics[std_key]):.4f}"
            elif not k.endswith("_cv_std"):
                out[k] = v
        return _json_safe(out)

    def train(self, tenant_id: str, dataset_name: str, target_column: str,
              start_date: str, end_date: str, algorithm: str = None, fast: bool = False):

        # =======================
        # 1. Carrega dados
        # =======================
        df = self.load_data(start_date, end_date, tenant_id)

        if fast and not df.empty:
            df = df.sample(n=min(500, len(df)), random_state=42)
            self.logger.info(f"⚡ FAST MODE: dataset reduzido para {len(df)} registros")

        if df.empty:
            raise RuntimeError(
                f"Dataset vazio! tenant={tenant_id}, dataset={dataset_name}, "
                f"período={start_date}..{end_date}"
            )

        # 🔎 Log inicial
        self.logger.info(f"🔎 Colunas carregadas: {list(df.columns)}")
        self.logger.info(f"🔎 Primeiras linhas do dataset:\n{df.head()}")

        # =======================
        # 2. Pré-processamento
        # =======================
        # Converte boolean -> int
        bool_cols = df.select_dtypes(include=["bool"]).columns.tolist()
        if bool_cols:
            df[bool_cols] = df[bool_cols].astype(int)
            self.logger.info(f"✅ Convertidas colunas boolean para int no train: {bool_cols}")

        # Executa pré-processamento (sem imputar/escalar)
        X, y = self.preprocess_data(df, target_column)

        self.logger.info(f"🔎 Shape pós-preprocess: X={X.shape}, y={y.shape}")
        self.logger.info(f"🔎 Features usadas (até 10): {list(X.columns)[:10]}... (total={len(X.columns)})")
        self.logger.info(f"🔎 Target amostra: {y.head().tolist()}")

        if X.empty:
            raise RuntimeError(f"Nenhuma feature disponível após preprocessamento. "
                               f"Tenant={tenant_id}, dataset={dataset_name}, período={start_date}..{end_date}")

        if y.empty:
            raise RuntimeError(f"Nenhum target disponível após preprocessamento. "
                               f"Tenant={tenant_id}, dataset={dataset_name}, período={start_date}..{end_date}")

        if y.isna().any():
            self.logger.error(f"🚨 Target '{target_column}' tem {y.isna().sum()} NaNs. Droppando...")
            mask = ~y.isna()
            X = X.loc[mask]
            y = y.loc[mask]

        # =======================
        # 3. Escolhe treinador
        # =======================
        if algorithm is None:
            if target_column == "custo_total":
                algorithm = os.getenv("ML_ALGO_COST", "random_forest")
            elif target_column == "is_ponto_otimo":
                algorithm = os.getenv("ML_ALGO_OPTIMO", "logistic")

        self.logger.info(f"🧩 Algoritmo selecionado para {target_column} = {algorithm}")
        trainer = TrainerFactory.get_trainer(target_column, algorithm=algorithm)

        # =======================
        # 4. Treina modelo
        # =======================
        model, (X_eval, y_eval) = trainer.train(X, y)

        # =======================
        # 5. Avalia modelo (robusto)
        # =======================
        if target_column == "is_ponto_otimo":
            metrics_raw = self._evaluate_classification_safely(trainer, model, X_eval, y_eval)
            threshold = getattr(trainer, "threshold", None)
            if threshold is not None and "threshold" not in metrics_raw:
                metrics_raw["threshold"] = float(threshold)
        else:
            metrics_raw = trainer.evaluate(model, X_eval, y_eval)
            threshold = None

        metrics_with_intervals = self._with_intervals(metrics_raw)
        self.logger.info(f"📊 Métricas de avaliação: {metrics_with_intervals}")

        if target_column == "is_ponto_otimo":
            self.logger.info(f"📊 Distribuição de classes (treino): {pd.Series(y).value_counts(normalize=True).round(3).to_dict()}")
            self.logger.info(f"📊 Distribuição de classes (validação): {pd.Series(y_eval).value_counts(normalize=True).round(3).to_dict()}")

        # =======================
        # 6. Salva modelo (+ cache)
        # =======================
        model_package = {
            "model": model,  # pode ser um Pipeline
            "preprocessor": self.preprocessor,  # guarda feature_names/normalização leve
            "features": self.preprocessor.feature_names,
            "threshold": threshold,
            "metrics": metrics_with_intervals,
            "algorithm": algorithm,
        }

        path = self.model_repo.save_model(tenant_id, dataset_name, target_column, model_package)
        cache_key = (tenant_id, dataset_name, target_column)
        self._model_cache[cache_key] = model_package  # 🔁 cache in-memory

        if threshold is not None:
            self.logger.info(f"💾 Modelo salvo em {path} | Threshold={threshold:.2f} | Algoritmo={algorithm}")
        else:
            self.logger.info(f"💾 Modelo salvo em {path} | Algoritmo={algorithm}")

        self.model = model

        # ✅ retorno padronizado
        return model, metrics_with_intervals, algorithm

    def predict(self, features: dict, tenant_id: str, dataset_name: str,
                target_column: str, algorithm: str = None):
        """
        Faz previsão carregando o modelo persistido (com cache em memória).
        """
        cache_key = (tenant_id, dataset_name, target_column)
        if cache_key in self._model_cache:
            model_package = self._model_cache[cache_key]
        else:
            model_package = self.model_repo.load_model(tenant_id, dataset_name, target_column)
            self._model_cache[cache_key] = model_package

        model = model_package["model"]
        preprocessor = model_package.get("preprocessor")
        feature_names = model_package.get("features")
        threshold = model_package.get("threshold", None)
        algorithm_used = model_package.get("algorithm", "desconhecido")

        # Montar DataFrame bruto
        X_raw = pd.DataFrame([features])

        # 🔑 Converte colunas boolean -> int (caso venham no payload)
        bool_cols = X_raw.select_dtypes(include=["bool"]).columns.tolist()
        if bool_cols:
            X_raw[bool_cols] = X_raw[bool_cols].astype(int)
            self.logger.info(f"✅ Convertidas colunas boolean para int no predict: {bool_cols}")

        # Avisar se faltou alguma feature (antes da transformação)
        missing = set(feature_names) - set(X_raw.columns)
        if missing:
            self.logger.warning(f"⚠️ Features ausentes no predict: {missing}")

        # Transform mínimo (coerção/seleção + alinhamento de colunas)
        if preprocessor:
            X_transformed = preprocessor.transform(X_raw)
        else:
            # fallback extremo: reindex e seguir
            X_transformed = X_raw.reindex(columns=feature_names, fill_value=np.nan)

        # Classificação com threshold (modelo com predict_proba dentro do Pipeline)
        if threshold is not None and hasattr(model, "predict_proba"):
            y_prob = model.predict_proba(X_transformed)[:, 1]
            prediction = int((y_prob >= threshold).astype(int)[0])
            self.logger.info(
                f"📌 Predict usando threshold={threshold:.2f} | Prob={y_prob[0]:.4f} | Pred={prediction} | Algoritmo={algorithm_used}"
            )
            return _json_safe({"prediction": int(prediction), "probability": float(y_prob[0]), "algorithm": algorithm_used})

        # Demais casos
        prediction = model.predict(X_transformed)[0]
        self.logger.info(f"📌 Predict padrão | Pred={prediction} | Algoritmo={algorithm_used}")

        if isinstance(prediction, (np.float32, np.float64, float)):
            return _json_safe({"prediction": float(prediction), "probability": None, "algorithm": algorithm_used})
        else:
            return _json_safe({"prediction": int(prediction), "probability": None, "algorithm": algorithm_used})

    def run(self, tenant_id: str, start_date: str, end_date: str,
            target_column: str = "custo_total"):
        """
        Executa o pipeline completo de ML (modo standalone)
        → agora usa TrainerFactory para manter consistência.
        """
        self.logger.info("🚀 Iniciando pipeline standalone...")

        df = self.load_data(start_date, end_date, tenant_id)

        # 🔑 Converte colunas boolean -> int (True=1, False=0)
        bool_cols = df.select_dtypes(include=["bool"]).columns.tolist()
        if bool_cols:
            df[bool_cols] = df[bool_cols].astype(int)
            self.logger.info(f"✅ Convertidas colunas boolean para int no run: {bool_cols}")

        # Pré-processamento mínimo
        X, y = self.preprocess_data(df, target_column)

        # Treina com o TrainerFactory
        default_algo = "random_forest" if target_column == "custo_total" else "logistic"
        trainer = TrainerFactory.get_trainer(target_column, algorithm=default_algo)
        model, (X_test, y_test) = trainer.train(X, y)
        metrics = trainer.evaluate(model, X_test, y_test)
        self.logger.info(f"✅ Pipeline concluído | Métricas: {metrics}")
        return model

    def plan_next_months(
        self,
        tenant_id: str,
        start_date: str,
        months: int = 3,
        scenarios=("base", "baixo", "alto"),
        debug: bool = False,
        fast: bool = False,
    ):
        """
        Recomendação de estrutura baseada em simulações históricas (pontos ótimos).
        - Não roda mais forecast/hubs (isso já foi validado nas simulações diárias).
        - Apenas agrega resultados das simulações existentes.
        """
        from .planning.planning_use_case import PlanningUseCase

        planner = PlanningUseCase(
            repository=self.repository,
            ml_pipeline=self,
            ct_client=None,   # não necessário aqui
            lm_client=None,   # não necessário aqui
            geo_adapter=None, # não necessário aqui
            logger=self.logger,
        )

        return planner.recommend_structure(
            tenant_id=tenant_id,
            start_date=start_date,
            months=months,
            scenarios=scenarios,
            debug=debug,
            fast=fast,
        )


    def evaluate(
        self,
        tenant_id: str,
        dataset_name: str,
        target_column: str,
        features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Avalia o modelo com UMA amostra enviada (diagnóstico pontual).
        Chaves marcadas como *_sample para não confundir com métricas de treino/CV.
        """
        try:
            # 🔎 Recupera modelo treinado do repositório (com cache)
            cache_key = (tenant_id, dataset_name, target_column)
            if cache_key in self._model_cache:
                model_package = self._model_cache[cache_key]
            else:
                model_package = self.model_repo.load_model(tenant_id, dataset_name, target_column)
                self._model_cache[cache_key] = model_package

            model = model_package["model"]
            preprocessor = model_package.get("preprocessor")
            feature_names = model_package.get("features")
            threshold = model_package.get("threshold", None)

            # 🔎 Prepara X no formato correto (sem imputar/escalar aqui)
            X_raw = pd.DataFrame([features]).reindex(columns=list(set(list(features.keys()) + feature_names)), fill_value=np.nan)
            if preprocessor:
                X = preprocessor.transform(X_raw)
            else:
                X = X_raw.reindex(columns=feature_names, fill_value=np.nan)

            # Valor real (se passado no features)
            y_true = np.array([features.get(target_column, 0)])

            # ========================================
            # Regressão (custo_total)
            # ========================================
            if target_column == "custo_total":
                y_pred = model.predict(X)
                mae = float(np.abs(y_pred - y_true).mean())
                mse = float(((y_pred - y_true) ** 2).mean())

                metrics = {
                    "mae_sample": mae,
                    "mse_sample": mse,
                    "r2_sample": None
                }

            # ========================================
            # Classificação (is_ponto_otimo)
            # ========================================
            elif target_column == "is_ponto_otimo":
                if hasattr(model, "predict_proba"):
                    y_prob = model.predict_proba(X)[:, 1]
                    thr = threshold if threshold is not None else 0.5
                    y_pred = (y_prob >= thr).astype(int)
                else:
                    y_pred = model.predict(X)
                    y_prob = None
                    thr = None

                # Para 1 amostra, métricas globais não se aplicam — retornamos versão *_sample
                acc = float((y_pred == y_true).astype(float)[0])
                from sklearn.metrics import f1_score
                f1s = float(f1_score(y_true, y_pred, zero_division=0))

                metrics = {
                    "threshold": float(thr) if thr is not None else None,
                    "accuracy_sample": acc,
                    "f1_sample": f1s,
                    "roc_auc_sample": None
                }

            else:
                raise ValueError(f"Target '{target_column}' não suportado para avaliação.")

            return _json_safe(metrics)

        except Exception as e:
            if self.logger:
                self.logger.warning(f"⚠️ Falha em evaluate para {target_column}: {e}")
            return {}


if __name__ == "__main__":
    tenant = os.getenv("DEFAULT_TENANT_ID", "default-tenant")
    pipeline = MLPipeline()
    pipeline.run(
        tenant_id=tenant,
        start_date="2024-08-01",
        end_date="2024-08-31",
        target_column="custo_total"
    )
