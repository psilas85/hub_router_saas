#hub_router_1.0.1/src/ml_pipeline/ml_pipeline.py

import os
import logging
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from ml_pipeline.infrastructure.dataset_repository import DatasetRepository
from ml_pipeline.application.ml_use_case import DatasetUseCase
from ml_pipeline.models.trainer_factory import TrainerFactory
from ml_pipeline.infrastructure.model_repository import ModelRepository
from ml_pipeline.preprocessing.feature_preprocessor import FeaturePreprocessor
from ml_pipeline.planning.planning_use_case import PlanningUseCase
from ml_pipeline.interface.costs_clients import CostsTransferClient, CostsLastMileClient
from ml_pipeline.infrastructure.geolocation_adapter import GeolocationAdapter



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
        self.scaler = None
        self.model_repo = ModelRepository()
        self.preprocessor = None

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
        Pré-processamento com FeaturePreprocessor
        """
        self.preprocessor = FeaturePreprocessor(
            scale_numeric=(target_column == "custo_total"),
            logger=self.logger,  # ✅ loga diagnósticos
        )
        X, y = self.preprocessor.fit_transform(df, target_column)

        # Assert final (defensivo)
        if X.isna().any().any():
            null_cols = X.columns[X.isna().any()].tolist()
            self.logger.warning(f"⚠️ Após preprocess, ainda há NaN em {null_cols}. Preenchendo com 0.")
            X = X.fillna(0)

        return X, y


    def train_and_evaluate(self, X_train, X_test, y_train, y_test):
        """
        Treina modelo de regressão linear e avalia
        """
        model = LinearRegression()
        model.fit(X_train, y_train)
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        self.logger.info(f"📊 Treino concluído | MSE={mse:.2f}")
        return model, mse

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

        # Executa pré-processamento
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

        # Corrige possíveis NaNs
        if X.isna().any().any():
            bad_cols = X.columns[X.isna().any()].tolist()
            self.logger.error(f"🚨 Ainda existem NaN nas features: {bad_cols}")
            X = X.fillna(0)

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
                algorithm = os.getenv("ML_ALGO_COST", "linear")
            elif target_column == "is_ponto_otimo":
                algorithm = os.getenv("ML_ALGO_OPTIMO", "logistic")

        self.logger.info(f"🧩 Algoritmo selecionado para {target_column} = {algorithm}")
        trainer = TrainerFactory.get_trainer(target_column, algorithm=algorithm)

        # =======================
        # 4. Treina modelo
        # =======================
        model, (X_eval, y_eval) = trainer.train(X, y)

        # =======================
        # 5. Avalia modelo
        # =======================
        metrics = trainer.evaluate(model, X_eval, y_eval)
        self.logger.info(f"📊 Métricas de avaliação: {metrics}")

        if target_column == "is_ponto_otimo":
            self.logger.info(f"📊 Distribuição de classes (treino): {pd.Series(y).value_counts().to_dict()}")
            self.logger.info(f"📊 Distribuição de classes (validação): {pd.Series(y_eval).value_counts().to_dict()}")

        threshold = getattr(trainer, "threshold", None)

        # =======================
        # 6. Salva modelo
        # =======================
        model_package = {
            "model": model,
            "preprocessor": self.preprocessor,
            "features": self.preprocessor.feature_names,
            "threshold": threshold,
            "metrics": metrics,
            "algorithm": algorithm,
        }

        path = self.model_repo.save_model(tenant_id, dataset_name, target_column, model_package)

        if threshold is not None:
            self.logger.info(f"💾 Modelo salvo em {path} | Threshold={threshold:.2f} | Algoritmo={algorithm}")
        else:
            self.logger.info(f"💾 Modelo salvo em {path} | Algoritmo={algorithm}")

        self.model = model

        # ✅ retorno padronizado SEMPRE em 3 valores
        return model, metrics, algorithm


    def predict(self, features: dict, tenant_id: str, dataset_name: str,
            target_column: str, algorithm: str = None):
        """
        Faz previsão carregando o modelo persistido
        """
        model_package = self.model_repo.load_model(tenant_id, dataset_name, target_column)
        model = model_package["model"]
        preprocessor = model_package.get("preprocessor")
        feature_names = model_package.get("features")
        threshold = model_package.get("threshold", None)
        algorithm_used = model_package.get("algorithm", "desconhecido")  # 👈 recupera algoritmo salvo

        # Montar DataFrame no formato esperado
        X = pd.DataFrame([features])

        # 🔑 Converte colunas boolean -> int
        bool_cols = X.select_dtypes(include=["bool"]).columns.tolist()
        if bool_cols:
            X[bool_cols] = X[bool_cols].astype(int)
            self.logger.info(f"✅ Convertidas colunas boolean para int no predict: {bool_cols}")

        # Reindex
        X = X.reindex(columns=feature_names, fill_value=0)

        # Avisar se faltou alguma feature
        missing = set(feature_names) - set(features.keys())
        if missing:
            self.logger.warning(f"⚠️ Features ausentes no predict: {missing}")

        # Transform
        if preprocessor:
            X_transformed = preprocessor.transform(X)
        else:
            X_transformed = X

        # Classificação com threshold
        if threshold is not None and hasattr(model, "predict_proba"):
            y_prob = model.predict_proba(X_transformed)[:, 1]
            prediction = int((y_prob >= threshold).astype(int)[0])
            self.logger.info(
                f"📌 Predict usando threshold={threshold:.2f} | Prob={y_prob[0]:.4f} | Pred={prediction} | Algoritmo={algorithm_used}"
            )
            return {"prediction": int(prediction), "probability": float(y_prob[0]), "algorithm": algorithm_used}
        else:
            prediction = model.predict(X_transformed)[0]
            self.logger.info(f"📌 Predict padrão | Pred={prediction} | Algoritmo={algorithm_used}")

            if isinstance(prediction, (np.float32, np.float64, float)):
                return {"prediction": float(prediction), "probability": None, "algorithm": algorithm_used}
            else:
                return {"prediction": int(prediction), "probability": None, "algorithm": algorithm_used}



    def run(self, tenant_id: str, start_date: str, end_date: str,
            target_column: str = "custo_total"):
        """
        Executa o pipeline completo de ML (modo standalone)
        """
        self.logger.info("🚀 Iniciando pipeline standalone...")

        df = self.load_data(start_date, end_date, tenant_id)

        # 🔑 Converte colunas boolean -> int (True=1, False=0)
        bool_cols = df.select_dtypes(include=["bool"]).columns.tolist()
        if bool_cols:
            df[bool_cols] = df[bool_cols].astype(int)
            self.logger.info(f"✅ Convertidas colunas boolean para int no run: {bool_cols}")

        # Pré-processamento
        X, y = self.preprocess_data(df, target_column)

        # Para rodar standalone, dividir aqui
        from sklearn.model_selection import train_test_split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        model, mse = self.train_and_evaluate(X_train, X_test, y_train, y_test)

        self.logger.info(f"✅ Pipeline concluído | MSE: {mse:.2f}")
        return model

    def plan_next_months(self, tenant_id: str, start_date: str, months: int = 3,
                     scenarios=("base","baixo","alto"), debug: bool = False,
                     fast: bool = False):

        geo_adapter = GeolocationAdapter()
        ct_client = CostsTransferClient(geo=geo_adapter)
        lm_client = CostsLastMileClient()

        planner = PlanningUseCase(
            repository=self.repository,
            ml_pipeline=self,
            ct_client=ct_client,
            lm_client=lm_client,
            logger=self.logger
        )

        return planner.recommend_structure(
            tenant_id=tenant_id,
            start_date=start_date,
            months=months,
            scenarios=scenarios,
            debug=debug,
            fast=fast  # 👈 novo
        )



if __name__ == "__main__":
    tenant = os.getenv("DEFAULT_TENANT_ID", "default-tenant")
    pipeline = MLPipeline()
    pipeline.run(
        tenant_id=tenant,
        start_date="2024-08-01",
        end_date="2024-08-31",
        target_column="custo_total"
    )
