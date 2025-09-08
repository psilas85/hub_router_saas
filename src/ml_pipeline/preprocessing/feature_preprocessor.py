# hub_router_1.0.1/src/ml_pipeline/preprocessing/feature_preprocessor.py

import pandas as pd
import numpy as np

class FeaturePreprocessor:
    """
    Pré-processador mínimo:
    - Converte bool->int
    - Coerção para numérico onde possível
    - Seleção apenas de colunas numéricas
    - Congela feature_names
    Obs.: Imputação e escala ficam por conta dos Pipelines dos Trainers.
    """
    def __init__(self, scale_numeric: bool = False, logger=None):
        self.scale_numeric = scale_numeric  # mantido por compatibilidade; não usado aqui
        self.feature_names = []
        self.logger = logger

    def _coerce_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # 0) converte boolean -> int (True=1, False=0)
        bool_cols = df.select_dtypes(include=["bool"]).columns.tolist()
        if bool_cols:
            df[bool_cols] = df[bool_cols].astype(int)
            if self.logger:
                self.logger.info(f"✅ Convertidas colunas boolean para int: {bool_cols}")

        # 1) normaliza inf/-inf -> NaN
        df.replace([np.inf, -np.inf], np.nan, inplace=True)

        # 2) tenta converter colunas object para número quando fizer sentido
        obj_cols = [c for c in df.columns if df[c].dtype == "object"]
        for c in obj_cols:
            coerced = pd.to_numeric(df[c], errors="coerce")
            # se alguma linha virou número, ficamos com coerced (permite colunas semi-numéricas)
            if coerced.notna().sum() > 0:
                df[c] = coerced

        return df

    def _select_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        X = df.select_dtypes(include=[np.number]).copy()
        X.dropna(axis=1, how="all", inplace=True)
        return X

    def fit_transform(self, df: pd.DataFrame, target_column: str):
        if target_column not in df.columns:
            raise ValueError(f"Coluna alvo '{target_column}' não encontrada no dataset")

        df = df.copy()
        df = df[~df[target_column].isna()]  # remove linhas sem target

        if self.logger:
            na_top = df.isna().sum().sort_values(ascending=False).head(10)
            self.logger.info(f"🔎 Top 10 colunas com NaN (pré-coerção):\n{na_top}")

        X_raw = df.drop(columns=[target_column], errors="ignore")
        X_raw = self._coerce_numeric(X_raw)
        X = self._select_numeric(X_raw)

        if X.empty:
            raise ValueError("Nenhuma feature numérica disponível para treino.")

        # não imputar/escalar aqui — Pipelines tratam isso
        if X.isna().any().any() and self.logger:
            null_cols = X.columns[X.isna().any()].tolist()
            self.logger.info(f"ℹ️ Há NaN em {null_cols}; imputação ocorrerá no Pipeline.")

        self.feature_names = list(X.columns)
        return X, df[target_column]

    def transform(self, X: pd.DataFrame):
        """
        Transform mínimo: coerção/seleção e alinhamento às feature_names do treino.
        Não executa imputação/escala (de responsabilidade do Pipeline do modelo).
        """
        if not self.feature_names:
            raise RuntimeError("Pré-processador não treinado. Chame fit_transform antes.")

        X = X.copy()
        X = self._coerce_numeric(X)
        X = self._select_numeric(X)

        # adiciona colunas ausentes como NaN e ordena conforme treino
        for col in self.feature_names:
            if col not in X.columns:
                X[col] = np.nan
        X = X[self.feature_names]

        if X.isna().any().any() and self.logger:
            null_cols = X.columns[X.isna().any()].tolist()
            self.logger.info(f"ℹ️ Transform com NaN em {null_cols}; Pipeline imputará.")

        return X
