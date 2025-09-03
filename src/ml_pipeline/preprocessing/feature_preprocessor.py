#ml_pipeline/preprocessing/feature_preprocessor.py

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer

class FeaturePreprocessor:
    def __init__(self, scale_numeric: bool = True, logger=None):
        self.scale_numeric = scale_numeric
        self.scaler = None
        self.imputer = None
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

        # 2) tenta converter colunas object para número
        obj_cols = [c for c in df.columns if df[c].dtype == "object"]
        for c in obj_cols:
            coerced = pd.to_numeric(df[c], errors="coerce")
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

        # imputação
        self.imputer = SimpleImputer(strategy="median")
        X_imputed = self.imputer.fit_transform(X)
        X = pd.DataFrame(X_imputed, columns=X.columns, index=df.index)

        # escala
        if self.scale_numeric:
            self.scaler = StandardScaler()
            X_scaled = self.scaler.fit_transform(X)
            X = pd.DataFrame(X_scaled, columns=X.columns, index=df.index)

        if X.isna().any().any():
            if self.logger:
                null_cols = X.columns[X.isna().any()].tolist()
                self.logger.warning(f"⚠️ Ainda há NaN após imputar/escalar em: {null_cols}. Preenchendo com 0.")
            X = X.fillna(0)

        self.feature_names = list(X.columns)
        return X, df[target_column]

    def transform(self, X: pd.DataFrame):
        if self.imputer is None:
            raise RuntimeError("Pré-processador não treinado. Chame fit_transform antes.")

        X = X.copy()
        X = self._coerce_numeric(X)
        X = self._select_numeric(X)

        for col in self.feature_names:
            if col not in X.columns:
                X[col] = np.nan
        X = X[self.feature_names]

        X_imputed = self.imputer.transform(X)
        X = pd.DataFrame(X_imputed, columns=self.feature_names, index=X.index)

        if self.scale_numeric and self.scaler is not None:
            X_scaled = self.scaler.transform(X)
            X = pd.DataFrame(X_scaled, columns=self.feature_names, index=X.index)

        if X.isna().any().any():
            if self.logger:
                null_cols = X.columns[X.isna().any()].tolist()
                self.logger.warning(f"⚠️ Ainda há NaN no transform em: {null_cols}. Preenchendo com 0.")
            X = X.fillna(0)

        return X
