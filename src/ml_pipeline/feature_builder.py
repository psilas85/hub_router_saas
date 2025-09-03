#ml_pipeline/feature_builder.py

import pandas as pd

class FeatureBuilder:
    def __init__(self):
        # Colunas que não devem entrar no modelo
        self.cols_drop = ["simulation_id", "tenant_id", "envio_data", "created_at"]

    def build_features(self, df: pd.DataFrame):
        """
        Recebe o dataframe bruto e retorna X, y
        """
        # Garantir que coluna alvo existe
        if "is_ponto_otimo" not in df.columns:
            raise ValueError("Coluna 'is_ponto_otimo' não encontrada no dataset.")

        # Remove colunas de controle
        df = df.drop(columns=[c for c in self.cols_drop if c in df.columns], errors="ignore")

        # Separar features e target
        X = df.drop(columns=["is_ponto_otimo"])
        y = df["is_ponto_otimo"]

        return X, y
