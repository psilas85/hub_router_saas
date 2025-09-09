#hub_router_1.0.1/src/machine_learning/demand/application/demand_use_case.py

import pandas as pd
import unidecode
from ..infrastructure.demand_repository import DemandRepository
from ..models.baselines import seasonal_naive_forecast
from ..models.xgboost_forecast import xgboost_forecast   # 👈 novo import
from ..utils.feature_engineering import build_features  # já disponível para expandir depois
from ..models.lightgbm_forecast import lightgbm_forecast



def normalize(text: str):
    """Normaliza nomes de cidades para comparação robusta."""
    return unidecode.unidecode(text.strip().lower()) if text else ""


class DemandUseCase:
    def __init__(self, repo: DemandRepository):
        self.repo = repo

    def forecast_demand(
        self,
        tenant_id: str,
        start_date: str,
        end_date: str,
        cidade: str,
        horizon: int = 7,
        method: str = "baseline",     # 👈 parâmetro do modelo
        with_features: bool = False,
    ) -> pd.DataFrame:
        """
        Gera previsão de demanda para uma cidade.

        Retorno
        -------
        forecast : pd.DataFrame
            DataFrame com previsões de demanda (+ métricas se method=xgboost).
        """

        # carrega histórico
        df = self.repo.fetch_daily_city(tenant_id, start_date, end_date)

        # normaliza nomes de cidades
        df["cidade_norm"] = df["cidade"].apply(normalize)
        cidade_norm = normalize(cidade)

        # filtra cidade
        df_city = df[df["cidade_norm"] == cidade_norm].sort_values("dt")

        if df_city.empty:
            raise ValueError(f"Nenhum dado encontrado para cidade={cidade}")

        # ========================
        # Baseline sazonal
        # ========================
        if method == "baseline":
            y = df_city["entregas"]
            preds = seasonal_naive_forecast(y, steps=horizon)

            future_dates = pd.date_range(
                start=df_city["dt"].max() + pd.Timedelta(days=1),
                periods=horizon,
                freq="D"
            )

            forecast = pd.DataFrame({
                "dt": future_dates,
                "cidade": cidade,
                "pred_entregas": preds
            })
            # baseline não tem métricas
            forecast["mae"] = None
            forecast["rmse"] = None
            forecast["r2"] = None

        # ========================
        # XGBoost
        # ========================
        elif method == "xgboost":
            forecast, metrics, _ = xgboost_forecast(df_city, target_col="entregas", horizon=horizon)
            print("📊 Métricas XGBoost:", metrics)

            # adiciona métricas em todas as linhas do forecast
            forecast["mae"] = metrics["mae"]
            forecast["rmse"] = metrics["rmse"]
            forecast["r2"] = metrics["r2"]

        elif method == "lightgbm":
            forecast, metrics, _ = lightgbm_forecast(df_city, target_col="entregas", horizon=horizon)
            print("📊 Métricas LightGBM:", metrics)
            forecast["mae"] = metrics["mae"]
            forecast["rmse"] = metrics["rmse"]
            forecast["r2"] = metrics["r2"]

        else:
            raise ValueError(f"❌ Método {method} não suportado.")


        # opcional: incluir features
        if with_features:
            df_features = build_features(df_city, target_col="entregas")
            return forecast, df_features

        return forecast
