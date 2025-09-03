#hub_router_1.0.1/src/ml_pipeline/planning/demand_forecaster.py

import pandas as pd


class DemandForecaster:
    """
    Forecast baseline simples por cidade/dia (média por weekday).
    Depois você pode trocar o miolo por SARIMAX/XGBoost sem mudar a interface.
    """
    def __init__(self, repository, logger=None):
        self.repository = repository
        self.logger = logger

    def forecast_city_daily(self, start_date: str, months: int, tenant_id: str, scenario: str = "base") -> pd.DataFrame:
        # 🔎 Histórico vem do clusterization_db
        hist = self.repository.load_city_daily_history(tenant_id)
        if self.logger:
            self.logger.info(f"📊 DemandForecaster: histórico carregado do clusterization_db ({hist.shape})")

        hist["data"] = pd.to_datetime(hist["data"])
        hist["weekday"] = hist["data"].dt.weekday

        # Média por cidade/uf/dia da semana
        wk = (
            hist.groupby(["cidade", "uf", "weekday"])[["entregas", "peso", "volumes", "valor_nf"]]
            .mean()
            .reset_index()
        )

        # Gera grade futura
        future_days = pd.date_range(start=start_date, periods=30 * months, freq="D")
        grid = (
            pd.MultiIndex.from_product([wk["cidade"].unique(), wk["uf"].unique(), future_days],
                                       names=["cidade", "uf", "data"])
            .to_frame(index=False)
        )
        grid["weekday"] = grid["data"].dt.weekday

        # Junta baseline
        df = grid.merge(wk, on=["cidade", "uf", "weekday"], how="left").fillna(0)

        # Aplica fator do cenário
        factor = {"baixo": 0.9, "base": 1.0, "alto": 1.1}.get(scenario, 1.0)
        for c in ["entregas", "peso", "volumes", "valor_nf"]:
            df[c] = (df[c] * factor).round(0)

        if self.logger:
            self.logger.info(f"✅ DemandForecaster: forecast gerado para cenário '{scenario}' ({len(df)} linhas)")

        return df[["data", "cidade", "uf", "entregas", "peso", "volumes", "valor_nf"]]
