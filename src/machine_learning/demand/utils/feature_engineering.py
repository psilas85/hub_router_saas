#hub_router_1.0.1/src/machine_learning/demand/utils/feature_engineering.py

import pandas as pd
import numpy as np
from workalendar.america import Brazil

cal = Brazil()


def build_features(
    df: pd.DataFrame,
    target_col: str = "entregas",
    lags: list = [1, 7, 30],
    rolling_windows: list = [3, 7, 14, 30],
) -> pd.DataFrame:
    """
    Constrói features temporais, sazonais, lags, rollings e interações.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame com colunas ["dt", "cidade", "uf", target_col].
    target_col : str
        Nome da coluna de demanda (default = "entregas").
    lags : list
        Defasagens (default: [1,7,30]).
    rolling_windows : list
        Janelas para médias móveis (default: [3,7,14,30]).

    Retorno
    -------
    df_feat : pd.DataFrame
        DataFrame com features adicionais.
    """

    df = df.copy()
    df["dt"] = pd.to_datetime(df["dt"])

    # =====================
    # 📅 Features de calendário
    # =====================
    df["year"] = df["dt"].dt.year.astype("int32")
    df["month"] = df["dt"].dt.month.astype("int32")
    df["day"] = df["dt"].dt.day.astype("int32")
    df["dayofweek"] = df["dt"].dt.dayofweek.astype("int32")
    df["is_weekend"] = df["dayofweek"].isin([5, 6]).astype("int8")

    # Sazonalidade expandida
    df["quarter"] = df["dt"].dt.quarter.astype("int32")
    df["weekofyear"] = df["dt"].dt.isocalendar().week.astype("int32")
    df["is_month_start"] = df["dt"].dt.is_month_start.astype("int8")
    df["is_month_end"] = df["dt"].dt.is_month_end.astype("int8")

    # Feriados nacionais
    df["is_holiday"] = df["dt"].apply(lambda x: int(cal.is_holiday(x)))

    # =====================
    # ⏪ Lag features
    # =====================
    for lag in lags:
        df[f"lag_{lag}"] = df.groupby("cidade")[target_col].shift(lag)

    # =====================
    # 📊 Rolling windows (média + desvio)
    # =====================
    for window in rolling_windows:
        grp = df.groupby("cidade")[target_col]
        df[f"roll_mean_{window}"] = (
            grp.shift(1).rolling(window=window, min_periods=1).mean().reset_index(level=0, drop=True)
        )
        df[f"roll_std_{window}"] = (
            grp.shift(1).rolling(window=window, min_periods=1).std().reset_index(level=0, drop=True)
        )

    # =====================
    # 🔀 Codificação categórica
    # =====================
    df = pd.get_dummies(df, columns=["uf"], prefix="uf")

    # =====================
    # ⚡ Interações
    # =====================
    if "volumes_total" in df.columns and target_col in df.columns:
        df["densidade_entrega"] = (
            df["volumes_total"] / df[target_col].replace(0, np.nan)
        ).fillna(0)

    if "peso_total" in df.columns and target_col in df.columns:
        df["peso_medio"] = (
            df["peso_total"] / df[target_col].replace(0, np.nan)
        ).fillna(0)

    # =====================
    # 🚨 Flag de missing
    # =====================
    df["is_missing"] = df[target_col].isna().astype("int8")

    return df
