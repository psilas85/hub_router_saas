# exploratory_analysis/domain/resumo.py

import pandas as pd


def calcular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"totais": {}, "cobertura_datas": {}, "nulos_pct": {}}

    df = df.copy()
    df["envio_data"] = pd.to_datetime(df["envio_data"], errors="coerce")

    totais = {
        "total_entregas": len(df),
        "total_peso": round(float(df["cte_peso"].sum()), 2),
        "total_volumes": int(df["cte_volumes"].sum()),
        "total_valor_nf": round(float(df["cte_valor_nf"].sum()), 2),
        "total_valor_frete": round(float(df["cte_valor_frete"].sum()), 2),
    }

    datas_validas = df["envio_data"].dropna()
    if not datas_validas.empty:
        cobertura_datas = {
            "data_minima": str(datas_validas.min().date()),
            "data_maxima": str(datas_validas.max().date()),
            "dias_cobertos": int((datas_validas.max() - datas_validas.min()).days) + 1,
        }
    else:
        cobertura_datas = {"data_minima": None, "data_maxima": None, "dias_cobertos": 0}

    cols_check = ["cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete", "destino_latitude", "destino_longitude"]
    total = len(df)
    nulos_pct = {
        col: round(df[col].isna().sum() / total * 100, 2) if col in df.columns else 100.0
        for col in cols_check
    }

    return {
        "totais": totais,
        "cobertura_datas": cobertura_datas,
        "nulos_pct": nulos_pct,
    }
