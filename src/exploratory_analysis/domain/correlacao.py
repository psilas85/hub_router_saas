# exploratory_analysis/domain/correlacao.py

import pandas as pd

VARIAVEIS = ["cte_peso", "cte_valor_nf", "cte_valor_frete", "cte_volumes"]


def calcular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"variaveis": VARIAVEIS, "matriz": []}

    cols = [c for c in VARIAVEIS if c in df.columns]
    df_corr = df[cols].dropna()

    if df_corr.empty:
        return {"variaveis": cols, "matriz": []}

    corr = df_corr.corr()

    matriz = [
        {
            "var_x": var_x,
            "var_y": var_y,
            "r": round(float(corr.loc[var_x, var_y]), 4),
        }
        for var_x in cols
        for var_y in cols
    ]

    return {"variaveis": cols, "matriz": matriz}
