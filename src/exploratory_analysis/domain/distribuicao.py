# exploratory_analysis/domain/distribuicao.py

import pandas as pd
import numpy as np


def _histograma(serie: pd.Series, bins: int = 30) -> list:
    serie_limpa = serie.dropna()
    serie_limpa = serie_limpa[serie_limpa > 0]
    if serie_limpa.empty:
        return []
    counts, edges = np.histogram(serie_limpa, bins=bins)
    return [
        {"bin_label": f"{edges[i]:.1f}–{edges[i + 1]:.1f}", "count": int(counts[i])}
        for i in range(len(counts))
    ]


def calcular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"peso": [], "valor_nf": [], "valor_frete": [], "volumes": [], "frete_sobre_nf": []}

    frete_sobre_nf: pd.Series = pd.Series(dtype=float)
    if "cte_valor_frete" in df.columns and "cte_valor_nf" in df.columns:
        df_valid = df[(df["cte_valor_nf"] > 0) & (df["cte_valor_frete"] > 0)]
        frete_sobre_nf = df_valid["cte_valor_frete"] / df_valid["cte_valor_nf"]

    return {
        "peso": _histograma(df["cte_peso"]) if "cte_peso" in df.columns else [],
        "valor_nf": _histograma(df["cte_valor_nf"]) if "cte_valor_nf" in df.columns else [],
        "valor_frete": _histograma(df["cte_valor_frete"]) if "cte_valor_frete" in df.columns else [],
        "volumes": _histograma(df["cte_volumes"]) if "cte_volumes" in df.columns else [],
        "frete_sobre_nf": _histograma(frete_sobre_nf),
    }
