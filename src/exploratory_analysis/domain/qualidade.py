# exploratory_analysis/domain/qualidade.py

import pandas as pd


def calcular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"outliers_iqr": [], "zerados": [], "campos_criticos_faltando": []}

    total = len(df)
    cols_numericas = ["cte_peso", "cte_valor_frete", "cte_valor_nf", "cte_volumes"]

    outliers_iqr = []
    for col in cols_numericas:
        if col not in df.columns:
            continue
        serie = df[col].dropna()
        if serie.empty:
            continue
        q1 = serie.quantile(0.25)
        q3 = serie.quantile(0.75)
        iqr = q3 - q1
        lim_inf = max(0.0, float(q1 - 1.5 * iqr))
        lim_sup = float(q3 + 1.5 * iqr)
        n_outliers = int(((serie < lim_inf) | (serie > lim_sup)).sum())
        outliers_iqr.append({
            "coluna": col,
            "total_observacoes": len(serie),
            "outliers": n_outliers,
            "percentual": round(n_outliers / len(serie) * 100, 2),
            "lim_inf": round(lim_inf, 4),
            "lim_sup": round(lim_sup, 4),
        })

    zerados = []
    for col in cols_numericas:
        if col not in df.columns:
            continue
        n_zerados = int((df[col] == 0).sum())
        n_nulos = int(df[col].isna().sum())
        zerados.append({
            "coluna": col,
            "zerados": n_zerados,
            "nulos": n_nulos,
            "pct_zerados": round(n_zerados / total * 100, 2),
            "pct_nulos": round(n_nulos / total * 100, 2),
        })

    campos_criticos = ["cte_peso", "cte_valor_nf", "destinatario_nome", "destino_latitude", "destino_longitude"]
    campos_criticos_faltando = []
    for col in campos_criticos:
        if col not in df.columns:
            campos_criticos_faltando.append({"campo": col, "faltando": total, "pct": 100.0})
        else:
            faltando = int(df[col].isna().sum())
            if faltando > 0:
                campos_criticos_faltando.append({
                    "campo": col,
                    "faltando": faltando,
                    "pct": round(faltando / total * 100, 2),
                })

    return {
        "outliers_iqr": outliers_iqr,
        "zerados": zerados,
        "campos_criticos_faltando": campos_criticos_faltando,
    }
