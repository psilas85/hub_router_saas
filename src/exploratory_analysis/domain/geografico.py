# exploratory_analysis/domain/geografico.py

import pandas as pd

MAX_PONTOS = 5000


def calcular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"pontos": [], "total_com_coordenadas": 0, "total_sem_coordenadas": 0}

    df_geo = df.dropna(subset=["destino_latitude", "destino_longitude"])
    total_com_coordenadas = len(df_geo)
    total_sem_coordenadas = len(df) - total_com_coordenadas

    if len(df_geo) > MAX_PONTOS:
        df_geo = df_geo.sample(MAX_PONTOS, random_state=42)

    pontos = [
        {
            "lat": float(row["destino_latitude"]),
            "lon": float(row["destino_longitude"]),
            "valor_nf": round(float(row["cte_valor_nf"]), 2) if pd.notnull(row.get("cte_valor_nf")) else 0.0,
            "destinatario_nome": str(row.get("destinatario_nome") or ""),
            "cidade": str(row.get("cte_cidade") or ""),
        }
        for _, row in df_geo.iterrows()
    ]

    return {
        "pontos": pontos,
        "total_com_coordenadas": total_com_coordenadas,
        "total_sem_coordenadas": total_sem_coordenadas,
    }
