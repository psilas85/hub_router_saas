# exploratory_analysis/domain/temporal.py

import pandas as pd


def calcular(df: pd.DataFrame, granularidade: str) -> dict:
    if df.empty:
        return {"granularidade": granularidade, "series": []}

    df = df.copy()
    df["envio_data"] = pd.to_datetime(df["envio_data"], errors="coerce")
    df = df.dropna(subset=["envio_data"])

    if granularidade == "mensal":
        df["periodo"] = df["envio_data"].dt.to_period("M").dt.to_timestamp()
    elif granularidade == "anual":
        df["periodo"] = df["envio_data"].dt.to_period("Y").dt.to_timestamp()
    else:
        df["periodo"] = df["envio_data"].dt.normalize()

    agrupado = df.groupby("periodo").agg(
        qtd_entregas=("cte_numero", "count"),
        total_peso=("cte_peso", "sum"),
        total_volumes=("cte_volumes", "sum"),
        total_valor_nf=("cte_valor_nf", "sum"),
        total_valor_frete=("cte_valor_frete", "sum"),
    ).reset_index()

    if granularidade == "mensal":
        agrupado["periodo"] = agrupado["periodo"].dt.strftime("%Y-%m")
    elif granularidade == "anual":
        agrupado["periodo"] = agrupado["periodo"].dt.strftime("%Y")
    else:
        agrupado["periodo"] = agrupado["periodo"].dt.strftime("%Y-%m-%d")

    series = [
        {
            "periodo": row["periodo"],
            "qtd_entregas": int(row["qtd_entregas"]),
            "total_peso": round(float(row["total_peso"]), 2),
            "total_volumes": int(row["total_volumes"]),
            "total_valor_nf": round(float(row["total_valor_nf"]), 2),
            "total_valor_frete": round(float(row["total_valor_frete"]), 2),
        }
        for _, row in agrupado.iterrows()
    ]

    return {"granularidade": granularidade, "series": series}
