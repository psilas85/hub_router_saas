# exploratory_analysis/domain/rankings.py

import pandas as pd


def calcular(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"top_frequencia": [], "top_valor_nf": [], "top_cidades": []}

    top_frequencia = (
        df.groupby(["destinatario_nome", "cte_cidade", "cte_uf"])
        .size()
        .reset_index(name="qtd_entregas")
        .sort_values("qtd_entregas", ascending=False)
        .head(20)
        .to_dict(orient="records")
    )

    top_valor_nf = (
        df.groupby(["destinatario_nome", "cte_cidade", "cte_uf"])["cte_valor_nf"]
        .sum()
        .reset_index(name="valor_total_nf")
        .sort_values("valor_total_nf", ascending=False)
        .head(20)
        .assign(valor_total_nf=lambda x: x["valor_total_nf"].round(2))
        .to_dict(orient="records")
    )

    df = df.copy()
    df["cte_cidade"] = df["cte_cidade"].fillna("INDEFINIDO")
    top_cidades = (
        df.groupby(["cte_cidade", "cte_uf"]).agg(
            qtd_entregas=("cte_numero", "count"),
            valor_total_nf=("cte_valor_nf", "sum"),
        )
        .reset_index()
        .sort_values("qtd_entregas", ascending=False)
        .head(20)
        .assign(valor_total_nf=lambda x: x["valor_total_nf"].round(2))
        .to_dict(orient="records")
    )

    return {
        "top_frequencia": top_frequencia,
        "top_valor_nf": top_valor_nf,
        "top_cidades": top_cidades,
    }
