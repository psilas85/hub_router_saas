#hub_router_1.0.1/src/simulation/visualization/gerar_resumo_clusterizacao.py

# hub_router_1.0.1/src/simulation/visualization/gerar_resumo_clusterizacao.py

import pandas as pd


def gerar_resumo_clusterizacao(
    db_conn,
    tenant_id: str,
    envio_data: str,
    k_clusters: int,
    simulation_id: str | None = None,
    formatar_valores: bool = True
) -> pd.DataFrame:
    """
    Gera resumo por cluster.

    Retorna DataFrame com:
    - cluster
    - cluster_cidade
    - qtd_entregas
    - peso_total_kg
    - volumes_total
    - valor_nf_total
    - valor_frete_total

    formatar_valores:
        True  -> retorna valores formatados (R$)
        False -> retorna valores numéricos (recomendado para cálculos)
    """

    envio_data = str(envio_data)

    query = """
        SELECT
            cluster,
            cluster_cidade,
            COUNT(*) AS qtd_entregas,
            COALESCE(SUM(cte_peso), 0) AS peso_total_kg,
            COALESCE(SUM(cte_volumes), 0) AS volumes_total,
            COALESCE(SUM(cte_valor_nf), 0) AS valor_nf_total,
            COALESCE(SUM(cte_valor_frete), 0) AS valor_frete_total
        FROM entregas_clusterizadas
        WHERE tenant_id = %s
          AND envio_data = %s
          AND k_clusters = %s
    """

    params = [tenant_id, envio_data, k_clusters]
    if simulation_id:
        query += "\n          AND simulation_id = %s"
        params.append(simulation_id)

    query += "\n        GROUP BY cluster, cluster_cidade\n        ORDER BY cluster"

    df = pd.read_sql(
        query,
        db_conn,
        params=tuple(params)
    )

    if df.empty:
        return df

    # 🔹 garante tipos numéricos
    numeric_cols = [
        "qtd_entregas",
        "peso_total_kg",
        "volumes_total",
        "valor_nf_total",
        "valor_frete_total"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 🔹 formatação opcional (somente apresentação)
    if formatar_valores:

        def _formatar_moeda(x):
            return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        df["valor_nf_total"] = df["valor_nf_total"].apply(_formatar_moeda)
        df["valor_frete_total"] = df["valor_frete_total"].apply(_formatar_moeda)

    return df