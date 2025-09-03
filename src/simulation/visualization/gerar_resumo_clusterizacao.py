#simulation/visualization/gerar_resumo_clusterizacao.py

import pandas as pd

def gerar_resumo_clusterizacao(db_conn, tenant_id: str, envio_data: str, k_clusters: int) -> pd.DataFrame:
    """
    Gera um resumo por cluster para o cenário (tenant_id, envio_data, k_clusters),
    contendo cidade, quantidade de entregas, peso total, volumes, valor NF e valor do frete.
    """
    query = """
        SELECT
            cluster,
            cluster_cidade,
            COUNT(*) AS qtd_entregas,
            SUM(cte_peso) AS peso_total_kg,
            SUM(cte_volumes) AS volumes_total,
            SUM(cte_valor_nf) AS valor_nf_total,
            SUM(cte_valor_frete) AS valor_frete_total
        FROM entregas_clusterizadas
        WHERE tenant_id = %s
          AND envio_data = %s
          AND k_clusters = %s
        GROUP BY cluster, cluster_cidade
        ORDER BY cluster
    """

    params = (tenant_id, envio_data, k_clusters)
    df = pd.read_sql(query, db_conn, params=params)

    # Formatação opcional dos valores
    df["valor_nf_total"] = df["valor_nf_total"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    df["valor_frete_total"] = df["valor_frete_total"].apply(lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

    return df
