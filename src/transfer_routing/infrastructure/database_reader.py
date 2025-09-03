# transfer_routing/infrastructure/database_reader.py

import pandas as pd


def buscar_hub_central(tenant_id: str, conn):
    """
    Busca o hub central cadastrado para o tenant.
    """
    query = """
        SELECT hub_central_nome, hub_central_latitude, hub_central_longitude
        FROM hubs_central
        WHERE tenant_id = %s
          AND ativo = true
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(query, (tenant_id,))
        row = cur.fetchone()

    if row:
        return {
            "nome": row[0],
            "latitude": row[1],
            "longitude": row[2]
        }
    else:
        return None


def carregar_entregas_completas(tenant_id: str, envio_data, conn):
    """
    Carrega entregas clusterizadas e seus dados detalhados.
    Faz join com a tabela de entregas para pegar peso, volumes, valores, etc.
    """
    query = """
        SELECT
            ec.cluster, ec.centro_lat, ec.centro_lon,
            ec.cte_numero, ec.transportadora,
            e.cte_peso, e.cte_volumes, e.cte_valor_nf, e.cte_valor_frete
        FROM entregas_clusterizadas ec
        INNER JOIN entregas e
            ON ec.cte_numero = e.cte_numero
            AND ec.transportadora = e.transportadora
        WHERE ec.tenant_id = %s
          AND ec.envio_data = %s
    """
    df = pd.read_sql(query, conn, params=(tenant_id, envio_data))
    return df
