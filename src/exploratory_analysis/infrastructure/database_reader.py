#exploratory_analysis/infrastructure/database_reader.py

import pandas as pd
from .database_connection import conectar_clusterization_db

def carregar_entregas(data_inicial, data_final, tenant_id):
    conn = conectar_clusterization_db()
    query = """
        SELECT 
            envio_data,
            cte_peso,
            cte_volumes,
            cte_valor_nf,
            cte_valor_frete,
            cte_cidade AS cte_cidade,
            cte_numero,
            destinatario_nome,
            destino_latitude,
            destino_longitude,
            cte_uf,
            remetente_cidade
        FROM entregas
        WHERE envio_data BETWEEN %s AND %s
        AND tenant_id = %s

    """
    df = pd.read_sql(query, conn, params=(data_inicial, data_final, tenant_id))
    conn.close()
    return df

