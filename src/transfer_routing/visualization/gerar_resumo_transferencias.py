import pandas as pd


def gerar_resumos_transferencias(conn, tenant_id: str, envio_data: str):
    query = """
        SELECT *
        FROM transferencias_resumo
        WHERE tenant_id = %s
          AND envio_data = %s
    """
    df_resumo = pd.read_sql(query, conn, params=(tenant_id, envio_data))

    query_detalhes = """
        SELECT *
        FROM transferencias_detalhes
        WHERE tenant_id = %s
          AND envio_data = %s
    """
    df_detalhes = pd.read_sql(query_detalhes, conn, params=(tenant_id, envio_data))

    return df_resumo, df_detalhes
