import pandas as pd

import pandas as pd

import pandas as pd

def validar_integridade_entregas_clusterizadas(
    db_conn,
    tenant_id: str,
    envio_data: str,
    simulation_id: str,
    k_clusters: int,
    df_novo: pd.DataFrame,
    logger
):
    """
    Valida se há duplicidade de entregas (cte_numero) para o mesmo envio_data,
    simulation_id e k_clusters.

    Parâmetros:
        db_conn: conexão ativa com o banco de dados simulation_db
        tenant_id (str): identificador do tenant
        envio_data (str): data do envio sendo processada
        simulation_id (str): id da simulação atual
        k_clusters (int): valor atual de k da simulação
        df_novo (pd.DataFrame): DataFrame que será persistido
        logger: logger para registrar mensagens
    """
    cursor = db_conn.cursor()

    query = """
        SELECT simulation_id, envio_data, cte_numero, k_clusters
        FROM entregas_clusterizadas
        WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
    """
    cursor.execute(query, (tenant_id, envio_data, k_clusters))
    registros = cursor.fetchall()
    cursor.close()

    if not registros:
        logger.info("✅ Nenhuma entrega anterior encontrada para essa data e k_clusters.")
        return

    df_antigo = pd.DataFrame(registros, columns=["simulation_id", "envio_data", "cte_numero", "k_clusters"])

    if 'k_clusters' not in df_novo.columns:
        logger.warning("⚠️ df_novo não contém coluna 'k_clusters'. Validação pode ser imprecisa.")
        df_novo['k_clusters'] = k_clusters

    df_novo = df_novo[["simulation_id", "envio_data", "cte_numero", "k_clusters"]]

    df_combinado = pd.concat([df_antigo, df_novo], ignore_index=True)
    duplicados = df_combinado[df_combinado.duplicated(subset=["cte_numero", "simulation_id", "envio_data", "k_clusters"], keep=False)]

    if not duplicados.empty:
        logger.warning("⚠️ CTEs duplicados detectados (mesmo simulation_id, envio_data e k_clusters):")
        logger.warning(f"\n{duplicados.sort_values(['cte_numero'])}")
    else:
        logger.info("✅ Nenhum CTE duplicado detectado para o mesmo simulation_id, envio_data e k_clusters.")


