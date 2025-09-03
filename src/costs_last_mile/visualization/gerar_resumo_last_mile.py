import os
import pandas as pd
from costs_last_mile.infrastructure.cost_db_last_mile import conectar_banco
from costs_last_mile.visualization.logging_factory import get_logger

logger = get_logger(__name__)

def gerar_resumo_last_mile(tenant_id: str, envio_data: str) -> pd.DataFrame:
    logger.info(f"🔍 Buscando resumo por veículo para tenant '{tenant_id}' e data '{envio_data}'")
    conn = conectar_banco()
    query = '''
        SELECT veiculo, data_envio, SUM(quantidade_entregas) AS quantidade_entregas,
               SUM(peso_total_kg) AS peso_total_kg, SUM(distancia_total_km) AS distancia_total_km,
               SUM(custo_entrega_total) AS custo_entrega_total, SUM(cte_frete_total) AS cte_frete_total,
               ROUND(SUM(custo_entrega_total) * 100.0 / NULLIF(SUM(cte_frete_total), 0), 2) AS percentual_custo
        FROM custos_rota_detalhes
        WHERE tenant_id = %s AND data_envio = %s
        GROUP BY veiculo, data_envio
        ORDER BY veiculo
    '''
    df = pd.read_sql(query, conn, params=(tenant_id, envio_data))
    conn.close()
    logger.info(f"✅ Resumo carregado com {len(df)} linhas.")

    output_path = f"exports/costs_last_mile/resumos/{tenant_id}"
    os.makedirs(output_path, exist_ok=True)
    csv_path = f"{output_path}/resumo_last_mile_{envio_data}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"💾 Resumo salvo em: {csv_path}")

    return df
