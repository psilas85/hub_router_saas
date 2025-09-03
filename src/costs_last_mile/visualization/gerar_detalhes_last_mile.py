#costs_last_mile/visualization/gerar_detalhes_last_mile.py

import os
import pandas as pd
from costs_last_mile.infrastructure.cost_db_last_mile import conectar_banco

from costs_last_mile.visualization.logging_factory import get_logger

logger = get_logger(__name__)

def gerar_detalhes_last_mile(tenant_id: str, envio_data: str) -> pd.DataFrame:
    logger.info(f"🔍 Buscando detalhes por rota para tenant '{tenant_id}' e data '{envio_data}'")
    conn = conectar_banco()
    query = '''
        SELECT data_envio, cluster, sub_cluster, veiculo,
               quantidade_entregas, peso_total_kg, distancia_total_km,
               custo_entrega_total, cte_frete_total, percentual_custo
        FROM custos_rota_detalhes
        WHERE tenant_id = %s AND data_envio = %s
        ORDER BY cluster, sub_cluster, veiculo
    '''
    df = pd.read_sql(query, conn, params=(tenant_id, envio_data))
    print(df[['cte_frete_total']])
    print(df['cte_frete_total'].describe())

    conn.close()
    logger.info(f"✅ Detalhes carregados com {len(df)} rotas.")

    output_path = f"exports/costs_last_mile/detalhes/{tenant_id}"
    os.makedirs(output_path, exist_ok=True)
    csv_path = f"{output_path}/detalhes_last_mile_{envio_data}.csv"
    df.to_csv(csv_path, index=False)
    logger.info(f"💾 Detalhes por rota salvos em: {csv_path}")

    return df
