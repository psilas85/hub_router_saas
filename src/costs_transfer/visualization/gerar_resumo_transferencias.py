# transfer_routing/visualization/gerar_resumo_transferencias.py

import pandas as pd
import logging
from transfer_routing.infrastructure.database_connection import conectar_banco

logger = logging.getLogger(__name__)

def gerar_resumo_transferencias(tenant_id: str, envio_data: str) -> pd.DataFrame:
    try:
        conn = conectar_banco()

        query = """
            SELECT
                hub_central_nome,
                tipo_veiculo,
                COUNT(*) AS qde_rotas,
                SUM(cte_peso) AS peso_total,
                SUM(cte_valor_frete) AS frete_total,
                SUM(distancia_total) AS distancia_total_km,
                SUM(custo_transferencia_total) AS custo_total,
                AVG(percentual_custo) AS percentual_medio
            FROM transfer_costs_details
            WHERE tenant_id = %s AND envio_data = %s
            GROUP BY hub_central_nome, tipo_veiculo
            ORDER BY hub_central_nome, tipo_veiculo
        """
        df = pd.read_sql(query, conn, params=(tenant_id, envio_data))
        conn.close()

        if df.empty:
            logger.warning("⚠️ Nenhum dado encontrado para tenant_id '%s' na data '%s'", tenant_id, envio_data)
            return df

        # Totalizador final
        total = {
            'hub_central_nome': 'TOTAL',
            'tipo_veiculo': '',
            'qde_rotas': df['qde_rotas'].sum(),
            'peso_total': df['peso_total'].sum(),
            'frete_total': df['frete_total'].sum(),
            'distancia_total_km': df['distancia_total_km'].sum(),
            'custo_total': df['custo_total'].sum(),
            'percentual_medio': round(df['custo_total'].sum() / df['frete_total'].sum(), 6)
        }

        df = pd.concat([df, pd.DataFrame([total])], ignore_index=True)
        return df

    except Exception as e:
        logger.error(f"❌ Erro ao gerar resumo de transferências: {e}")
        return pd.DataFrame()
