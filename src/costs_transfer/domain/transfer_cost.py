import logging
import psycopg2
import pandas as pd
from datetime import datetime

from costs_transfer.infrastructure.transfer_cost_db import conectar_banco

class TransferCostRepository:
    def conectar_banco(self):
        """Estabelece conexão com o banco de dados."""
        return conectar_banco()

    def buscar_transferencias_resumo(self, data_inicial, data_final, tenant_id: str):
        query = """
            SELECT envio_data, rota_transf, cte_peso, cte_valor_frete, clusters_qde,
                hub_central_nome, distancia_total_km AS distancia_total, tipo_veiculo
            FROM public.transferencias_resumo
            WHERE envio_data BETWEEN %s AND %s
            AND tenant_id = %s
            AND rota_transf IS NOT NULL
            AND rota_transf != 'HUB'
        """
        conexao = self.conectar_banco()
        if not conexao:
            return pd.DataFrame()
        try:
            df = pd.read_sql(query, conexao, params=(data_inicial, data_final, tenant_id))
            logging.info(f"✅ {len(df)} registros de transferências carregados para tenant '{tenant_id}'.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar resumo de transferências: {e}")
            return pd.DataFrame()
        finally:
            conexao.close()


    def buscar_custos_transferencia(self):
        """Busca os custos de transferência cadastrados por tipo de veículo."""
        query = "SELECT tipo_veiculo, custo_por_km FROM public.transfer_costs"
        conexao = self.conectar_banco()
        if not conexao:
            return pd.DataFrame()

        try:
            df = pd.read_sql(query, conexao)
            logging.info(f"✅ {len(df)} registros de custos de transferência carregados.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar custos de transferência: {e}")
            return pd.DataFrame()
        finally:
            conexao.close()

    def persistir_custos_transferencia(self, custos_calculados):
        """Persiste os custos de transferência na tabela transfer_costs_details."""
        conexao = self.conectar_banco()
        if not conexao:
            return

        try:
            with conexao.cursor() as cursor:
                # 🔹 1. Remover registros antigos para envio_data antes de inserir os novos
                datas_processadas = set(custo["envio_data"] for custo in custos_calculados)
                for envio_data in datas_processadas:
                    delete_query = "DELETE FROM transfer_costs_details WHERE envio_data = %s;"
                    cursor.execute(delete_query, (envio_data,))
                    logging.info(f"🗑️ Registros antigos removidos para envio_data {envio_data}.")

                # 🔹 2. Inserir os novos registros
                insert_query = """
                INSERT INTO transfer_costs_details (envio_data, rota_transf, cte_peso, cte_valor_frete, clusters_qde,
                                    hub_central_nome, distancia_total, tipo_veiculo, custo_transferencia_total, 
                                    percentual_custo, criado_em)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now());
                """
                for custo in custos_calculados:
                    cursor.execute(insert_query, (
                        custo["envio_data"], custo["rota_transf"], custo["cte_peso"], custo["cte_valor_frete"],
                        custo["clusters_qde"], custo["hub_central_nome"], custo["distancia_total"], custo["tipo_veiculo"],
                        custo["custo_transferencia_total"], custo["percentual_custo"]
                    ))

            conexao.commit()
            logging.info(f"✅ {len(custos_calculados)} registros de custos de transferência salvos com sucesso!")

        except Exception as e:
            logging.error(f"❌ Erro ao persistir custos de transferência: {e}")

        finally:
            conexao.close()
