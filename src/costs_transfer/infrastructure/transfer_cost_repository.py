# costs_transfer/infrastructure/transfer_cost_repository.py

import logging
import psycopg2
import pandas as pd

from costs_transfer.infrastructure.transfer_cost_db import conectar_banco

class TransferCostRepository:
    def conectar_banco(self):
        return conectar_banco()

    def buscar_transferencias_resumo(self, data_inicial, data_final, tenant_id: str):
        query = """
            SELECT envio_data, rota_transf, cte_peso, cte_valor_frete, clusters_qde,
                hub_central_nome, distancia_total_km AS distancia_total, tipo_veiculo
            FROM public.transferencias_resumo
            WHERE envio_data BETWEEN %s AND %s AND tenant_id = %s
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

    def buscar_custos_transferencia(self, tenant_id: str):
        query = """
            SELECT tipo_veiculo, custo_por_km
            FROM public.transfer_costs
            WHERE tenant_id = %s
        """
        conexao = self.conectar_banco()
        if not conexao:
            return pd.DataFrame()
        try:
            df = pd.read_sql(query, conexao, params=(tenant_id,))
            logging.info(f"✅ {len(df)} custos carregados para tenant '{tenant_id}'.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar custos: {e}")
            return pd.DataFrame()
        finally:
            conexao.close()

    def persistir_custos_transferencia(self, custos_calculados: list, modo_forcar: bool = False):
        conexao = self.conectar_banco()
        if not conexao:
            return
        try:
            with conexao.cursor() as cursor:
                datas = set((c["tenant_id"], c["envio_data"]) for c in custos_calculados)

                for tenant_id, envio_data in datas:
                    if not modo_forcar:
                        cursor.execute("""
                            SELECT COUNT(*) FROM transfer_costs_details
                            WHERE tenant_id = %s AND envio_data = %s
                        """, (tenant_id, envio_data))
                        count = cursor.fetchone()[0]
                        if count > 0:
                            logging.warning(f"⏭️ Dados já existentes para envio_data {envio_data} (tenant: {tenant_id}). Ignorando gravação.")
                            return
                    else:
                        cursor.execute("""
                            DELETE FROM transfer_costs_details 
                            WHERE tenant_id = %s AND envio_data = %s
                        """, (tenant_id, envio_data))
                        logging.info(f"🧹 Registros antigos removidos para {envio_data} (tenant: {tenant_id})")

                insert_query = """
                    INSERT INTO transfer_costs_details (
                        tenant_id, envio_data, rota_transf, cte_peso, cte_valor_frete,
                        clusters_qde, hub_central_nome, distancia_total, tipo_veiculo,
                        custo_transferencia_total, percentual_custo, criado_em
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                """

                for c in custos_calculados:
                    cursor.execute(insert_query, (
                        c["tenant_id"], c["envio_data"], c["rota_transf"], c["cte_peso"],
                        c["cte_valor_frete"], c["clusters_qde"], c["hub_central_nome"],
                        c["distancia_total"], c["tipo_veiculo"], c["custo_transferencia_total"],
                        c["percentual_custo"]
                    ))

            conexao.commit()
            logging.info(f"✅ {len(custos_calculados)} custos de transferência salvos.")
        except Exception as e:
            logging.error(f"❌ Erro ao persistir custos: {e}")
            conexao.rollback()
        finally:
            conexao.close()

    def adicionar_custo_transferencia(self, tipo_veiculo, custo_km, capacidade_min, capacidade_max, tenant_id: str):
        conexao = self.conectar_banco()
        if not conexao:
            return
        query = """
            INSERT INTO transfer_costs (
                tipo_veiculo, custo_por_km, capacidade_kg_min, capacidade_kg_max, tenant_id, atualizado_em
            )
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (tipo_veiculo, tenant_id) DO UPDATE
            SET custo_por_km = EXCLUDED.custo_por_km,
                capacidade_kg_min = EXCLUDED.capacidade_kg_min,
                capacidade_kg_max = EXCLUDED.capacidade_kg_max,
                atualizado_em = now()
        """
        try:
            tipo_normalizado = tipo_veiculo.strip().lower()
            with conexao.cursor() as cursor:
                cursor.execute(query, (tipo_normalizado, custo_km, capacidade_min, capacidade_max, tenant_id))
            conexao.commit()
            logging.info(f"✅ Custo adicionado/atualizado para {tipo_normalizado} (tenant '{tenant_id}').")
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao adicionar custo: {e}")
            conexao.rollback()
        finally:
            conexao.close()

    def listar_custos_transferencia(self, tenant_id: str):
        conexao = self.conectar_banco()
        if not conexao:
            return pd.DataFrame()
        query = """
            SELECT tipo_veiculo, custo_por_km, capacidade_kg_min, capacidade_kg_max
            FROM transfer_costs
            WHERE tenant_id = %s
            ORDER BY capacidade_kg_min ASC
        """
        try:
            df = pd.read_sql(query, conexao, params=(tenant_id,))
            logging.info(f"✅ Custos listados para tenant '{tenant_id}'.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao listar custos: {e}")
            return pd.DataFrame()
        finally:
            conexao.close()

    def editar_custo_transferencia(self, tipo_veiculo, novo_custo_km, nova_cap_min, nova_cap_max, tenant_id: str):
        conexao = self.conectar_banco()
        if not conexao:
            return
        query = """
            UPDATE transfer_costs
            SET custo_por_km = %s,
                capacidade_kg_min = %s,
                capacidade_kg_max = %s,
                atualizado_em = now()
            WHERE TRIM(LOWER(tipo_veiculo)) = %s AND tenant_id = %s
        """
        try:
            tipo_normalizado = tipo_veiculo.strip().lower()
            with conexao.cursor() as cursor:
                cursor.execute(query, (novo_custo_km, nova_cap_min, nova_cap_max, tipo_normalizado, tenant_id))
                if cursor.rowcount == 0:
                    logging.warning(f"⚠️ Nenhum custo encontrado para '{tipo_veiculo}' (tenant: {tenant_id}).")
                else:
                    logging.info(f"✅ Custo atualizado para {tipo_veiculo} (tenant '{tenant_id}').")
            conexao.commit()
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao editar custo: {e}")
            conexao.rollback()
        finally:
            conexao.close()

    def remover_custo_transferencia(self, tipo_veiculo, tenant_id: str):
        conexao = self.conectar_banco()
        if not conexao:
            return
        query = "DELETE FROM transfer_costs WHERE TRIM(LOWER(tipo_veiculo)) = %s AND tenant_id = %s"
        try:
            tipo_normalizado = tipo_veiculo.strip().lower()
            with conexao.cursor() as cursor:
                cursor.execute(query, (tipo_normalizado, tenant_id))
                if cursor.rowcount == 0:
                    logging.warning(f"⚠️ Nenhum custo encontrado para '{tipo_veiculo}' (tenant: {tenant_id}).")
                else:
                    logging.info(f"✅ Custo removido para {tipo_veiculo} (tenant '{tenant_id}').")
            conexao.commit()
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao remover custo: {e}")
            conexao.rollback()
        finally:
            conexao.close()

