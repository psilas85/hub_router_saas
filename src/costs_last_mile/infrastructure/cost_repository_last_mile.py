## costs_last_mile/infrastructure/cost_repository_last_mile.py

import logging
import psycopg2
import pandas as pd
from datetime import datetime

from costs_last_mile.infrastructure.cost_db_last_mile import conectar_banco

class CostRepository:
    def conectar_banco(self):
        """Estabelece conexão com o banco de dados."""
        return conectar_banco()

    def buscar_resumo_rotas(self, data_inicial, data_final, tenant_id: str):
        query = """
            SELECT 
                envio_data AS data_envio,
                cluster,
                sub_cluster,
                peso_total_kg,
                distancia_total_km,
                veiculo,
                SUM(
                    CASE 
                        WHEN entrega->>'cte_valor_frete_total' IS NOT NULL AND entrega->>'cte_valor_frete_total' <> ''
                        THEN (entrega->>'cte_valor_frete_total')::float
                        ELSE 0
                    END
                ) AS cte_frete_total,
                COUNT(*) AS quantidade_entregas
            FROM public.last_mile_rotas,
                LATERAL jsonb_array_elements(entregas) AS entrega
            WHERE envio_data BETWEEN %s AND %s
            AND tenant_id = %s
            GROUP BY envio_data, cluster, sub_cluster, peso_total_kg, distancia_total_km, veiculo
        """
        conexao = self.conectar_banco()
        if not conexao:
            return pd.DataFrame()
        try:
            df = pd.read_sql(query, conexao, params=(data_inicial, data_final, tenant_id))
            logging.info(f"✅ {len(df)} registros de rotas carregados para tenant '{tenant_id}'.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar resumo de rotas: {e}")
            return pd.DataFrame()
        finally:
            conexao.close()


    def buscar_custos_veiculo(self, tenant_id: str):
        query = """
            SELECT veiculo, custo_por_km, custo_por_entrega, 
                peso_minimo_kg,
                peso_maximo_kg
            FROM public.custos_entrega
            WHERE tenant_id = %s
            ORDER BY peso_minimo_kg
        """
        conexao = self.conectar_banco()
        if not conexao:
            return pd.DataFrame()
        try:
            df = pd.read_sql(query, conexao, params=(tenant_id,))
            logging.info(f"✅ {len(df)} registros de custos carregados para tenant '{tenant_id}'.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar custos de veículos: {e}")
            return pd.DataFrame()
        finally:
            conexao.close()

    def persistir_custos_rota_detalhes(self, custos_calculados, modo_forcar: bool = False):
        conexao = self.conectar_banco()
        if not conexao:
            return

        try:
            with conexao.cursor() as cursor:
                datas = set((c["tenant_id"], c["data_envio"]) for c in custos_calculados)

                for tenant_id, data_envio in datas:
                    if modo_forcar:
                        cursor.execute(
                            "DELETE FROM custos_rota_detalhes WHERE tenant_id = %s AND data_envio = %s",
                            (tenant_id, data_envio)
                        )
                        logging.info(f"🧹 Registros antigos removidos para {data_envio} (tenant: {tenant_id})")
                    else:
                        cursor.execute(
                            "SELECT COUNT(*) FROM custos_rota_detalhes WHERE tenant_id = %s AND data_envio = %s",
                            (tenant_id, data_envio)
                        )
                        if cursor.fetchone()[0] > 0:
                            logging.warning(f"⚠️ Já existem registros para {data_envio} (tenant: {tenant_id}). Pulei devido ao modo_forcar=False.")
                            continue

                insert_query = """
                    INSERT INTO custos_rota_detalhes (
                        tenant_id, data_envio, cluster, sub_cluster, quantidade_entregas, 
                        peso_total_kg, distancia_total_km, cte_frete_total, veiculo, 
                        custo_entrega_total, percentual_custo, criado_em
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now());
                """

                for c in custos_calculados:
                    cursor.execute(insert_query, (
                        c["tenant_id"], c["data_envio"], c["cluster"], c["sub_cluster"],
                        c["quantidade_entregas"], c["peso_total_kg"], c["distancia_total_km"],
                        c["cte_frete_total"], c["veiculo"], c["custo_entrega_total"],
                        c["percentual_custo"]
                    ))

            conexao.commit()
            logging.info(f"✅ {len(custos_calculados)} registros inseridos com sucesso.")
        except Exception as e:
            logging.error(f"❌ Erro ao persistir custos: {e}")
            conexao.rollback()
        finally:
            conexao.close()


    def adicionar_custo_veiculo(self, veiculo, custo_km, custo_entrega, peso_minimo_kg, peso_maximo_kg, tenant_id):
        conexao = conectar_banco()
        if not conexao:
            logging.error("❌ Falha ao conectar no banco de dados.")
            return

        query = """
        INSERT INTO custos_entrega (veiculo, custo_por_km, custo_por_entrega, 
                                    peso_minimo_kg, peso_maximo_kg, tenant_id, atualizado_em)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (veiculo, tenant_id) DO UPDATE 
        SET custo_por_km = EXCLUDED.custo_por_km, 
            custo_por_entrega = EXCLUDED.custo_por_entrega,
            peso_minimo_kg = EXCLUDED.peso_minimo_kg,
            peso_maximo_kg = EXCLUDED.peso_maximo_kg,
            atualizado_em = now();
        """

        try:
            with conexao.cursor() as cursor:
                logging.info(f"📤 Executando SQL:\n{query}")
                logging.info(f"📦 Parâmetros: ({veiculo}, {custo_km}, {custo_entrega}, {peso_minimo_kg}, {peso_maximo_kg}, {tenant_id})")
                veiculo_normalizado = veiculo.strip().lower()
                cursor.execute(query, (
                    veiculo_normalizado,
                    custo_km,
                    custo_entrega,
                    peso_minimo_kg,
                    peso_maximo_kg,
                    tenant_id
                ))



            conexao.commit()
            logging.info(f"✅ Custo do veículo '{veiculo}' adicionado/atualizado com sucesso para tenant '{tenant_id}'.")
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao adicionar custo do veículo {veiculo}: {e}")
            conexao.rollback()
        finally:
            conexao.close()


    def editar_custo_veiculo(self, veiculo, novo_custo_km, novo_custo_entrega, 
                         novo_peso_min, novo_peso_max, tenant_id):
        conexao = conectar_banco()
        if not conexao:
            return

        query = """
        UPDATE custos_entrega
        SET custo_por_km = %s,
            custo_por_entrega = %s,
            peso_minimo_kg = %s,
            peso_maximo_kg = %s,
            atualizado_em = now()
        WHERE TRIM(LOWER(veiculo)) = %s AND tenant_id = %s;
        """

        try:
            veiculo_normalizado = veiculo.strip().lower()
            with conexao.cursor() as cursor:
                cursor.execute(query, (
                    novo_custo_km, novo_custo_entrega, novo_peso_min, 
                    novo_peso_max, veiculo_normalizado, tenant_id
                ))
            conexao.commit()
            logging.info(f"✅ Custo do veículo '{veiculo}' atualizado para tenant '{tenant_id}'.")
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao atualizar custo do veículo {veiculo}: {e}")
            conexao.rollback()
        finally:
            conexao.close()


    def remover_custo_veiculo(self, veiculo, tenant_id):
        """Remove o custo de um veículo específico para um tenant."""
        conexao = conectar_banco()
        if not conexao:
            return

        query = """
            DELETE FROM custos_entrega 
            WHERE TRIM(LOWER(veiculo)) = %s AND tenant_id = %s;
        """

        try:
            veiculo_normalizado = veiculo.strip().lower()
            with conexao.cursor() as cursor:
                cursor.execute(query, (veiculo_normalizado, tenant_id))
                if cursor.rowcount == 0:
                    logging.warning(f"⚠️ Nenhum custo encontrado para '{veiculo}' (tenant: {tenant_id}).")
                else:
                    logging.info(f"✅ Custo do veículo '{veiculo}' removido com sucesso para tenant '{tenant_id}'.")
            conexao.commit()
        except psycopg2.Error as e:
            logging.error(f"❌ Erro ao remover custo do veículo {veiculo}: {e}")
            conexao.rollback()
        finally:
            conexao.close()

    
    def deletar_dados_existentes(self, data_envio, tenant_id):
        conexao = self.conectar_banco()
        if not conexao:
            return

        try:
            with conexao.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM custos_rota_detalhes
                    WHERE data_envio = %s AND tenant_id = %s
                """, (data_envio, tenant_id))
            conexao.commit()
            logging.info(f"🧹 Registros antigos removidos para {data_envio} (tenant: {tenant_id})")
        except Exception as e:
            logging.error(f"❌ Erro ao remover registros antigos: {e}")
            conexao.rollback()
        finally:
            conexao.close()
