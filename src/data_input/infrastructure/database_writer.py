#hub_router_1.0.1/src/data_input/infrastructure/database_writer.py

import os
import logging
import pandas as pd
from decimal import Decimal
from typing import List
from psycopg2.extras import execute_values
import traceback


from data_input.domain.entities import Entrega
from data_input.utils.address_normalizer import normalize_address

class DatabaseWriter:

    def __init__(self, conexao):
        self.conexao = conexao


    def inserir_dados_entregas(self, entregas):


        if self.conexao is None or not entregas:
            logging.warning("⚠ Conexão inativa ou lista de entregas vazia.")
            return

        try:

            values = []

            for e in entregas:
                values.append((
                    e.cte_numero,
                    e.transportadora,
                    e.remetente_cnpj,
                    e.cte_rua,
                    e.cte_bairro,
                    e.cte_complemento,
                    e.cte_numero_endereco,  # ← NOVO CAMPO
                    e.cte_cidade,
                    e.cte_uf,
                    e.cte_cep,
                    e.cte_nf,
                    float(e.cte_volumes) if e.cte_volumes is not None else None,
                    float(e.cte_peso) if e.cte_peso is not None else None,
                    e.cte_tempo_atendimento_min,
                    e.cte_prazo_min,
                    float(e.cte_valor_nf) if e.cte_valor_nf is not None else None,
                    float(e.cte_valor_frete) if e.cte_valor_frete is not None else None,
                    e.envio_data,
                    e.endereco_completo,
                    e.remetente_nome,
                    e.destinatario_nome,
                    e.destinatario_cnpj,
                    float(e.destino_latitude) if e.destino_latitude is not None else None,
                    float(e.destino_longitude) if e.destino_longitude is not None else None,
                    e.remetente_cidade,
                    e.remetente_uf,
                    e.doc_min,
                    e.tenant_id,
                    e.geocode_source
                ))

            # -----------------------------------------------------
            # DEBUG
            # -----------------------------------------------------
            logging.info(f"[DEBUG] total registros para UPSERT: {len(values)}")

            if values:
                logging.info(f"[DEBUG SAMPLE] {values[:3]}")

            # -----------------------------------------------------
            # QUERY (SEM RETURNING!)
            # -----------------------------------------------------
            query = """
            INSERT INTO entregas (
                cte_numero,
                transportadora,
                remetente_cnpj,
                cte_rua,
                cte_bairro,
                cte_complemento,
                cte_numero_endereco,  -- ← NOVO CAMPO
                cte_cidade,
                cte_uf,
                cte_cep,
                cte_nf,
                cte_volumes,
                cte_peso,
                cte_tempo_atendimento_min,
                cte_prazo_min,
                cte_valor_nf,
                cte_valor_frete,
                envio_data,
                endereco_completo,
                remetente_nome,
                destinatario_nome,
                destinatario_cnpj,
                destino_latitude,
                destino_longitude,
                remetente_cidade,
                remetente_uf,
                doc_min,
                tenant_id,
                geocode_source
            )
            VALUES %s
            ON CONFLICT (tenant_id, cte_numero, transportadora)
            DO UPDATE SET
                remetente_cnpj = EXCLUDED.remetente_cnpj,
                cte_rua = EXCLUDED.cte_rua,
                cte_bairro = EXCLUDED.cte_bairro,
                cte_complemento = EXCLUDED.cte_complemento,
                cte_numero_endereco = EXCLUDED.cte_numero_endereco,  -- ← NOVO CAMPO
                cte_cidade = EXCLUDED.cte_cidade,
                cte_uf = EXCLUDED.cte_uf,
                cte_cep = EXCLUDED.cte_cep,
                cte_nf = EXCLUDED.cte_nf,
                cte_volumes = EXCLUDED.cte_volumes,
                cte_peso = EXCLUDED.cte_peso,
                cte_tempo_atendimento_min = EXCLUDED.cte_tempo_atendimento_min,
                cte_prazo_min = EXCLUDED.cte_prazo_min,
                cte_valor_nf = EXCLUDED.cte_valor_nf,
                cte_valor_frete = EXCLUDED.cte_valor_frete,
                envio_data = COALESCE(EXCLUDED.envio_data, entregas.envio_data),
                endereco_completo = EXCLUDED.endereco_completo,
                remetente_nome = EXCLUDED.remetente_nome,
                destinatario_nome = EXCLUDED.destinatario_nome,
                destinatario_cnpj = EXCLUDED.destinatario_cnpj,
                destino_latitude = EXCLUDED.destino_latitude,
                destino_longitude = EXCLUDED.destino_longitude,
                remetente_cidade = EXCLUDED.remetente_cidade,
                remetente_uf = EXCLUDED.remetente_uf,
                doc_min = EXCLUDED.doc_min,
                tenant_id = EXCLUDED.tenant_id,
                geocode_source = EXCLUDED.geocode_source
            """

            with self.conexao.cursor() as cursor:
                execute_values(cursor, query, values)

            logging.info(f"📊 UPSERT entregas concluído | total_processado={len(values)}")

        except Exception:
            logging.error(f"❌ Erro ao inserir dados:\n{traceback.format_exc()}")
            raise

    def inserir_localizacao(self, endereco, latitude, longitude, origem=None):

        if not endereco:
            return

        endereco_norm = normalize_address(endereco)

        if not endereco_norm:
            return

        if len(endereco_norm) < 10:
            return

        if endereco_norm in ["NAN", "NONE", "-", ""]:
            return

        if endereco_norm.replace(" ", "").isdigit():
            return

        if latitude is None or longitude is None:
            return

        query = """
        INSERT INTO localizacoes (endereco, latitude, longitude, origem, criado_em, atualizado_em)
        VALUES (%s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (endereco)
        DO UPDATE SET
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            origem = COALESCE(EXCLUDED.origem, localizacoes.origem),
            atualizado_em = NOW()
        RETURNING xmax = 0 AS inserted;
        """

        try:
            with self.conexao.cursor() as cursor:

                cursor.execute(
                    query,
                    (endereco_norm, latitude, longitude, origem)
                )

                inserted = cursor.fetchone()[0]

            if inserted:
                logging.info(f"[CACHE INSERT] {endereco_norm}")
            else:
                logging.info(f"[CACHE UPDATE] {endereco_norm}")

        except Exception as e:
            logging.error(f"❌ Erro ao inserir localização: {e}")

    def salvar_clusterizacao(self, clustered_data):
        if self.conexao is None:
            logging.error("❌ Conexão com o banco não está ativa!")
            return

        if clustered_data.empty:
            logging.warning("⚠ Nenhum dado para salvar em 'entregas_clusterizadas'.")
            return

        clustered_data["envio_data"] = pd.to_datetime(clustered_data["envio_data"], errors="coerce").dt.date

        insert_query = """
            INSERT INTO entregas_clusterizadas (
                id_entrega, cte_numero, transportadora, envio_data,
                cluster, cluster_cidade, centro_lat, centro_lon, tenant_id, data_hora
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cte_numero, transportadora) DO UPDATE SET
                cluster = EXCLUDED.cluster,
                cluster_cidade = EXCLUDED.cluster_cidade,
                centro_lat = EXCLUDED.centro_lat,
                centro_lon = EXCLUDED.centro_lon,
                tenant_id = EXCLUDED.tenant_id,
                envio_data = COALESCE(entregas_clusterizadas.envio_data, EXCLUDED.envio_data),
                data_hora = NOW();
        """

        registros_novos = clustered_data[
            [
                "id_entrega", "cte_numero", "transportadora", "envio_data",
                "cluster", "cluster_cidade", "centro_lat", "centro_lon", "tenant_id"
            ]
        ].astype(object).where(pd.notna(clustered_data), None).to_records(index=False).tolist()

        try:
            with self.conexao.cursor() as cursor:
                cursor.executemany(insert_query, registros_novos)

            logging.info(f"✅ {len(registros_novos)} registros de clusterização salvos/atualizados.")
        except Exception as e:

            logging.error(f"❌ Erro ao salvar clusterização: {e}")

    def salvar_resumo_clusters(self, df_resumo: pd.DataFrame, envio_data: str, tenant_id: str):
        """
        Salva o resumo da clusterização no banco de dados.
        Espera um DataFrame com colunas:
            ['cluster', 'quantidade_entregas', 'peso_total_kg',
            'distancia_media_km', 'tempo_estimado_min',
            'quantidade_volumes', 'centro_lat', 'centro_lon']
        """

        if self.conexao is None or df_resumo.empty:
            logging.warning("⚠ Conexão inativa ou DataFrame de resumo vazio.")
            return

        df_resumo["envio_data"] = pd.to_datetime(envio_data).date()
        df_resumo["tenant_id"] = tenant_id

        registros = df_resumo[[
            "envio_data", "tenant_id", "cluster", "quantidade_entregas", "peso_total_kg",
            "distancia_media_km", "tempo_estimado_min", "quantidade_volumes", "centro_lat", "centro_lon"
        ]].astype(object).where(pd.notna(df_resumo), None).to_records(index=False).tolist()

        query = """
            INSERT INTO resumo_clusterizacao (
                envio_data, tenant_id, cluster, quantidade_entregas, peso_total_kg,
                distancia_media_km, tempo_estimado_min, quantidade_volumes,
                centro_lat, centro_lon
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (envio_data, tenant_id, cluster)
            DO UPDATE SET
                quantidade_entregas = EXCLUDED.quantidade_entregas,
                peso_total_kg = EXCLUDED.peso_total_kg,
                distancia_media_km = EXCLUDED.distancia_media_km,
                tempo_estimado_min = EXCLUDED.tempo_estimado_min,
                quantidade_volumes = EXCLUDED.quantidade_volumes,
                centro_lat = EXCLUDED.centro_lat,
                centro_lon = EXCLUDED.centro_lon;
        """

        try:
            with self.conexao.cursor() as cursor:
                cursor.executemany(query, registros)

            logging.info(f"✅ {len(registros)} registros salvos/atualizados em 'resumo_clusterizacao'.")
        except Exception as e:

            logging.error(f"❌ Erro ao salvar resumo da clusterização: {e}")

    def salvar_historico_data_input(
        self, tenant_id: str, job_id: str, arquivo: str, status: str,
        total_processados: int, validos: int, invalidos: int, mensagem: str,
        tipo_processamento: str = "padrao"
    ):
        query = """
            INSERT INTO historico_data_input
                (
                    tenant_id, job_id, arquivo, status, total_processados,
                    validos, invalidos, mensagem, tipo_processamento, criado_em
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(query, (
                    tenant_id, job_id, arquivo, status,
                    total_processados, validos, invalidos, mensagem,
                    tipo_processamento,
                ))

            logging.info(f"📝 Histórico salvo em historico_data_input: job_id={job_id}")
        except Exception as e:

            logging.error(f"❌ Erro ao salvar histórico do data input: {e}")


    def atualizar_data_processamento_lote(self, entregas):

        ctes = [e.cte_numero for e in entregas if e.cte_numero]

        if not ctes:
            return

        query = """
        UPDATE entregas
        SET data_processamento = NOW()
        WHERE cte_numero = ANY(%s)
        """

        with self.conexao.cursor() as cursor:
            cursor.execute(query, (ctes,))

