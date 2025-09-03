#clusterization/infrastructure/database_writer.py

import os
import logging
import pandas as pd
from decimal import Decimal
from typing import List

from clusterization.domain.entities import Entrega

class DatabaseWriter:
    def __init__(self, conexao):
        self.conexao = conexao

    def inserir_dados_entregas(self, entregas: List[Entrega]):
        if self.conexao is None or not entregas:
            logging.warning("⚠ Conexão inativa ou lista de entregas vazia.")
            return

        values = [
            (
                e.cte_numero, e.remetente_cnpj, e.cte_rua, e.cte_bairro, e.cte_complemento, e.cte_cidade,
                e.cte_uf, e.cte_cep, e.cte_nf, e.cte_volumes,
                Decimal(str(e.cte_peso)) if e.cte_peso is not None else None,
                Decimal(str(e.cte_valor_nf)) if e.cte_valor_nf is not None else None,
                Decimal(str(e.cte_valor_frete)) if e.cte_valor_frete is not None else None,
                e.envio_data, e.endereco_completo, e.transportadora, e.remetente_nome,
                e.destinatario_nome, e.destinatario_cnpj, e.destino_latitude,
                e.destino_longitude, e.remetente_cidade, e.remetente_uf, e.doc_min,
                e.tenant_id  # ✅ novo campo
            ) for e in entregas
        ]

        query = """
        INSERT INTO entregas (
            cte_numero, remetente_cnpj, cte_rua, cte_bairro, cte_complemento, cte_cidade, cte_uf,
            cte_cep, cte_nf, cte_volumes, cte_peso, cte_valor_nf, cte_valor_frete, envio_data,
            endereco_completo, transportadora, remetente_nome, destinatario_nome, destinatario_cnpj,
            destino_latitude, destino_longitude, remetente_cidade, remetente_uf, doc_min,
            tenant_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cte_numero, transportadora) DO UPDATE SET
            remetente_cnpj = EXCLUDED.remetente_cnpj,
            cte_rua = EXCLUDED.cte_rua,
            cte_bairro = EXCLUDED.cte_bairro,
            cte_complemento = EXCLUDED.cte_complemento,
            cte_cidade = EXCLUDED.cte_cidade,
            cte_uf = EXCLUDED.cte_uf,
            cte_cep = EXCLUDED.cte_cep,
            cte_nf = EXCLUDED.cte_nf,
            cte_volumes = EXCLUDED.cte_volumes,
            cte_peso = EXCLUDED.cte_peso,
            cte_valor_nf = EXCLUDED.cte_valor_nf,
            cte_valor_frete = EXCLUDED.cte_valor_frete,
            envio_data = COALESCE(EXCLUDED.envio_data, entregas.envio_data),
            endereco_completo = EXCLUDED.endereco_completo,
            transportadora = EXCLUDED.transportadora,
            remetente_nome = EXCLUDED.remetente_nome,
            destinatario_nome = EXCLUDED.destinatario_nome,
            destinatario_cnpj = EXCLUDED.destinatario_cnpj,
            destino_latitude = EXCLUDED.destino_latitude,
            destino_longitude = EXCLUDED.destino_longitude,
            remetente_cidade = EXCLUDED.remetente_cidade,
            remetente_uf = EXCLUDED.remetente_uf,
            doc_min = EXCLUDED.doc_min,
            tenant_id = EXCLUDED.tenant_id  -- ✅ garante update do tenant também
        ;
        """

        try:
            with self.conexao.cursor() as cursor:
                cursor.executemany(query, values)
            self.conexao.commit()
            logging.info(f"✅ {len(entregas)} registros inseridos/atualizados na tabela 'entregas'.")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao inserir dados no banco: {e}")
            raise


    def inserir_localizacao(self, endereco, latitude, longitude):
        query = """
        INSERT INTO localizacoes (endereco, latitude, longitude, criado_em)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (endereco) DO NOTHING;
        """
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(query, (endereco, latitude, longitude))
            self.conexao.commit()
            logging.info(f"✅ Coordenadas salvas: {endereco} → ({latitude}, {longitude})")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao inserir localização: {e}")

    def atualizar_data_processamento(self, entregas: List[Entrega]):
        if self.conexao is None or not entregas:
            logging.warning("⚠ Conexão inativa ou entregas vazias para atualizar.")
            return

        try:
            with self.conexao.cursor() as cursor:
                for entrega in entregas:
                    cursor.execute(
                        """
                        UPDATE entregas
                        SET data_processamento = NOW()
                        WHERE cte_numero = %s;
                        """, (entrega.cte_numero,)
                    )
            self.conexao.commit()
            logging.info(f"✅ Data de processamento atualizada para {len(entregas)} CT-es.")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao atualizar data_processamento: {e}")


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
                cluster, cluster_cidade, centro_lat, centro_lon, 
                destino_latitude, destino_longitude,
                tenant_id, data_hora
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cte_numero, transportadora) DO UPDATE SET 
                cluster = EXCLUDED.cluster,
                cluster_cidade = EXCLUDED.cluster_cidade,
                centro_lat = EXCLUDED.centro_lat,
                centro_lon = EXCLUDED.centro_lon,
                destino_latitude = EXCLUDED.destino_latitude,
                destino_longitude = EXCLUDED.destino_longitude,
                tenant_id = EXCLUDED.tenant_id,
                envio_data = COALESCE(entregas_clusterizadas.envio_data, EXCLUDED.envio_data),
                data_hora = NOW();
        """

        registros_novos = clustered_data[
            [
                "id_entrega", "cte_numero", "transportadora", "envio_data",
                "cluster", "cluster_cidade", "centro_lat", "centro_lon",
                "destino_latitude", "destino_longitude",
                "tenant_id"
            ]
        ].astype(object).where(pd.notna(clustered_data), None).to_records(index=False).tolist()

        try:
            with self.conexao.cursor() as cursor:
                cursor.executemany(insert_query, registros_novos)
            self.conexao.commit()
            logging.info(f"✅ {len(registros_novos)} registros de clusterização salvos/atualizados.")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao salvar clusterização: {e}")


    def salvar_resumo_clusters(self, df_resumo: pd.DataFrame, envio_data: str, tenant_id: str):
        """
        Salva o resumo da clusterização no banco de dados.
        Espera um DataFrame com colunas:
            ['cluster', 'quantidade_entregas', 'peso_total_kg',
            'distancia_media_km', 'tempo_estimado_min',
            'quantidade_volumes', 'centro_lat', 'centro_lon',
            'cte_valor_nf_total', 'cte_valor_frete_total']
        """

        if self.conexao is None or df_resumo.empty:
            logging.warning("⚠ Conexão inativa ou DataFrame de resumo vazio.")
            return

        try:
            df_resumo["envio_data"] = pd.to_datetime(envio_data).date()
            df_resumo["tenant_id"] = tenant_id

            # 🧼 Preencher NaNs com zero para campos numéricos obrigatórios
            campos_numericos = [
                "quantidade_entregas", "peso_total_kg", "distancia_media_km",
                "tempo_estimado_min", "quantidade_volumes",
                "cte_valor_nf_total", "cte_valor_frete_total"
            ]
            for col in campos_numericos:
                if col in df_resumo.columns:
                    df_resumo[col] = df_resumo[col].fillna(0)

            logging.info("📋 Colunas disponíveis em df_resumo:")
            logging.info(df_resumo.columns.tolist())

            logging.info("📊 Primeiras linhas de df_resumo antes da gravação:")
            logging.info(df_resumo.head().to_string(index=False))




            registros = df_resumo[[
                "envio_data", "tenant_id", "cluster", "quantidade_entregas", "peso_total_kg",
                "distancia_media_km", "tempo_estimado_min", "quantidade_volumes",
                "cte_valor_nf_total", "cte_valor_frete_total",  # ⬅ ADICIONAR AQUI
                "centro_lat", "centro_lon"
            ]].astype(object).where(pd.notna(df_resumo), None).to_records(index=False).tolist()


            query = """
                INSERT INTO resumo_clusterizacao (
                    envio_data, tenant_id, cluster, quantidade_entregas, peso_total_kg,
                    distancia_media_km, tempo_estimado_min, quantidade_volumes,
                    cte_valor_nf_total, cte_valor_frete_total,  -- ⬅ ADICIONAR AQUI
                    centro_lat, centro_lon
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (envio_data, tenant_id, cluster)
                DO UPDATE SET 
                    quantidade_entregas = EXCLUDED.quantidade_entregas,
                    peso_total_kg = EXCLUDED.peso_total_kg,
                    distancia_media_km = EXCLUDED.distancia_media_km,
                    tempo_estimado_min = EXCLUDED.tempo_estimado_min,
                    quantidade_volumes = EXCLUDED.quantidade_volumes,
                    cte_valor_nf_total = EXCLUDED.cte_valor_nf_total,
                    cte_valor_frete_total = EXCLUDED.cte_valor_frete_total,
                    centro_lat = EXCLUDED.centro_lat,
                    centro_lon = EXCLUDED.centro_lon;

            """

            with self.conexao.cursor() as cursor:
                cursor.executemany(query, registros)
            self.conexao.commit()
            logging.info(f"✅ {len(registros)} registros salvos/atualizados em 'resumo_clusterizacao'.")

        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao salvar resumo da clusterização: {e}")

    def existe_resumo_clusterizacao(self, envio_data, tenant_id) -> bool:
        query = """
        SELECT EXISTS(
            SELECT 1 FROM resumo_clusterizacao
            WHERE envio_data = %s AND tenant_id = %s
        );
        """
        with self.conexao.cursor() as cursor:
            cursor.execute(query, (envio_data, tenant_id))
            result = cursor.fetchone()
            return result[0] if result else False


    def excluir_clusterizacao_por_data_tenant(self, envio_data, tenant_id):
        queries = [
            "DELETE FROM resumo_clusterizacao WHERE envio_data = %s AND tenant_id = %s;",
            "DELETE FROM entregas_clusterizadas WHERE envio_data = %s AND tenant_id = %s;",
            # Adicione outras tabelas relacionadas aqui, se houver
        ]
        try:
            with self.conexao.cursor() as cursor:
                for q in queries:
                    cursor.execute(q, (envio_data, tenant_id))
            self.conexao.commit()
        except Exception as e:
            self.conexao.rollback()
            raise e
