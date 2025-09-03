# data_input/infrastructure/database_reader.py

import os
import logging
import pandas as pd
from typing import List, Optional


class DatabaseReader:
    def __init__(self, conexao):
        self.conexao = conexao

    def buscar_dados_clusterizacao(self, data_inicial, data_final):
        query = """
        SELECT * FROM clusterizacao_formatada
        WHERE envio_data BETWEEN %s AND %s;
        """
        try:
            df = pd.read_sql(query, self.conexao, params=[data_inicial, data_final])
            logging.info(f"✅ Dados de clusterização recuperados: {len(df)} registros.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar dados de clusterização: {e}")
            raise

    def buscar_localizacao(self, endereco):
        query = "SELECT latitude, longitude FROM localizacoes WHERE endereco = %s"
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(query, (endereco,))
                return cursor.fetchone()
        except Exception as e:
            logging.error(f"❌ Erro ao buscar localização no banco: {e}")
            return None

    def buscar_entregas(self, datas: list, tenant_id: str):
        """
        Busca as entregas para uma lista de datas (envio_data) e um tenant_id.
        """
        if not datas:
            logging.warning("⚠ Nenhuma data informada para consulta de entregas.")
            return pd.DataFrame()

        query = """
            SELECT
                id AS id_entrega, cte_numero, transportadora, envio_data,
                cte_cidade, cte_uf, cte_cep, cte_volumes,
                cte_peso, cte_valor_nf, cte_valor_frete, destino_latitude, destino_longitude,
                tenant_id
            FROM public.entregas
            WHERE envio_data = ANY(%s)
            AND tenant_id = %s
        """
        try:
            df = pd.read_sql(query, self.conexao, params=(datas, tenant_id))
            logging.info(f"✅ {len(df)} registros de entregas recuperados para tenant '{tenant_id}'.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar entregas: {e}")
            return pd.DataFrame()



    def buscar_centro_urbano(self, lat, lon):
        if self.conexao is None:
            logging.error("❌ Erro: conexão com o banco não está ativa!")
            return None, None

        query = """
        SELECT endereco, latitude, longitude 
        FROM localizacoes
        ORDER BY ST_Distance(
            ST_SetSRID(ST_MakePoint(longitude::double precision, latitude::double precision), 4326),
            ST_SetSRID(ST_MakePoint(%s, %s), 4326)
        )
        LIMIT 1;
        """
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(query, (float(lon), float(lat)))
                resultado = cursor.fetchone()
                if resultado:
                    endereco, latitude, longitude = resultado
                    logging.info(f"📍 Centro urbano encontrado: {endereco} ({latitude}, {longitude})")
                    return endereco, (latitude, longitude)
                else:
                    logging.warning(f"⚠ Nenhum centro urbano próximo às coordenadas ({lat}, {lon}).")
                    return None, None
        except Exception as e:
            logging.error(f"❌ Erro ao buscar centro urbano: {e}")
            return None, None