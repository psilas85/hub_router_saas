# hub_router_1.0.1/src/data_input/infrastructure/db.py

import psycopg2
import logging
import os
import traceback
import pandas as pd
from decimal import Decimal
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class Database:
    """Gerencia a conexão com o banco de dados clusterization_db"""

    def __init__(self):
        self.DB_HOST = os.getenv("DB_HOST", "localhost")
        self.DB_PORT = os.getenv("DB_PORT", "5432")
        self.DB_NAME = "clusterization_db"
        self.DB_USER = os.getenv("DB_USER", "postgres")
        self.DB_PASSWORD = os.getenv("DB_PASSWORD", "senha")
        self.conexao = None

    def conectar(self):
        """Cria uma conexão com o banco clusterization_db."""
        try:
            self.conexao = psycopg2.connect(
                dbname=self.DB_NAME,
                user=self.DB_USER,
                password=self.DB_PASSWORD,
                host=self.DB_HOST,
                port=self.DB_PORT
            )
            logging.info("✅ Conexão com clusterization_db estabelecida com sucesso.")
        except Exception as e:
            logging.error(f"❌ Erro ao conectar ao clusterization_db: {e}")
            self.conexao = None

    def fechar_conexao(self):
        """Fecha a conexão com o banco de dados."""
        if self.conexao:
            self.conexao.close()
            logging.info("🔒 Conexão com clusterization_db encerrada.")


    def inserir_dados_entregas(self, df):
        """Insere os dados no banco garantindo que os tipos estão corretos."""
        if self.conexao is None:
            logging.error("❌ Erro: conexão com o banco não está ativa!")
            return

        if df.empty:
            logging.warning("⚠ Nenhum dado para inserir na tabela 'entregas'.")
            return

        # 🔹 Substituir NaN e NaT por None para evitar erro na inserção
        df = df.where(pd.notna(df), None)

        # 🔹 Convertendo colunas numéricas para Decimal (necessário para o PostgreSQL)
        colunas_numericas = ["cte_peso", "cte_valor_nf", "cte_valor_frete"]
        for col in colunas_numericas:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: Decimal(str(x)) if x is not None else None)

        # 🔹 Garantindo que a data está formatada corretamente e convertendo `NaT` para `None`
        df["envio_data"] = df["envio_data"].apply(lambda x: x if pd.notna(x) else None)  # Mantém apenas valores válidos
        df["envio_data"] = df["envio_data"].apply(lambda x: x if pd.notna(x) else None)

        # 🔹 Convertendo DataFrame para lista de tuplas, garantindo que os valores estejam na ordem correta
        colunas_ordem = [
            "cte_numero", "remetente_cnpj", "cte_rua", "cte_bairro", "cte_complemento", "cte_cidade",
            "cte_uf", "cte_cep", "cte_nf", "cte_volumes", "cte_peso", "cte_valor_nf", "cte_valor_frete",
            "envio_data", "endereco_completo", "transportadora", "remetente_nome", "destinatario_nome",
            "destinatario_cnpj", "destino_latitude", "destino_longitude", "remetente_cidade", "remetente_uf",
            "doc_min"
        ]
        data_values = df[colunas_ordem].astype(object).where(pd.notna(df), None).to_records(index=False).tolist()

        logging.info(f"📌 Primeiros valores de 'envio_data' antes da inserção no banco:\n{df[['envio_data']].dropna().head(20).to_string(index=False)}")

        # 🔹 Query de inserção
        query = """
        INSERT INTO entregas (
            cte_numero, remetente_cnpj, cte_rua, cte_bairro, cte_complemento, cte_cidade, cte_uf,
            cte_cep, cte_nf, cte_volumes, cte_peso, cte_valor_nf, cte_valor_frete, envio_data,
            endereco_completo, transportadora, remetente_nome, destinatario_nome, destinatario_cnpj,
            destino_latitude, destino_longitude, remetente_cidade, remetente_uf, doc_min
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cte_numero, transportadora)
        DO UPDATE SET
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
            envio_data = COALESCE(EXCLUDED.envio_data, entregas.envio_data), -- Evita sobrescrever com NULL
            endereco_completo = EXCLUDED.endereco_completo,
            transportadora = EXCLUDED.transportadora,
            remetente_nome = EXCLUDED.remetente_nome,
            destinatario_nome = EXCLUDED.destinatario_nome,
            destinatario_cnpj = EXCLUDED.destinatario_cnpj,
            destino_latitude = EXCLUDED.destino_latitude,
            destino_longitude = EXCLUDED.destino_longitude,
            remetente_cidade = EXCLUDED.remetente_cidade,
            remetente_uf = EXCLUDED.remetente_uf,
            doc_min = EXCLUDED.doc_min;
        """

       # 🔹 Log para verificar os dados antes da inserção
        logging.info(f"📊 Tipos de dados antes da inserção:\n{df.dtypes}")
        logging.info(f"🔍 Primeiras 5 linhas antes da inserção:\n{df.head().to_dict(orient='records')}")
        # 📊 Verificar se há valores nulos na 'envio_data'
        logging.info(f"📊 Valores nulos em 'envio_data': {df['envio_data'].isna().sum()} de {len(df)} registros")
        logging.info(f"📊 Exemplos de 'envio_data' antes da inserção:\n{df[['envio_data']].head(10)}")

        # 🔹 Inserção no banco com tratamento de erros
        try:
            with self.conexao.cursor() as cursor:
                cursor.executemany(query, data_values)
            self.conexao.commit()
            logging.info(f"✅ {len(df)} registros inseridos/atualizados com sucesso na tabela 'entregas'.")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao inserir dados no banco: {e}")
            raise

    import traceback

    def buscar_dados_clusterizacao(self, data_inicial, data_final):
        """Busca os dados da clusterização armazenados no banco."""
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
            logging.error(traceback.format_exc())  # Exibe traceback completo
            raise  # Relança a exceção para depuração adequada


    def salvar_resumo_clusterizacao(self, resumo):
        """
        Salva o resumo da clusterização no banco.
        """
        query = """
        INSERT INTO resumo_clusterizacao (
            envio_data, cluster, quantidade_entregas, peso_total_kg,
            distancia_media_km, tempo_estimado_min, quantidade_volumes, centroid_lat, centroid_lon
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (envio_data, cluster)
        DO UPDATE SET
            quantidade_entregas = EXCLUDED.quantidade_entregas,
            peso_total_kg = EXCLUDED.peso_total_kg,
            distancia_media_km = EXCLUDED.distancia_media_km,
            tempo_estimado_min = EXCLUDED.tempo_estimado_min,
            quantidade_volumes = EXCLUDED.quantidade_volumes,
            centroid_lat = EXCLUDED.centroid_lat,
            centroid_lon = EXCLUDED.centroid_lon;
        """
        try:
            with self.conexao.cursor() as cursor:
                cursor.executemany(query, resumo)
            self.conexao.commit()
            logging.info("✅ Resumo da clusterização salvo no banco de dados.")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao salvar resumo da clusterização: {e}")


    def buscar_localizacao(self, endereco):
        """Busca latitude e longitude de um endereço no banco de dados."""
        query = "SELECT latitude, longitude FROM localizacoes WHERE endereco = %s"
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(query, (endereco,))
                return cursor.fetchone()
        except Exception as e:
            logging.error(f"❌ Erro ao buscar localização no banco: {e}")
            return None

    def inserir_localizacao(self, endereco, latitude, longitude):
        """Insere nova localização no banco de dados se ainda não existir."""
        query_insert = """
        INSERT INTO localizacoes (endereco, latitude, longitude, criado_em)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (endereco) DO NOTHING;
        """
        try:
            with self.conexao.cursor() as cursor:
                cursor.execute(query_insert, (endereco, latitude, longitude))
            self.conexao.commit()
            logging.info(f"✅ Coordenadas de '{endereco}' salvas no banco: ({latitude}, {longitude})")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao inserir localização no banco: {e}")

    def buscar_entregas(self, data_inicial, data_final, transportadora=None):
        """
        Busca os dados necessários da tabela entregas para clusterização.
        Filtra por transportadora, se especificado.
        """
        query = """
        SELECT
            id AS id_entrega, cte_numero, transportadora, envio_data,
            cte_cidade, cte_uf, cte_cep, cte_volumes,
            cte_peso, cte_valor_nf, cte_valor_frete, destino_latitude, destino_longitude
        FROM public.entregas
        WHERE envio_data BETWEEN %s AND %s
        """

        params = [data_inicial, data_final]

        if transportadora:
            query += " AND transportadora = %s"
            params.append(transportadora)

        try:
            df = pd.read_sql(query, self.conexao, params=params)
            logging.info(f"✅ {len(df)} registros de entregas recuperados para clusterização.")
            return df
        except Exception as e:
            logging.error(f"❌ Erro ao buscar entregas para clusterização: {e}")
            return pd.DataFrame()


    def buscar_centro_urbano(self, lat, lon):
        """
        Busca um endereço na tabela 'localizacoes' baseado na latitude e longitude.
        Retorna o endereço mais próximo encontrado.
        """
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
                cursor.execute(query, (float(lon), float(lat)))  # Longitude primeiro!
                resultado = cursor.fetchone()
                if resultado:
                    endereco, latitude, longitude = resultado
                    logging.info(f"📍 Centro urbano encontrado na tabela 'localizacoes': {endereco} ({latitude}, {longitude})")
                    return endereco, (latitude, longitude)
                else:
                    logging.warning(f"⚠ Nenhum centro urbano encontrado próximo às coordenadas ({lat}, {lon}).")
                    return None, None
        except Exception as e:
            logging.error(f"❌ Erro ao buscar centro urbano: {e}")
            return None, None


    def salvar_clusterizacao(self, clustered_data):
        """Salva os dados clusterizados garantindo integridade da `envio_data`."""
        if self.conexao is None:
            logging.error("❌ Conexão com o banco não está ativa!")
            return

        if clustered_data.empty:
            logging.warning("⚠ Nenhum dado para salvar em 'entregas_clusterizadas'.")
            return

        clustered_data["envio_data"] = pd.to_datetime(clustered_data["envio_data"], errors="coerce").dt.date

        logging.info(f"📊 Primeiras linhas antes de salvar no banco:\n{clustered_data.head().to_string(index=False)}")
        logging.info(f"📊 Dados antes da inserção:\n{clustered_data[['cte_numero', 'cluster']].head(10)}")

        insert_query = """
            INSERT INTO entregas_clusterizadas (
                id_entrega, cte_numero, transportadora, envio_data,
                cluster, cluster_cidade, centro_lat, centro_lon, data_hora
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (cte_numero, transportadora)
            DO UPDATE SET
                cluster = EXCLUDED.cluster,
                cluster_cidade = EXCLUDED.cluster_cidade,
                centro_lat = EXCLUDED.centro_lat,
                centro_lon = EXCLUDED.centro_lon,
                envio_data = COALESCE(entregas_clusterizadas.envio_data, EXCLUDED.envio_data),
                data_hora = NOW();
        """

        registros_novos = clustered_data[
            ["id_entrega", "cte_numero", "transportadora", "envio_data", "cluster", "cluster_cidade", "centro_lat", "centro_lon"]
        ].astype(object).where(pd.notna(clustered_data), None).to_records(index=False).tolist()

        try:
            with self.conexao.cursor() as cursor:
                cursor.executemany(insert_query, registros_novos)
            self.conexao.commit()
            logging.info(f"✅ {len(registros_novos)} registros de clusterização salvos/atualizados.")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao salvar clusterização: {e}")

    def atualizar_data_processamento(self, df):
        """Atualiza a data e hora de processamento na tabela `entregas`."""
        if self.conexao is None:
            logging.error("❌ Erro: conexão com o banco não está ativa!")
            return

        if df.empty:
            logging.warning("⚠ Nenhum dado para atualizar `data_processamento` em 'entregas'.")
            return

        try:
            with self.conexao.cursor() as cursor:
                for cte_numero in df["cte_numero"].unique():
                    query = """
                    UPDATE entregas
                    SET data_processamento = NOW()
                    WHERE cte_numero = %s;
                    """
                    cursor.execute(query, (cte_numero,))

            self.conexao.commit()
            logging.info(f"✅ Data de processamento atualizada para {len(df)} registros na tabela 'entregas'.")
        except Exception as e:
            self.conexao.rollback()
            logging.error(f"❌ Erro ao atualizar `data_processamento`: {e}")
