import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
import os

# Carrega variáveis de ambiente
load_dotenv()


def conectar_banco_cluster():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("CLUSTER_DB"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except OperationalError as e:
        raise Exception(f"❌ Erro ao conectar no banco de clusterização: {e}")


def conectar_banco_routing():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("ROUTING_DB"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except OperationalError as e:
        raise Exception(f"❌ Erro ao conectar no banco de roteirização: {e}")


def fechar_conexao(conn):
    if conn:
        conn.close()
