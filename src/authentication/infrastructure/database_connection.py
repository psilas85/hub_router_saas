#authentication/infrastructure/database_connection.py

import os
import psycopg2
from dotenv import load_dotenv

# 🟩 Carrega variáveis do .env global do projeto
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
load_dotenv(dotenv_path)

def conectar_banco():
    try:
        conn = psycopg2.connect(
            dbname = os.getenv("DB_DATABASE") or os.getenv("CLUSTER_DB"),  # se necessário
            user = os.getenv("DB_USER"),
            password = os.getenv("DB_PASS"),  # <- aqui estava o erro
            host = os.getenv("DB_HOST"),
            port = os.getenv("DB_PORT")

        )
        print("✅ Conexão com o banco de dados (authentication) estabelecida com sucesso.")
        return conn
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco de dados (authentication): {e}")
        return None


def conectar_banco_cluster():
    return conectar_banco()

def conectar_banco_routing():
    return conectar_banco("DB_DATABASE_ROUTING")

def fechar_conexao(conn):
    if conn:
        conn.close()
