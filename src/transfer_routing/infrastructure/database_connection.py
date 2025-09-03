import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


def conectar_banco_cluster():
    return psycopg2.connect(
        dbname=os.getenv("CLUSTER_DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )


def conectar_banco_routing():
    return psycopg2.connect(
        dbname=os.getenv("ROUTING_DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )


def fechar_conexao(conn):
    if conn:
        conn.close()
