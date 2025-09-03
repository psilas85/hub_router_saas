#simulation_database_connection.py
import psycopg2
from dotenv import load_dotenv
import os

# Carrega variáveis de ambiente
load_dotenv()

def conectar_clusterization_db():
    conn = psycopg2.connect(
        dbname=os.getenv("CLUSTERIZATION_DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO public;")
    return conn

def conectar_simulation_db():
    conn = psycopg2.connect(
        dbname=os.getenv("SIMULATION_DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    # Define explicitamente o schema como public
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO public;")
    return conn

