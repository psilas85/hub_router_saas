#exploratory_analysis/infrastructure/database_connection.py

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path="exploratory_analysis/.env")

def conectar_clusterization_db():
    try:
        return psycopg2.connect(
            dbname=os.getenv("CLUSTERIZATION_DB"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
    except Exception as e:
        raise RuntimeError(f"Erro ao conectar ao banco: {e}")

