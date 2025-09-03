#costs_last_mile/infrastructure/cost_db_last_mile.py

import psycopg2
import logging
import os
from dotenv import load_dotenv

# 🔹 Carregar variáveis do .env
load_dotenv()
print("🔍 Banco selecionado:", os.getenv("DB_NAME"))


def conectar_banco():
    """Conecta ao banco de dados PostgreSQL utilizando variáveis do .env."""
    try:
        conexao = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        logging.info("✅ Conexão com o banco de dados estabelecida com sucesso.")
        return conexao
    except Exception as e:
        logging.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None
