# costs_transfer/infrastructure/transfer_cost_db.py

import psycopg2
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

# Carrega o .env da pasta do módulo
dotenv_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=dotenv_path, override=True)


def conectar_banco():
    """Conecta ao banco de dados PostgreSQL utilizando variáveis do .env e define o search_path para 'public'."""
    try:
        conexao = psycopg2.connect(
            dbname=os.getenv("DB_DATABASE"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )

        # 🔧 Força o uso do schema 'public'
        with conexao.cursor() as cursor:
            cursor.execute("SET search_path TO public;")

        logging.info("✅ Conexão com o banco de dados estabelecida com sucesso.")
        return conexao

    except Exception as e:
        logging.error(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None
