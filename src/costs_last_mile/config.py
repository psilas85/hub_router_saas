import os
from dotenv import load_dotenv

# 🔹 Carregar variáveis do .env
load_dotenv()

# Configurações do Banco de Dados
DATABASE_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}
