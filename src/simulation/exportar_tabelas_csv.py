import os
import pandas as pd
import psycopg2
from datetime import datetime

# 🔧 Configurações
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "simulation_db",
    "user": "postgres",
    "password": "Psilas@85"
}

TABELAS = [
    "cache_localizacoes",
    "cache_rotas",
    "detalhes_rotas",
    "detalhes_transferencias",
    "entregas_clusterizadas",
    "hubs",
    "localizacoes",
    "resultados_simulacao",
    "resumo_transferencias",
    "rotas_last_mile",
    "rotas_transferencias",
    "resumo_clusters",
    "veiculos_last_mile",
    "veiculos_transferencia"
]

TENANT_ID = "dev_tenant"
DATA_INICIAL = "2025-05-23"
DATA_FINAL = "2025-05-23"
OUTPUT_DIR = "csv_exportados"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 🧠 Exportação
conn = psycopg2.connect(**DB_CONFIG)

for tabela in TABELAS:
    try:
        query = f"""
        SELECT * FROM {tabela}
        WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
        """
        df = pd.read_sql_query(query, conn, params=(TENANT_ID, DATA_INICIAL, DATA_FINAL))
        output_path = os.path.join(OUTPUT_DIR, f"{tabela}_{DATA_INICIAL}_a_{DATA_FINAL}.csv")
        df.to_csv(output_path, index=False)
        print(f"✅ Exportado: {output_path}")
    except Exception as e:
        print(f"❌ Erro ao exportar {tabela}: {e}")

conn.close()
