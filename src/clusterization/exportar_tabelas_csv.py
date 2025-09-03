import os
import pandas as pd
import psycopg2
from datetime import datetime

# 🔧 Configurações de conexão
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "clusterization_db",
    "user": "postgres",
    "password": "Psilas@85"
}

# 🔧 Parâmetros de execução
TABELAS = ["entregas", "entregas_clusterizadas"]
TENANT_ID = "dev_tenant"
DATA_INICIAL = "2025-05-23"
DATA_FINAL = "2025-05-23"
OUTPUT_DIR = "csv_exportados"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 📦 Conexão com banco de dados
conn = psycopg2.connect(**DB_CONFIG)

for tabela in TABELAS:
    try:
        query = f"""
        SELECT * FROM {tabela}
        WHERE tenant_id = %s
        AND envio_data BETWEEN %s AND %s
        """
        df = pd.read_sql_query(query, conn, params=(TENANT_ID, DATA_INICIAL, DATA_FINAL))

        # ✅ Conversão opcional de colunas decimais para padrão brasileiro
        colunas_decimais = ["cte_peso", "cte_valor", "cte_volumes"]
        for col in colunas_decimais:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: str(x).replace('.', ','))

        output_path = os.path.join(OUTPUT_DIR, f"{tabela}_{DATA_INICIAL}_a_{DATA_FINAL}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8-sig", sep=";")
        print(f"✅ Exportado: {output_path}")
    except Exception as e:
        print(f"❌ Erro ao exportar {tabela}: {e}")

conn.close()

# 📁 Exporta registros inválidos (pré-processamento)
invalidos_path = os.path.join("tenants", TENANT_ID, "output", "registros_invalidos.csv")
if os.path.exists(invalidos_path):
    df_invalidos = pd.read_csv(invalidos_path)
    output_invalidos = os.path.join(OUTPUT_DIR, f"registros_invalidos_{DATA_INICIAL}_a_{DATA_FINAL}.csv")
    df_invalidos.to_csv(output_invalidos, index=False, encoding="utf-8-sig", sep=";")
    print(f"⚠️ Exportado inválidos: {output_invalidos}")
else:
    print("ℹ️ Nenhum arquivo de registros inválidos encontrado.")

# 📂 Exporta input original
input_path = os.path.join("tenants", TENANT_ID, "input", "dados_input.csv")
if os.path.exists(input_path):
    df_input = pd.read_csv(input_path, delimiter=";", encoding="utf-8", dtype=str)
    output_input = os.path.join(OUTPUT_DIR, f"dados_input_original_{DATA_INICIAL}.csv")
    df_input.to_csv(output_input, index=False, encoding="utf-8-sig", sep=";")
    print(f"📄 Exportado input original: {output_input}")
else:
    print("ℹ️ Arquivo de input original não encontrado.")
