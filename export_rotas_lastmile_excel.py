import pandas as pd
import psycopg2

# Parâmetros
TENANT_ID = "59919562-0160-4eee-95ac-8f2df3c6b043"
DATA = "2026-04-01"
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "simulation_db",
    "user": "postgres",
    "password": "Psilas@85"
}

# Consulta
query = '''
SELECT * FROM rotas_last_mile
WHERE tenant_id = %s AND envio_data = %s
'''

with psycopg2.connect(**DB_CONFIG) as conn:
    df = pd.read_sql_query(query, conn, params=(TENANT_ID, DATA))
    df.to_excel(f"rotas_last_mile_{TENANT_ID}_{DATA}.xlsx", index=False)
    print(f"Arquivo gerado: rotas_last_mile_{TENANT_ID}_{DATA}.xlsx")
