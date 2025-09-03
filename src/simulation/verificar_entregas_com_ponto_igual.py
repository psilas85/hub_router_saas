import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# 🔧 Parâmetros
TENANT_ID = 'dev_tenant'
ENVIO_DATA = '2025-05-19'

# Teste rápido das conexões
def testar_conexao(dbname):
    try:
        conn = psycopg2.connect(
            host="localhost", port="5432", database=dbname, user="postgres", password="Psilas@85"
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        conn.close()
        print(f"✅ Conexão com {dbname} bem-sucedida.")
    except Exception as e:
        print(f"❌ Erro ao conectar ao {dbname}: {e}")

testar_conexao("simulation_db")
testar_conexao("clusterization_db")

# Criar conexões reais para uso
conn_sim = psycopg2.connect(
    host="localhost", port="5432", database="simulation_db", user="postgres", password="Psilas@85"
)
conn_clu = psycopg2.connect(
    host="localhost", port="5432", database="clusterization_db", user="postgres", password="Psilas@85"
)

# 📤 Query entregas clusterizadas
df_clusterizadas = pd.read_sql("""
    SELECT cte_numero, centro_lat, centro_lon
    FROM entregas_clusterizadas
    WHERE tenant_id = %s AND envio_data = %s
""", conn_sim, params=(TENANT_ID, ENVIO_DATA))

# 📤 Query entregas originais
df_entregas = pd.read_sql("""
    SELECT cte_numero, destino_latitude, destino_longitude
    FROM entregas
    WHERE tenant_id = %s AND envio_data = %s
""", conn_clu, params=(TENANT_ID, ENVIO_DATA))


# 🔗 Merge
df_merged = pd.merge(df_clusterizadas, df_entregas, on="cte_numero", how="inner")

# 🔍 Verificar entregas com ponto do cluster igual ao destino
df_iguais = df_merged[
    (df_merged["centro_lat"].round(5) == df_merged["destino_latitude"].round(5)) &
    (df_merged["centro_lon"].round(5) == df_merged["destino_longitude"].round(5))
]

# 💾 Salvar CSV para auditoria
output_path = f"output/auditorias/entregas_iguais_{TENANT_ID}_{ENVIO_DATA}.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)
df_iguais.to_csv(output_path, index=False)
print(f"✅ Entregas com pontos idênticos salvas em: {output_path}")
