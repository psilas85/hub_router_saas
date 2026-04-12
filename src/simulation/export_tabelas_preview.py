import pandas as pd
from sqlalchemy import create_engine
import os

# 🔹 Conexão com o banco dentro do Docker
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Psilas@85")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_DATABASE_SIMULATION", "simulation_db")

# 🔹 Monta a string de conexão
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 🔹 Lista de tabelas no simulation_db
tabelas = [
    "resultados_simulacao",
    "resumo_clusters",
    "resumo_rotas_last_mile",
    "rotas_last_mile",
    "detalhes_rotas",
    "resumo_transferencias",
    "rotas_transferencias",
    "detalhes_transferencias",
    "entregas_clusterizadas",
    "cluster_costs",
    "veiculos_last_mile",
    "veiculos_transferencia",
    "hubs",
    "localizacoes",
    "cache_localizacoes",
    "cache_rotas"
]

# 🔹 Caminho para salvar no output
output_dir = "exports/simulation/entregas"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "tabelas_preview.xlsx")

# 🔹 Criar Excel com várias abas
with pd.ExcelWriter(output_path) as writer:
    for tabela in tabelas:
        query = f"SELECT * FROM {tabela} LIMIT 5"
        try:
            df = pd.read_sql(query, engine)
            df.to_excel(writer, sheet_name=tabela[:31], index=False)  # Nome de aba limitado a 31 chars
            print(f"✅ {tabela}: {len(df)} linhas")
        except Exception as e:
            print(f"⚠️ Erro ao ler {tabela}: {e}")

print(f"\n📂 Arquivo gerado: {output_path}")
