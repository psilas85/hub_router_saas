import os
import psycopg2
from dotenv import load_dotenv

from simulation.infrastructure.cache_routes import obter_rota_real, obter_rota_last_mile

load_dotenv()

# 🔹 Conexão com o banco
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_DATABASE = os.getenv("SIMULATION_DB", "simulation_db")
TENANT_ID = os.getenv("DEFAULT_TENANT_ID", "dev_tenant")

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_DATABASE
)

# 🔹 Coordenadas de teste (lat, lon) - pode trocar se quiser
origem = (-3.85632, -38.49850)      # Horizonte/CE
destino = (-4.331452, -40.153602)   # Canindé/CE


def testar_obter_rota_real():
    print("\n=== Testando obter_rota_real ===")
    distancia, tempo, rota = obter_rota_real(origem, destino, TENANT_ID, conn)
    print(f"Distância: {distancia} km | Tempo: {tempo} min | Pontos: {len(rota)}")


def testar_obter_rota_last_mile():
    print("\n=== Testando obter_rota_last_mile ===")
    distancia, tempo, rota = obter_rota_last_mile(origem, destino, TENANT_ID, DB_DATABASE, conn)
    print(f"Distância: {distancia} km | Tempo: {tempo} min | Pontos: {len(rota)}")


if __name__ == "__main__":
    try:
        testar_obter_rota_real()
        testar_obter_rota_last_mile()
    finally:
        conn.close()
