import psycopg2
import argparse
import os
import time
from dotenv import load_dotenv
from simulation.utils.helpers import ajustar_para_centro_urbano

# 🔄 Carrega o .env
load_dotenv()

def corrigir_em_chunks(tenant_id, chunk_size=10000, pause=2):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres_db"),
        dbname=os.getenv("DB_DATABASE_SIMULATION", "simulation_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
        port=os.getenv("DB_PORT", "5432")
    )
    cursor = conn.cursor()

    while True:
        # Busca um lote de coordenadas únicas
        cursor.execute("""
            SELECT DISTINCT centro_lat, centro_lon
            FROM entregas_clusterizadas
            WHERE tenant_id = %s AND cluster_cidade = 'Desconhecido'
            LIMIT %s
        """, (tenant_id, chunk_size))
        coords = cursor.fetchall()

        if not coords:
            print("🎉 Nenhuma coordenada 'Desconhecido' restante.")
            break

        print(f"🔎 Processando lote com {len(coords)} coordenadas...")

        atualizados = 0
        for lat, lon in coords:
            endereco, cidade = ajustar_para_centro_urbano(lat, lon, conn, tenant_id)

            if cidade and cidade != "Desconhecido":
                cursor.execute("""
                    UPDATE entregas_clusterizadas
                    SET cluster_cidade = %s
                    WHERE tenant_id = %s
                      AND cluster_cidade = 'Desconhecido'
                      AND centro_lat = %s
                      AND centro_lon = %s
                """, (cidade, tenant_id, lat, lon))
                atualizados += cursor.rowcount
                print(f"✅ ({lat:.5f}, {lon:.5f}) → {cidade} ({cursor.rowcount} registros atualizados)")

        conn.commit()
        print(f"📌 Lote finalizado: {atualizados} registros atualizados")
        time.sleep(pause)  # pequeno intervalo para não sobrecarregar

    cursor.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant_id", required=True)
    parser.add_argument("--chunk_size", type=int, default=10000)
    parser.add_argument("--pause", type=int, default=2, help="Segundos de pausa entre os lotes")
    args = parser.parse_args()

    corrigir_em_chunks(args.tenant_id, args.chunk_size, args.pause)
