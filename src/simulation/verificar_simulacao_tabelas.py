import psycopg2
import pandas as pd

TENANT_ID = 'dev_tenant'
ENVIO_DATA = '2025-06-23'

QUERIES = {
    "📌 entregas_clusterizadas": f"""
        SELECT cluster, cte_numero, centro_lat, centro_lon, cluster_cidade, k_clusters
        FROM entregas_clusterizadas
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY cluster, cte_numero
    """,
    "📊 resumo_clusters": f"""
        SELECT cluster, centro_lat, centro_lon, qde_ctes
        FROM resumo_clusters
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY cluster
    """,
    "🚛 detalhes_transferencias": f"""
        SELECT cluster, cte_numero, rota_id, tipo_veiculo
        FROM detalhes_transferencias
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY rota_id, cte_numero
    """,
    "🔁 resumo_transferencias": f"""
        SELECT rota_id, qde_entregas, qde_clusters_rota, tipo_veiculo
        FROM resumo_transferencias
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY rota_id
    """,
    "🚚 rotas_transferencias": f"""
        SELECT rota_id, tipo_veiculo, tempo_total_min, distancia_total_km
        FROM rotas_transferencias
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY rota_id
    """,
    "📦 detalhes_rotas (last-mile)": f"""
        SELECT cluster, cte_numero, rota_id, tipo_veiculo
        FROM detalhes_rotas
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY rota_id, cte_numero
    """,
    "🛣️ resumo_rotas_last_mile": f"""
        SELECT rota_id, qde_entregas, tipo_veiculo
        FROM resumo_rotas_last_mile
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY rota_id
    """,
    "🗺️ rotas_last_mile": f"""
        SELECT rota_id, tempo_total_min, distancia_total_km, tipo_veiculo
        FROM rotas_last_mile
        WHERE tenant_id = '{TENANT_ID}' AND envio_data = '{ENVIO_DATA}'
        ORDER BY rota_id
    """
}

def conectar():
    return psycopg2.connect(
        dbname="simulation_db",
        user="postgres",
        password="Psilas@85",
        host="localhost",
        port="5432"
    )

def executar_consultas():
    conn = conectar()
    for titulo, query in QUERIES.items():
        print(f"\n{'='*80}\n{titulo}\n{'='*80}")
        try:
            df = pd.read_sql(query, conn)
            print(df.to_string(index=False))
        except Exception as e:
            print(f"❌ Erro ao consultar {titulo}: {e}")
    conn.close()

if __name__ == "__main__":
    executar_consultas()
