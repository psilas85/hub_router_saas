# simulation/visualization/gerar_grafico_distribuicao_k.py

import os
import pandas as pd
import matplotlib.pyplot as plt
from simulation.infrastructure.simulation_database_connection import conectar_simulation_db

def gerar_grafico_distribuicao_k(tenant_id: str, data_inicial: str, data_final: str, output_dir="exports/simulation/graphs"):
    """
    Gera gráfico de barras com a frequência de k_clusters eleitos ponto ótimo
    no período informado (limitado a 12 meses).
    """
    conn = conectar_simulation_db()

    query = """
        SELECT k_clusters, COUNT(*) as qtd
        FROM resultados_simulacao
        WHERE tenant_id = %s
          AND envio_data BETWEEN %s AND %s
          AND is_ponto_otimo = TRUE
        GROUP BY k_clusters
        ORDER BY k_clusters
    """
    df = pd.read_sql(query, conn, params=(tenant_id, data_inicial, data_final))
    conn.close()

    if df.empty:
        return None, None

    os.makedirs(f"{output_dir}/{tenant_id}", exist_ok=True)
    filename = os.path.join(
        output_dir, tenant_id,
        f"distribuicao_k_{data_inicial}_{data_final}.png"
    )

    plt.figure(figsize=(8, 6))
    plt.bar(df["k_clusters"], df["qtd"], color="steelblue")
    plt.xlabel("Número de Clusters (k)")
    plt.ylabel("Frequência como Ponto Ótimo")
    plt.title(f"Distribuição de k_clusters ({data_inicial} → {data_final})")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    plt.savefig(filename, bbox_inches="tight")
    plt.close()

    return filename, df.to_dict(orient="records")
