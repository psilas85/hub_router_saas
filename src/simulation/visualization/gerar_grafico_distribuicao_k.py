# simulation/visualization/gerar_grafico_distribuicao_k.py

import os
import pandas as pd
import matplotlib.pyplot as plt
from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.utils.artefatos_cleaner_distribuicao import limpar_artefatos_distribuicao

def gerar_grafico_distribuicao_k(
    tenant_id: str, data_inicial: str, data_final: str, output_dir="exports/simulation/graphs"
):
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

    # 🔄 Limpar artefatos antigos
    limpar_artefatos_distribuicao(output_dir, tenant_id, data_inicial, data_final)

    filename = os.path.join(
        output_dir, tenant_id, f"distribuicao_k_{data_inicial}_{data_final}.png"
    )

    # 🎨 Garantir estilo colorido
    plt.style.use("default")

    # 🎨 Gráfico mais bonito
    plt.figure(figsize=(8, 6))
    bars = plt.bar(
        df["k_clusters"],
        df["qtd"],
        color=(70/255, 130/255, 180/255),  # steelblue RGB
        edgecolor="black"
    )

    # Adicionar valores no topo de cada barra
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2,
            yval + 0.1,
            int(yval),
            ha='center', va='bottom',
            fontsize=10, fontweight='bold'
        )

    plt.xlabel("Número de Clusters (k)", fontsize=12, fontweight="bold")
    plt.ylabel("Frequência como Ponto Ótimo", fontsize=12, fontweight="bold")
    plt.title(
        f"Distribuição de k_clusters ({data_inicial} → {data_final})",
        fontsize=14, fontweight="bold", color="#333333"
    )

    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=120)
    plt.close()

    return filename, df.to_dict(orient="records")
