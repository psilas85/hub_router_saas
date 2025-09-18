# simulation/visualization/gerar_grafico_frequencia_cidades.py

import os
import pandas as pd
import matplotlib.pyplot as plt
from simulation.infrastructure.simulation_database_connection import conectar_simulation_db

def gerar_grafico_frequencia_cidades(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    output_dir="exports/simulation/graphs"
):
    """
    Gera gráfico de barras com a frequência das cidades centro (cluster_cidade)
    que aparecem em simulações ponto ótimo no período informado (limitado a 12 meses).
    """
    conn = conectar_simulation_db()

    query = """
        SELECT ec.cluster_cidade, COUNT(*) AS qtd
        FROM entregas_clusterizadas ec
        JOIN resultados_simulacao r
          ON ec.simulation_id = r.simulation_id
         AND ec.k_clusters = r.k_clusters
         AND ec.tenant_id = r.tenant_id
         AND ec.envio_data = r.envio_data
        WHERE ec.tenant_id = %s
          AND ec.envio_data BETWEEN %s AND %s
          AND r.is_ponto_otimo = TRUE
        GROUP BY ec.cluster_cidade
        ORDER BY qtd DESC
    """
    df = pd.read_sql(query, conn, params=(tenant_id, data_inicial, data_final))
    conn.close()

    if df.empty:
        return {
            "status": "vazio",
            "data_inicial": data_inicial,
            "data_final": data_final,
            "grafico": None,
            "csv": None,
            "dados": []
        }

    os.makedirs(f"{output_dir}/{tenant_id}", exist_ok=True)

    # Arquivos de saída
    filename_png = os.path.join(
        output_dir, tenant_id, f"frequencia_cidades_{data_inicial}_{data_final}.png"
    )
    filename_csv = os.path.join(
        output_dir, tenant_id, f"frequencia_cidades_{data_inicial}_{data_final}.csv"
    )

    # Gráfico
    plt.figure(figsize=(10, 6))
    plt.barh(df["cluster_cidade"], df["qtd"], color="seagreen")
    plt.xlabel("Frequência como Centro (ponto ótimo)")
    plt.ylabel("Cidade")
    plt.title(f"Frequência de Cidades em Pontos Ótimos ({data_inicial} → {data_final})")
    plt.gca().invert_yaxis()
    plt.grid(axis="x", linestyle="--", alpha=0.7)
    plt.tight_layout()
    plt.savefig(filename_png, bbox_inches="tight")
    plt.close()

    # CSV
    df.to_csv(filename_csv, index=False, encoding="utf-8-sig")

    return {
        "status": "ok",
        "data_inicial": data_inicial,
        "data_final": data_final,
        "grafico": filename_png.replace("./", "/"),
        "csv": filename_csv.replace("./", "/"),
        "dados": df.to_dict(orient="records")
    }
