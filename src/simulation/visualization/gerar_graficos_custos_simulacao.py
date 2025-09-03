#hub_router_1.0.1/src/simulation/visualization/gerar_graficos_custos_simulacao.py

import os
import pandas as pd
import matplotlib.pyplot as plt

def gerar_graficos_custos_por_envio(simulation_db, tenant_id, datas_filtradas=None, output_dir="exports/simulation/graphs"):
    output_dir = os.path.join(output_dir, tenant_id)
    os.makedirs(output_dir, exist_ok=True)

    if datas_filtradas:
        placeholders = ', '.join(['%s'] * len(datas_filtradas))
        query = f"""
            SELECT envio_data, k_clusters, custo_transferencia, custo_last_mile, custo_cluster, is_ponto_otimo
            FROM resultados_simulacao
            WHERE tenant_id = %s AND envio_data IN ({placeholders})
            ORDER BY envio_data, k_clusters
        """
        params = [tenant_id] + datas_filtradas
    else:
        query = """
            SELECT envio_data, k_clusters, custo_transferencia, custo_last_mile, custo_cluster, is_ponto_otimo
            FROM resultados_simulacao
            WHERE tenant_id = %s
            ORDER BY envio_data, k_clusters
        """
        params = [tenant_id]

    try:
        simulation_db.rollback()
    except Exception:
        pass

    df = pd.read_sql(query, simulation_db, params=params)
    for col in ["custo_transferencia", "custo_last_mile", "custo_cluster"]:
        if col not in df.columns:
            df[col] = 0.0

    df["envio_data"] = df["envio_data"].astype(str)

    for envio_data, df_envio in df.groupby("envio_data"):
        df_envio = df_envio.sort_values("k_clusters").copy()
        df_envio["custo_total"] = (
            df_envio["custo_transferencia"] +
            df_envio["custo_last_mile"] +
            df_envio["custo_cluster"]
        )

        for _, row in df_envio.iterrows():
            k = row["k_clusters"]
            fig, ax = plt.subplots(figsize=(8, 5))

            # Barras empilhadas
            ax.bar([1], [row["custo_transferencia"]], label="Transferência")
            ax.bar([1], [row["custo_last_mile"]], bottom=row["custo_transferencia"], label="Last-mile")
            ax.bar([1], [row["custo_cluster"]],
                   bottom=row["custo_transferencia"] + row["custo_last_mile"],
                   label="Cluster")

            custo_total = row["custo_total"]
            ax.set_title(f"Custo total (k={k}) — {envio_data}")
            ax.set_ylabel("Custo (R$)")
            ax.legend()
            ax.grid(True)

            path = os.path.join(output_dir, f"grafico_custos_{envio_data}_k{k}.png")
            plt.tight_layout()
            plt.savefig(path)
            plt.close()
            print(f"✅ Gráfico salvo: {path}")

    return True
