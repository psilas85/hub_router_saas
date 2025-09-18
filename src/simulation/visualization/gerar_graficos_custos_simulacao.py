#hub_router_1.0.1/src/simulation/visualization/gerar_graficos_custos_simulacao.py

import os
import pandas as pd
import matplotlib.pyplot as plt

def gerar_graficos_custos_por_envio(
    simulation_db,
    tenant_id,
    datas_filtradas,
    output_dir="exports/simulation/graphs",
    modo_forcar: bool = False
):
    output_dir = os.path.join(output_dir, tenant_id)
    os.makedirs(output_dir, exist_ok=True)

    if not datas_filtradas:
        print("⚠️ Nenhuma data processada recebida. Nenhum gráfico será gerado.")
        return False

    placeholders = ', '.join(['%s'] * len(datas_filtradas))
    query = f"""
        SELECT envio_data, k_clusters, custo_transferencia, custo_last_mile, custo_cluster, is_ponto_otimo
        FROM resultados_simulacao
        WHERE tenant_id = %s AND envio_data IN ({placeholders})
        ORDER BY envio_data, k_clusters
    """
    params = [tenant_id] + datas_filtradas

    try:
        simulation_db.rollback()
    except Exception:
        pass

    df = pd.read_sql(query, simulation_db, params=params)
    if df.empty:
        print(f"⚠️ Nenhum dado encontrado para datas {datas_filtradas}")
        return False

    for envio_data in df["envio_data"].unique():
        df_data = df[df["envio_data"] == envio_data]
        grafico_path = os.path.join(output_dir, f"grafico_simulacao_{envio_data}.png")

        if modo_forcar or not os.path.exists(grafico_path):
            try:
                fig, ax = plt.subplots(figsize=(8, 5))
                ax.bar(df_data["k_clusters"], df_data["custo_transferencia"], label="Transferência")
                ax.bar(df_data["k_clusters"], df_data["custo_last_mile"],
                       bottom=df_data["custo_transferencia"], label="Last-mile")
                ax.bar(df_data["k_clusters"], df_data["custo_cluster"],
                       bottom=df_data["custo_transferencia"] + df_data["custo_last_mile"],
                       label="Cluster")

                df_data["custo_total"] = (
                    df_data["custo_transferencia"].fillna(0)
                    + df_data["custo_last_mile"].fillna(0)
                    + df_data["custo_cluster"].fillna(0)
                )
                ax.plot(df_data["k_clusters"], df_data["custo_total"],
                        color="black", marker="o", label="Custo Total")

                ax.set_title(f"Custo Total por k_clusters — {envio_data}")
                ax.set_xlabel("k_clusters")
                ax.set_ylabel("Custo (R$)")
                ax.legend()
                ax.grid(True)

                plt.tight_layout()
                plt.savefig(grafico_path)
                plt.close()
                print(f"✅ Gráfico de custos salvo: {grafico_path}")
            except Exception as e:
                print(f"❌ Erro ao gerar gráfico para {envio_data}: {e}")

