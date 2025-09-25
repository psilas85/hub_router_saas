#hub_router_1.0.1/src/simulation/visualization/gerador_relatorio_final.py

import os
import pandas as pd
import matplotlib.pyplot as plt

from simulation.visualization.gerar_relatorio_simulacao import gerar_relatorio_simulacao


def executar_geracao_relatorio_final(
    tenant_id: str,
    envio_data: str,
    simulation_id: str,
    simulation_db,
    base_dir="exports/simulation",
    modo_forcar: bool = False
):
    """
    Gera o relatório final em PDF com base nos resultados da simulação.
    - Gráficos consolidados de custos (PNG).
    - Dados de resultados por k_clusters.
    - Não converte mais mapas HTML para PNG.
    """
    maps_dir = os.path.join(base_dir, "maps", tenant_id)

    # Buscar os k_clusters salvos
    cursor = simulation_db.cursor()
    cursor.execute("""
        SELECT DISTINCT k_clusters
        FROM resultados_simulacao
        WHERE tenant_id = %s AND envio_data = %s
        ORDER BY k_clusters
    """, (tenant_id, envio_data))
    k_clusters_testados = [row[0] for row in cursor.fetchall()]
    cursor.close()

    if not k_clusters_testados:
        print(f"❌ Nenhum resultado encontrado em resultados_simulacao para envio_data = {envio_data}")
        return

    # Caminho do gráfico consolidado
    grafico_custo_path = os.path.join(base_dir, "graphs", tenant_id, f"grafico_simulacao_{envio_data}.png")

    # 🔹 Respeita modo_forcar para gráficos
    if modo_forcar or not os.path.exists(grafico_custo_path):
        try:
            query = """
                SELECT k_clusters, custo_transferencia, custo_last_mile, custo_cluster
                FROM resultados_simulacao
                WHERE tenant_id = %s AND envio_data = %s
                ORDER BY k_clusters
            """
            df = pd.read_sql(query, simulation_db, params=(tenant_id, envio_data))
            if not df.empty:
                df["custo_total"] = (
                    df["custo_transferencia"].fillna(0)
                    + df["custo_last_mile"].fillna(0)
                    + df["custo_cluster"].fillna(0)
                )

                fig, ax = plt.subplots(figsize=(8, 5))
                ax.bar(df["k_clusters"], df["custo_transferencia"], label="Transferência")
                ax.bar(df["k_clusters"], df["custo_last_mile"], bottom=df["custo_transferencia"], label="Last-mile")
                ax.bar(
                    df["k_clusters"],
                    df["custo_cluster"],
                    bottom=df["custo_transferencia"] + df["custo_last_mile"],
                    label="Cluster"
                )

                ax.plot(df["k_clusters"], df["custo_total"], color="black", marker="o", label="Custo Total")
                ax.set_title(f"Custo Total por cenário de clusters — {envio_data}")
                ax.set_xlabel("Número de clusters (0 = baseline hub central)")
                ax.set_ylabel("Custo (R$)")
                ax.legend()
                ax.grid(True)

                os.makedirs(os.path.dirname(grafico_custo_path), exist_ok=True)
                plt.tight_layout()
                plt.savefig(grafico_custo_path)
                plt.close()
                print(f"✅ Gráfico consolidado gerado: {grafico_custo_path}")
            else:
                print(f"⚠️ Nenhum dado encontrado para gerar gráfico consolidado ({envio_data}).")
                grafico_custo_path = None
        except Exception as e:
            print(f"⚠️ Erro ao gerar gráfico consolidado: {e}")
            grafico_custo_path = None

    # Geração do PDF (sempre sobrescreve)
    relatorio_dir = os.path.join(base_dir, "relatorios", tenant_id)
    os.makedirs(relatorio_dir, exist_ok=True)
    relatorio_path = gerar_relatorio_simulacao(
        tenant_id=tenant_id,
        envio_data=envio_data,
        simulation_id=simulation_id,
        k_clusters_testados=k_clusters_testados,
        simulation_db=simulation_db,
        base_dir=base_dir,
        grafico_custo_path=grafico_custo_path,
    )

    print(f"✅ Relatório consolidado gerado: {relatorio_path}")
    return relatorio_path
