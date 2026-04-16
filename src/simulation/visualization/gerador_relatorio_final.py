#hub_router_1.0.1/src/simulation/visualization/gerador_relatorio_final.py

# hub_router_1.0.1/src/simulation/visualization/gerador_relatorio_final.py

import os
import pandas as pd
import matplotlib.pyplot as plt

from simulation.visualization.gerar_relatorio_simulacao import gerar_relatorio_simulacao
from simulation.utils.path_builder import build_output_path


def _formatar_rotulo_cenario(k: int) -> str:
    return "Hub unico" if int(k) == 0 else str(int(k))


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

    Estrutura padronizada:
    exports/simulation/{tenant_id}/{envio_data}/
        ├── maps/
        ├── tables/
        ├── graphs/
        └── reports/
    """

    envio_data = str(envio_data)

    # 🔥 Caminhos padronizados
    maps_dir = build_output_path(base_dir, tenant_id, envio_data, "maps")
    graphs_dir = build_output_path(base_dir, tenant_id, envio_data, "graphs")
    reports_dir = build_output_path(base_dir, tenant_id, envio_data, "reports")

    # =============================
    # 🔹 Buscar cenários (k_clusters)
    # =============================
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
        print(f"❌ Nenhum resultado encontrado para envio_data={envio_data}")
        return None

    # =============================
    # 🔹 Gráfico consolidado
    # =============================
    grafico_custo_path = os.path.join(
        graphs_dir,
        f"grafico_simulacao_{envio_data}.png"
    )

    if modo_forcar or not os.path.exists(grafico_custo_path):

        try:
            query = """
                SELECT k_clusters, custo_transferencia, custo_last_mile, custo_cluster
                FROM resultados_simulacao
                WHERE tenant_id = %s AND envio_data = %s
                ORDER BY k_clusters
            """

            df = pd.read_sql(query, simulation_db, params=(tenant_id, envio_data))

            if df.empty:
                print(f"⚠️ Sem dados para gráfico ({envio_data})")
                grafico_custo_path = None

            else:
                df["custo_total"] = (
                    df["custo_transferencia"].fillna(0)
                    + df["custo_last_mile"].fillna(0)
                    + df["custo_cluster"].fillna(0)
                )

                fig, ax = plt.subplots(figsize=(8, 5))

                # barras empilhadas
                ax.bar(df["k_clusters"], df["custo_transferencia"], label="Transferência")

                ax.bar(
                    df["k_clusters"],
                    df["custo_last_mile"],
                    bottom=df["custo_transferencia"],
                    label="Last-mile"
                )

                ax.bar(
                    df["k_clusters"],
                    df["custo_cluster"],
                    bottom=df["custo_transferencia"] + df["custo_last_mile"],
                    label="Cluster"
                )

                # linha total
                ax.plot(
                    df["k_clusters"],
                    df["custo_total"],
                    color="black",
                    marker="o",
                    label="Custo Total"
                )

                ax.set_title(f"Custo Total por cenário — {envio_data}")
                ax.set_xticks(df["k_clusters"])
                ax.set_xticklabels([_formatar_rotulo_cenario(k) for k in df["k_clusters"]])
                ax.set_xlabel("Cenários")
                ax.set_ylabel("Custo (R$)")
                ax.legend()
                ax.grid(True)

                plt.tight_layout()
                plt.savefig(grafico_custo_path)
                plt.close()

                print(f"✅ Gráfico salvo: {grafico_custo_path}")

        except Exception as e:
            print(f"❌ Erro ao gerar gráfico: {e}")
            grafico_custo_path = None

    # =============================
    # 🔹 Geração do relatório PDF
    # =============================
    relatorio_path = gerar_relatorio_simulacao(
        tenant_id=tenant_id,
        envio_data=envio_data,
        simulation_id=simulation_id,
        k_clusters_testados=k_clusters_testados,
        simulation_db=simulation_db,
        base_dir=base_dir,
        grafico_custo_path=grafico_custo_path,
    )

    print(f"✅ Relatório gerado: {relatorio_path}")

    return relatorio_path