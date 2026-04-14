# hub_router_1.0.1/src/simulation/visualization/gerar_graficos_custos_simulacao.py

import os
import pandas as pd
import matplotlib.pyplot as plt


def _formatar_rotulo_cenario(k: int) -> str:
    return "Hub unico" if int(k) == 0 else str(int(k))


def gerar_graficos_custos_por_envio(
    simulation_db,
    tenant_id,
    datas_filtradas,
    modo_forcar=False
):
    """
    Gera gráficos de custo por envio_data.
    Compatível com SimulationUseCase.
    """

    for envio_data in datas_filtradas:

        envio_data_local = str(envio_data)

        try:
            query = """
                SELECT k_clusters, custo_transferencia, custo_last_mile, custo_cluster
                FROM resultados_simulacao
                WHERE tenant_id = %s AND envio_data = %s
                ORDER BY k_clusters
            """

            df = pd.read_sql(query, simulation_db, params=(tenant_id, envio_data_local))

            if df.empty:
                print(f"⚠️ Sem dados para gráfico ({envio_data_local})")
                continue

            # 🔹 custo total
            df["custo_total"] = (
                df["custo_transferencia"].fillna(0)
                + df["custo_last_mile"].fillna(0)
                + df["custo_cluster"].fillna(0)
            )

            # 🔹 gráfico
            fig, ax = plt.subplots(figsize=(8, 5))

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

            ax.plot(
                df["k_clusters"],
                df["custo_total"],
                marker="o",
                label="Custo Total"
            )

            ax.set_title(f"Custo Total por cenário — {envio_data_local}")
            ax.set_xticks(df["k_clusters"])
            ax.set_xticklabels([_formatar_rotulo_cenario(k) for k in df["k_clusters"]])
            ax.set_xlabel("Cenários")
            ax.set_ylabel("Custo (R$)")
            ax.legend()
            ax.grid(True)

            # 🔹 salvar
            output_dir = os.path.join(
                "exports/simulation/graphs",
                tenant_id,
                envio_data_local
            )

            os.makedirs(output_dir, exist_ok=True)

            grafico_path = os.path.join(
                output_dir,
                f"grafico_simulacao_{envio_data_local}.png"
            )

            if modo_forcar or not os.path.exists(grafico_path):
                plt.tight_layout()
                plt.savefig(grafico_path)
                print(f"✅ Gráfico salvo: {grafico_path}")

            plt.close()

        except Exception as e:
            print(f"❌ Erro ao gerar gráfico ({envio_data_local}): {e}")