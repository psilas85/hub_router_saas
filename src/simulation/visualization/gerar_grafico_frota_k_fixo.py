#hub_router_1.0.1/src/simulation/visualization/gerar_grafico_frota_k_fixo.py

# hub_router_1.0.1/src/simulation/visualization/gerar_grafico_frota_k_fixo.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

from matplotlib.ticker import MaxNLocator
from matplotlib.patches import Patch

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.utils.path_builder import build_output_path

matplotlib.use("Agg")


def _gerar_grafico(df_frota, png_path, data_inicial, data_final, k_fixo, cobertura_pct, titulo_extra=""):

    df_frota = df_frota.sort_values("frota_sugerida", ascending=False)

    unique_veiculos = df_frota["tipo_veiculo"].unique()
    cmap = plt.get_cmap("tab20")
    cores = {tv: cmap(i % cmap.N) for i, tv in enumerate(unique_veiculos)}

    plt.figure(figsize=(10, 6))

    bars = plt.bar(
        df_frota["tipo_veiculo"],
        df_frota["frota_sugerida"],
        color=[cores[tv] for tv in df_frota["tipo_veiculo"]],
        edgecolor="black"
    )

    plt.ylabel("Frota média sugerida (veículos/dia)")
    plt.xlabel("Tipo de Veículo")
    plt.grid(axis="y", linestyle="--", alpha=0.3)

    total_frota = df_frota["frota_sugerida"].sum()
    titulo_k = "todos os cenários" if k_fixo == 0 else f"k={k_fixo}"

    plt.title(
        f"Frota Média • {titulo_k} • {data_inicial} → {data_final}\n"
        f"{titulo_extra} • Total: {total_frota} veículos • Cobertura: {cobertura_pct:.1%}",
        fontsize=12,
        fontweight="bold"
    )

    plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True))

    for bar, val in zip(bars, df_frota["frota_sugerida"]):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.3,
            f"{val}",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold"
        )

    legend_handles = [
        Patch(facecolor=cores[tv], edgecolor="black", label=tv)
        for tv in unique_veiculos
    ]

    plt.legend(handles=legend_handles, title="Tipos de Veículo", bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.tight_layout()
    plt.savefig(png_path, bbox_inches="tight", dpi=150)
    plt.close()

    print(f"✅ Gráfico salvo: {png_path}")


def gerar_grafico_frota_k_fixo(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    k_fixo: int,
    base_dir: str = "exports/simulation",
    modo_forcar: bool = False
):

    periodo = f"{data_inicial}_{data_final}"

    graphs_dir = build_output_path(
        base_dir,
        tenant_id,
        periodo,
        "graphs"
    )

    conn = conectar_simulation_db()

    try:
        # =============================
        # 🔹 total de dias
        # =============================
        q_tot_dias = """
            SELECT COUNT(DISTINCT envio_data) AS total_dias
            FROM resumo_rotas_last_mile
            WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
        """

        tot = pd.read_sql(q_tot_dias, conn, params=(tenant_id, data_inicial, data_final))
        total_dias = int(tot["total_dias"].iat[0] or 0)

        if total_dias == 0:
            print("⚠️ Sem dados no período")
            return None, None, [], []

        resultados_last, resultados_transf = [], []

        # =============================
        # 🔹 LAST-MILE
        # =============================
        if k_fixo == 0:
            q_last = """
                SELECT envio_data, tipo_veiculo, k_clusters, COUNT(*) AS qtd_veiculos
                FROM resumo_rotas_last_mile
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                GROUP BY envio_data, tipo_veiculo, k_clusters
            """
            df_last = pd.read_sql(q_last, conn, params=(tenant_id, data_inicial, data_final))
        else:
            q_last = """
                SELECT envio_data, tipo_veiculo, COUNT(*) AS qtd_veiculos
                FROM resumo_rotas_last_mile
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                  AND k_clusters = %s
                GROUP BY envio_data, tipo_veiculo
            """
            df_last = pd.read_sql(q_last, conn, params=(tenant_id, data_inicial, data_final, k_fixo))

        csv_lastmile = None
        csv_transfer = None

        if not df_last.empty:

            dias_presentes = df_last["envio_data"].nunique()
            cobertura_pct = dias_presentes / total_dias if total_dias else 0

            df_frota = (
                df_last.groupby("tipo_veiculo")["qtd_veiculos"]
                .mean()
                .apply(lambda x: int(-(-x // 1)))
                .reset_index()
                .rename(columns={"qtd_veiculos": "frota_sugerida"})
            )

            resultados_last.append(df_frota)

            png_path = os.path.join(
                graphs_dir,
                f"frota_lastmile_{periodo}_k{k_fixo}.png"
            )

            if modo_forcar or not os.path.exists(png_path):
                _gerar_grafico(df_frota, png_path, data_inicial, data_final, k_fixo, cobertura_pct, "Last-mile")

            csv_lastmile = os.path.join(
                graphs_dir,
                f"frota_lastmile_{periodo}_k{k_fixo}.csv"
            )
            df_frota.to_csv(csv_lastmile, index=False)

        # =============================
        # 🔹 TRANSFER
        # =============================
        if k_fixo != 0:

            q_transfer = """
                SELECT envio_data, tipo_veiculo, COUNT(DISTINCT rota_id) AS qtd_veiculos
                FROM detalhes_transferencias
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                  AND k_clusters = %s
                GROUP BY envio_data, tipo_veiculo
            """

            df_transf = pd.read_sql(q_transfer, conn, params=(tenant_id, data_inicial, data_final, k_fixo))

            if not df_transf.empty:

                dias_presentes = df_transf["envio_data"].nunique()
                cobertura_pct = dias_presentes / total_dias if total_dias else 0

                df_frota = (
                    df_transf.groupby("tipo_veiculo")["qtd_veiculos"]
                    .mean()
                    .apply(lambda x: int(-(-x // 1)))
                    .reset_index()
                    .rename(columns={"qtd_veiculos": "frota_sugerida"})
                )

                resultados_transf.append(df_frota)

                png_path = os.path.join(
                    graphs_dir,
                    f"frota_transfer_{periodo}_k{k_fixo}.png"
                )

                if modo_forcar or not os.path.exists(png_path):
                    _gerar_grafico(df_frota, png_path, data_inicial, data_final, k_fixo, cobertura_pct, "Transfer")

                csv_transfer = os.path.join(
                    graphs_dir,
                    f"frota_transfer_{periodo}_k{k_fixo}.csv"
                )
                df_frota.to_csv(csv_transfer, index=False)

        lastmile = pd.concat(resultados_last, ignore_index=True).to_dict(orient="records") if resultados_last else []
        transfer = pd.concat(resultados_transf, ignore_index=True).to_dict(orient="records") if resultados_transf else []

        return csv_lastmile, csv_transfer, lastmile, transfer

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant_id", required=True)
    parser.add_argument("--data_inicial", required=True)
    parser.add_argument("--data_final", required=True)
    parser.add_argument("--k_fixo", required=True, type=int)
    parser.add_argument("--modo_forcar", action="store_true")

    args = parser.parse_args()

    gerar_grafico_frota_k_fixo(
        tenant_id=args.tenant_id,
        data_inicial=args.data_inicial,
        data_final=args.data_final,
        k_fixo=args.k_fixo,
        modo_forcar=args.modo_forcar
    )