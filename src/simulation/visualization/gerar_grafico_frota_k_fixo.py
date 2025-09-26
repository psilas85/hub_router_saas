# src/simulation/visualization/gerar_grafico_frota_k_fixo.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.ticker import MaxNLocator
from matplotlib.patches import Patch
from simulation.utils.artefatos_cleaner_frota import limpar_artefatos_frota

matplotlib.use("Agg")

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db


def _gerar_grafico(df_frota, png_path, data_inicial, data_final, k_fixo, cobertura_pct, titulo_extra=""):
    """Gera gráfico de barras para frota média sugerida."""
    df_frota = df_frota.sort_values("frota_sugerida", ascending=False)

    # Paleta dinâmica
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
        f"Frota Média Sugerida • {titulo_k} • {data_inicial} → {data_final}\n"
        f"{titulo_extra} • Total: {total_frota} veículos • Cobertura: {cobertura_pct:.1%}",
        fontsize=12,
        fontweight="bold"
    )

    plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True))

    # Rótulos nas barras
    for bar, val in zip(bars, df_frota["frota_sugerida"]):
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.3,
            f"{val:,}".replace(",", "."),
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold"
        )

    # Legenda automática
    legend_handles = [Patch(facecolor=cores[tv], edgecolor="black", label=tv) for tv in unique_veiculos]
    plt.legend(handles=legend_handles, title="Tipos de Veículo", bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.tight_layout()
    plt.savefig(png_path, bbox_inches="tight", dpi=150)
    plt.close()
    print(f"✅ PNG gráfico: {png_path}")


def gerar_grafico_frota_k_fixo(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    k_fixo: int,
    output_dir: str = "exports/simulation/graphs",
    min_cobertura_parcial: float = 0.70
):
    os.makedirs(os.path.join(output_dir, tenant_id), exist_ok=True)

    # Limpeza logo no início
    limpar_artefatos_frota(output_dir, tenant_id, data_inicial, data_final)

    conn = conectar_simulation_db()
    try:
        # Dias totais no período
        q_tot_dias = """
            SELECT COUNT(DISTINCT envio_data) AS total_dias
            FROM resumo_rotas_last_mile
            WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
        """
        tot = pd.read_sql(q_tot_dias, conn, params=(tenant_id, data_inicial, data_final))
        total_dias = int(tot["total_dias"].iat[0] or 0)
        if total_dias == 0:
            print("⚠️ Não há dados no período informado.")
            return None, None, [], []

        resultados_last, detalhados_last = [], []
        resultados_transf, detalhados_transf = [], []

        # --- Last-mile ---
        if k_fixo == 0:
            q_last_mile = """
                SELECT envio_data, tipo_veiculo, k_clusters, COUNT(*) AS qtd_veiculos
                FROM resumo_rotas_last_mile
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                GROUP BY envio_data, tipo_veiculo, k_clusters
            """
            df_last = pd.read_sql(q_last_mile, conn, params=(tenant_id, data_inicial, data_final))
        else:
            q_last_mile = """
                SELECT envio_data, tipo_veiculo, COUNT(*) AS qtd_veiculos
                FROM resumo_rotas_last_mile
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                  AND k_clusters = %s
                GROUP BY envio_data, tipo_veiculo
            """
            df_last = pd.read_sql(q_last_mile, conn, params=(tenant_id, data_inicial, data_final, k_fixo))

        csv_lastmile = None
        csv_transfer = None

        if not df_last.empty:
            dias_presentes = df_last["envio_data"].nunique()
            cobertura_pct = dias_presentes / total_dias if total_dias else 0
            modo = "FULL" if cobertura_pct >= 0.999999 else "PARCIAL"

            df_frota = (
                df_last.groupby("tipo_veiculo")["qtd_veiculos"]
                .mean()
                .apply(lambda x: int(-(-x // 1)))  # arredonda para cima
                .reset_index()
            )
            df_frota = df_frota.rename(columns={"qtd_veiculos": "frota_sugerida"})
            df_frota["k_clusters"] = k_fixo
            df_frota["dias_presentes"] = dias_presentes
            df_frota["total_dias"] = total_dias
            df_frota["cobertura_pct"] = round(cobertura_pct, 4)
            df_frota["modo"] = modo
            df_frota["origem"] = "lastmile"

            resultados_last.append(df_frota)
            df_last["k_clusters"] = k_fixo
            df_last["origem"] = "lastmile"
            detalhados_last.append(df_last)

            png_path = os.path.join(output_dir, tenant_id, f"frota_lastmile_{data_inicial}_{data_final}_k{k_fixo}.png")
            _gerar_grafico(df_frota, png_path, data_inicial, data_final, k_fixo, cobertura_pct, "Last-mile")

            base_name = f"frota_lastmile_{data_inicial}_{data_final}_k{k_fixo}"
            csv_lastmile = os.path.join(output_dir, tenant_id, base_name + ".csv")
            df_frota.to_csv(csv_lastmile, index=False)

        # --- Transferências ---
        if k_fixo == 0:
            df_transf = pd.DataFrame()  # 🚫 vazio para k=0
        else:
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
            modo = "FULL" if cobertura_pct >= 0.999999 else "PARCIAL"

            df_frota = (
                df_transf.groupby("tipo_veiculo")["qtd_veiculos"]
                .mean()
                .apply(lambda x: int(-(-x // 1)))
                .reset_index()
            )
            df_frota = df_frota.rename(columns={"qtd_veiculos": "frota_sugerida"})
            df_frota["k_clusters"] = k_fixo
            df_frota["dias_presentes"] = dias_presentes
            df_frota["total_dias"] = total_dias
            df_frota["cobertura_pct"] = round(cobertura_pct, 4)
            df_frota["modo"] = modo
            df_frota["origem"] = "transfer"

            resultados_transf.append(df_frota)
            df_transf["k_clusters"] = k_fixo
            df_transf["origem"] = "transfer"
            detalhados_transf.append(df_transf)

            png_path = os.path.join(output_dir, tenant_id, f"frota_transfer_{data_inicial}_{data_final}_k{k_fixo}.png")
            _gerar_grafico(df_frota, png_path, data_inicial, data_final, k_fixo, cobertura_pct, "Transferências")

            base_name = f"frota_transfer_{data_inicial}_{data_final}_k{k_fixo}"
            csv_transfer = os.path.join(output_dir, tenant_id, base_name + ".csv")
            df_frota.to_csv(csv_transfer, index=False)

        # Retorno final
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

    parser = argparse.ArgumentParser(description="Gera gráficos de frota média por tipo de veículo (last-mile e transferências).")
    parser.add_argument("--tenant_id", required=True, type=str)
    parser.add_argument("--data_inicial", required=True, type=str)
    parser.add_argument("--data_final", required=True, type=str)
    parser.add_argument("--k_fixo", required=True, type=int, help="Valor de k fixo (0 = todos os cenários, ex: 8)")
    parser.add_argument("--output_dir", default="exports/simulation/graphs")
    parser.add_argument("--min_cobertura_parcial", default=0.70, type=float)
    args = parser.parse_args()

    gerar_grafico_frota_k_fixo(
        tenant_id=args.tenant_id,
        data_inicial=args.data_inicial,
        data_final=args.data_final,
        k_fixo=args.k_fixo,
        output_dir=args.output_dir,
        min_cobertura_parcial=args.min_cobertura_parcial,
    )
