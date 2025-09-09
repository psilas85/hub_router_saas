# simulation/visualization/gerar_grafico_frota_k_fixo.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
from matplotlib.ticker import MaxNLocator

matplotlib.use("Agg")

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db


def gerar_grafico_frota_k_fixo(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    k_list: list,
    output_dir: str = "exports/simulation/graphs",
    min_cobertura_parcial: float = 0.70
):
    """
    Avalia frota média necessária no período para um ou mais k_fixo informados.
    Consolida rotas de last-mile e transferências (sem filtrar por simulation_id).
    Exporta CSV consolidado e detalhado, e gera gráfico de barras por k_fixo.
    """

    os.makedirs(os.path.join(output_dir, tenant_id), exist_ok=True)
    base_name = f"frota_k_fixo_{data_inicial}_{data_final}_{'_'.join(map(str,k_list))}"
    csv_path = os.path.join(output_dir, tenant_id, base_name + ".csv")
    csv_detalhado = os.path.join(output_dir, tenant_id, base_name + "_detalhado.csv")

    conn = conectar_simulation_db()
    try:
        # 1) Total de dias distintos no período
        q_tot_dias = """
            SELECT COUNT(DISTINCT envio_data) AS total_dias
            FROM resumo_rotas_last_mile
            WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
        """
        tot = pd.read_sql(q_tot_dias, conn, params=(tenant_id, data_inicial, data_final))
        total_dias = int(tot["total_dias"].iat[0] or 0)
        if total_dias == 0:
            print("⚠️ Não há dados no período informado.")
            return None, None, pd.DataFrame()

        resultados, detalhados = [], []

        for k_fixo in k_list:
            # 2) Buscar rotas last-mile
            q_last_mile = """
                SELECT envio_data, tipo_veiculo, COUNT(*) AS qtd_veiculos
                FROM resumo_rotas_last_mile
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                  AND k_clusters = %s
                GROUP BY envio_data, tipo_veiculo
            """
            df_last = pd.read_sql(q_last_mile, conn, params=(tenant_id, data_inicial, data_final, k_fixo))

            # 3) Buscar transferências
            q_transfer = """
                SELECT envio_data, tipo_veiculo, COUNT(DISTINCT rota_id) AS qtd_veiculos
                FROM detalhes_transferencias
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                  AND k_clusters = %s
                GROUP BY envio_data, tipo_veiculo
            """
            df_transf = pd.read_sql(q_transfer, conn, params=(tenant_id, data_inicial, data_final, k_fixo))

            # 4) Consolida
            df_comb = pd.concat([df_last, df_transf], ignore_index=True)
            if df_comb.empty:
                print(f"⚠️ Nenhum dado encontrado para k={k_fixo}.")
                continue

            # 5) Cobertura
            dias_presentes = df_comb["envio_data"].nunique()
            cobertura_pct = dias_presentes / total_dias if total_dias else 0
            modo = "FULL" if cobertura_pct >= 0.999999 else "PARCIAL"

            # 6) Média consolidada (ceil)
            df_frota = (
                df_comb.groupby("tipo_veiculo")["qtd_veiculos"]
                .mean()
                .apply(lambda x: int(-(-x // 1)))  # ceil inteiro
                .reset_index()
            )
            df_frota = df_frota.rename(columns={"qtd_veiculos": "frota_sugerida"})
            df_frota["k_clusters"] = k_fixo
            df_frota["dias_presentes"] = dias_presentes
            df_frota["total_dias"] = total_dias
            df_frota["cobertura_pct"] = round(cobertura_pct, 4)
            df_frota["modo"] = modo

            resultados.append(df_frota)
            df_comb["k_clusters"] = k_fixo
            detalhados.append(df_comb)

            # 7) Gráfico de barras individual (ordenado decrescente)
            png_path = os.path.join(output_dir, tenant_id, f"{base_name}_k{k_fixo}.png")
            df_frota = df_frota.sort_values("frota_sugerida", ascending=False)

            plt.figure(figsize=(10, 6))
            plt.bar(df_frota["tipo_veiculo"], df_frota["frota_sugerida"], color="steelblue")

            plt.ylabel("Frota média sugerida (veículos/dia)")
            plt.xlabel("Tipo de Veículo")

            # Calcular total da frota
            total_frota = df_frota["frota_sugerida"].sum()

            plt.title(
                f"Frota Média Sugerida • {data_inicial} → {data_final} • k={k_fixo}\n"
                f"Total de Frota: {total_frota} veículos",
                fontsize=12,
                fontweight="bold"
            )

            # Forçar eixo Y inteiro
            plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True))

            # Adicionar rótulo em cada barra
            for idx, row in df_frota.iterrows():
                plt.text(
                    row["tipo_veiculo"],
                    row["frota_sugerida"] + 0.2,
                    str(row["frota_sugerida"]),
                    ha="center",
                    va="bottom",
                    fontsize=9,
                    fontweight="bold"
                )

            plt.tight_layout()
            plt.savefig(png_path, bbox_inches="tight")
            plt.close()

            print(f"✅ PNG gráfico: {png_path}")

        # 8) Exporta consolidado e detalhado
        df_consolidado = pd.concat(resultados, ignore_index=True)
        df_detalhado = pd.concat(detalhados, ignore_index=True)

        df_consolidado.to_csv(csv_path, index=False)
        df_detalhado.to_csv(csv_detalhado, index=False)

        print(f"✅ CSV consolidado: {csv_path}")
        print(f"✅ CSV detalhado:  {csv_detalhado}")

        return None, csv_path, df_consolidado

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Gera gráfico da frota média por tipo de veículo para cada k_fixo informado.")
    parser.add_argument("--tenant_id", required=True, type=str)
    parser.add_argument("--data_inicial", required=True, type=str)
    parser.add_argument("--data_final", required=True, type=str)
    parser.add_argument("--k_fixo", required=True, nargs="+", type=int, help="Um ou mais valores de k (ex: 8 9 10)")
    parser.add_argument("--output_dir", default="exports/simulation/graphs")
    parser.add_argument("--min_cobertura_parcial", default=0.70, type=float)
    args = parser.parse_args()

    _, csv, df = gerar_grafico_frota_k_fixo(
        tenant_id=args.tenant_id,
        data_inicial=args.data_inicial,
        data_final=args.data_final,
        k_list=args.k_fixo,
        output_dir=args.output_dir,
        min_cobertura_parcial=args.min_cobertura_parcial,
    )
    print("CSV consolidado:", csv)
    print("Linhas:", 0 if df is None else len(df))
