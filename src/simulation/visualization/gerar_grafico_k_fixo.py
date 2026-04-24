# hub_router_1.0.1/src/simulation/visualization/gerar_grafico_k_fixo.py

# hub_router_1.0.1/src/simulation/visualization/gerar_grafico_k_fixo.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.ticker as mtick

matplotlib.use("Agg")

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.utils.path_builder import build_output_path


def _formatar_rotulo_cenario(k: int) -> str:
    return "Hub unico" if int(k) == 0 else str(int(k))


def gerar_grafico_k_fixo(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    base_dir: str = "exports/simulation",
    modo_forcar: bool = False,
    min_cobertura_parcial: float = 0.70
):
    """
    Avalia cenários de k_clusters com base em cobertura:

    FULL: 100% cobertura → usa custo real
    PARCIAL: projeta custo usando custo médio por entrega
    """

    # 🔧 normalização
    min_cobertura_parcial = max(0.01, min(1.0, min_cobertura_parcial))
    cobertura_tag = f"cov{int(round(min_cobertura_parcial * 100)):03d}"

    periodo = f"{data_inicial}_{data_final}"

    graphs_dir = build_output_path(
        base_dir,
        tenant_id,
        periodo,
        "graphs"
    )

    png_path = os.path.join(graphs_dir, f"k_fixo_{periodo}_{cobertura_tag}.png")
    csv_path = os.path.join(graphs_dir, f"k_fixo_{periodo}_{cobertura_tag}.csv")

    # 🔥 controle overwrite
    if not modo_forcar and os.path.exists(png_path) and os.path.exists(csv_path):
        print(f"🟡 Já existe: {png_path}")
        return png_path, csv_path, pd.read_csv(csv_path)

    conn = conectar_simulation_db()

    try:
        # =============================
        # 🔹 totais do período
        # =============================
        q_totais = """
            SELECT
              COUNT(*) AS total_dias,
              SUM(max_entregas) AS total_entregas
            FROM (
              SELECT envio_data, MAX(quantidade_entregas) AS max_entregas
              FROM resultados_simulacao
              WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
              GROUP BY envio_data
            ) sub
        """

        tot = pd.read_sql(q_totais, conn, params=(tenant_id, data_inicial, data_final))

        total_dias = int(tot["total_dias"].iat[0] or 0)
        total_entregas = int(tot["total_entregas"].iat[0] or 0)

        if total_dias == 0 or total_entregas == 0:
            print("⚠️ Sem dados no período")
            return None, None, pd.DataFrame()

        # =============================
        # 🔹 cobertura
        # =============================
        q_cobertura = """
            WITH dias AS (
              SELECT DISTINCT envio_data
              FROM resultados_simulacao
              WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
            ),
            tot AS ( SELECT COUNT(*) AS total_dias FROM dias ),
            presenca AS (
              SELECT k_clusters, COUNT(DISTINCT envio_data) AS dias_presentes
              FROM resultados_simulacao
              WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
              GROUP BY k_clusters
            )
            SELECT
              p.k_clusters,
              p.dias_presentes,
              t.total_dias,
              p.dias_presentes::numeric / NULLIF(t.total_dias::numeric, 0) AS cobertura_pct
            FROM presenca p CROSS JOIN tot t
        """

        cov = pd.read_sql(
            q_cobertura,
            conn,
            params=(tenant_id, data_inicial, data_final, tenant_id, data_inicial, data_final)
        )

        if cov.empty:
            print("⚠️ Sem cenários")
            return None, None, pd.DataFrame()

        cov = cov[cov["cobertura_pct"] >= min_cobertura_parcial].copy()

        if cov.empty:
            print(f"❌ Nenhum cenário ≥ {min_cobertura_parcial:.0%}")
            return None, None, pd.DataFrame()

        cov["modo"] = cov["cobertura_pct"].apply(lambda x: "F" if x >= 0.999999 else "P")
        k_list = cov["k_clusters"].astype(int).tolist()

        # =============================
        # 🔹 agregados
        # =============================
        q_agregado = """
            SELECT
              k_clusters,
              SUM(custo_total)         AS soma_custo_total,
              SUM(quantidade_entregas) AS soma_entregas_presentes
            FROM resultados_simulacao
            WHERE tenant_id = %s
              AND envio_data BETWEEN %s AND %s
              AND k_clusters = ANY(%s)
            GROUP BY k_clusters
        """

        df = pd.read_sql(q_agregado, conn, params=(tenant_id, data_inicial, data_final, k_list))

        # =============================
        # 🔹 benchmark ótimo
        # =============================
        q_otimo = """
            SELECT SUM(min_custo) AS soma_otimo
            FROM (
              SELECT envio_data, MIN(custo_total) AS min_custo
              FROM resultados_simulacao
              WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
              GROUP BY envio_data
            ) x
        """

        soma_otimo = pd.read_sql(q_otimo, conn, params=(tenant_id, data_inicial, data_final))["soma_otimo"].iat[0] or 0.0

        # =============================
        # 🔹 merge
        # =============================
        df = df.merge(
            cov[["k_clusters", "dias_presentes", "total_dias", "cobertura_pct", "modo"]],
            on="k_clusters",
            how="left"
        )

        # =============================
        # 🔹 custo projetado
        # =============================
        def calcular_custo(row):
            if row["modo"] == "F":
                return row["soma_custo_total"]
            else:
                custo_medio = row["soma_custo_total"] / max(1, row["soma_entregas_presentes"])
                faltantes = max(0, total_entregas - row["soma_entregas_presentes"])
                return row["soma_custo_total"] + (faltantes * custo_medio)

        df["custo_alvo"] = df.apply(calcular_custo, axis=1)

        # =============================
        # 🔹 regret
        # =============================
        df["regret_absoluto"] = df["custo_alvo"] - soma_otimo
        df["regret_relativo"] = df["custo_alvo"] / soma_otimo - 1 if soma_otimo else None

        df = df.sort_values("k_clusters").reset_index(drop=True)

        df_export = df[[
            "k_clusters",
            "dias_presentes",
            "total_dias",
            "cobertura_pct",
            "custo_alvo",
            "regret_absoluto",
            "regret_relativo"
        ]]

        df_export.to_csv(csv_path, index=False)

        # =============================
        # 🔹 gráfico
        # =============================
        x = df["k_clusters"].astype(int)

        plt.figure(figsize=(10, 6))
        plt.bar(x, df["custo_alvo"])

        plt.ylabel("Custo consolidado (R$)")
        plt.xlabel("Cenários")
        plt.xticks(x, [_formatar_rotulo_cenario(k) for k in x])

        best = df.iloc[df["custo_alvo"].argmin()]
        best_k = int(best["k_clusters"])

        plt.title(
            f"Custo por cenário • {data_inicial} → {data_final}\n"
            f"Melhor: {_formatar_rotulo_cenario(best_k)}"
        )

        plt.gca().yaxis.set_major_formatter(
            mtick.FuncFormatter(lambda x, _: f"{int(x/1000)} mil")
        )

        plt.grid(axis="y", linestyle="--", alpha=0.7)

        plt.tight_layout()
        plt.savefig(png_path)
        plt.close()

        print(f"✅ Gráfico: {png_path}")
        print(f"✅ CSV: {csv_path}")

        return png_path, csv_path, df_export

    finally:
        try:
            conn.close()
        except:
            pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant_id", required=True)
    parser.add_argument("--data_inicial", required=True)
    parser.add_argument("--data_final", required=True)
    parser.add_argument("--modo_forcar", action="store_true")

    args = parser.parse_args()

    gerar_grafico_k_fixo(
        tenant_id=args.tenant_id,
        data_inicial=args.data_inicial,
        data_final=args.data_final,
        modo_forcar=args.modo_forcar
    )