# hub_router_1.0.1/src/simulation/visualization/gerar_grafico_k_fixo.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.ticker as mtick


matplotlib.use("Agg")

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.utils.artefatos_cleaner_k import limpar_artefatos_k


def _formatar_rotulo_cenario(k: int) -> str:
    return "Hub unico" if int(k) == 0 else str(int(k))


def gerar_grafico_k_fixo(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    output_dir: str = None,
    min_cobertura_parcial: float = 0.70
):
    """
    Avalia todos os cenários de k_clusters no período informado:
      - Considera apenas aqueles com cobertura >= min_cobertura_parcial.
      - FULL = 100% de cobertura → usa custo total real.
      - PARCIAL = entre min_cobertura_parcial e 100% → projeta custo
        completando entregas faltantes com custo médio por entrega.
    """

    # 🚦 Normaliza min_cobertura_parcial
    if min_cobertura_parcial < 0.01:
        min_cobertura_parcial = 0.01
    elif min_cobertura_parcial > 1.0:
        min_cobertura_parcial = 1.0

    if output_dir is None:
        raise ValueError("output_dir deve ser informado no novo padrão!")
    os.makedirs(output_dir, exist_ok=True)

    # 🔄 Limpeza deve ocorrer logo no início
    limpar_artefatos_k(output_dir, tenant_id, data_inicial, data_final)

    png_path = os.path.join(output_dir, f"k_fixo_{data_inicial}_{data_final}.png")
    csv_path = os.path.join(output_dir, f"k_fixo_{data_inicial}_{data_final}.csv")

    conn = conectar_simulation_db()
    try:
        # 1) Totais do período (máximo por dia)
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
            print("⚠️ Não há dados no período informado.")
            return None, None, pd.DataFrame()

        # 2) Cobertura por k
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
        cov = pd.read_sql(q_cobertura, conn,
                          params=(tenant_id, data_inicial, data_final,
                                  tenant_id, data_inicial, data_final))
        if cov.empty:
            print("⚠️ Não há cenários k no período.")
            return None, None, pd.DataFrame()

        # 3) Filtrar >= min_cobertura_parcial
        cov = cov[cov["cobertura_pct"] >= min_cobertura_parcial].copy()
        if cov.empty:
            print(f"❌ Nenhum cenário com cobertura ≥ {min_cobertura_parcial:.0%}.")
            return None, None, pd.DataFrame()

        # Classificação F ou P
        cov["modo"] = cov["cobertura_pct"].apply(lambda x: "F" if x >= 0.999999 else "P")
        k_list = cov["k_clusters"].astype(int).tolist()

        # 4) Agregados por k
        q_agregado = """
            SELECT
              k_clusters,
              SUM(custo_total)         AS soma_custo_total,
              SUM(quantidade_entregas) AS soma_entregas_presentes,
              COUNT(*)                 AS qtd_dias
            FROM resultados_simulacao
            WHERE tenant_id = %s
              AND envio_data BETWEEN %s AND %s
              AND k_clusters = ANY(%s)
            GROUP BY k_clusters
        """
        df = pd.read_sql(q_agregado, conn, params=(tenant_id, data_inicial, data_final, k_list))

        # 5) Benchmark ótimo diário
        q_otimo = """
            SELECT SUM(min_custo) AS soma_otimo_diario
            FROM (
              SELECT envio_data, MIN(custo_total) AS min_custo
              FROM resultados_simulacao
              WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
              GROUP BY envio_data
            ) x
        """
        soma_otimo = pd.read_sql(q_otimo, conn, params=(tenant_id, data_inicial, data_final))["soma_otimo_diario"].iat[0] or 0.0

        # 6) Merge cobertura
        df = df.merge(cov[["k_clusters", "dias_presentes", "total_dias", "cobertura_pct", "modo"]],
                      on="k_clusters", how="left")

        # 7) Cálculo do custo alvo (real + projetado)
        def calcular_custo(row):
            if row["modo"] == "F":
                return row["soma_custo_total"]
            else:
                custo_medio_entrega = row["soma_custo_total"] / max(1, row["soma_entregas_presentes"])
                entregas_faltantes = max(0, total_entregas - row["soma_entregas_presentes"])
                return row["soma_custo_total"] + (entregas_faltantes * custo_medio_entrega)

        df["custo_alvo"] = df.apply(calcular_custo, axis=1)

        # 8) Regret
        df["soma_otimo_diario"] = soma_otimo
        df["regret_absoluto"] = df["custo_alvo"] - soma_otimo
        df["regret_relativo"] = df.apply(
            lambda r: (float(r["custo_alvo"]) / soma_otimo - 1.0) if soma_otimo else None, axis=1
        )

        # 9) Ordenar e salvar (sem breakdown)
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

        # 10) Gráfico (barra única)
        x = df["k_clusters"].astype(int)
        plt.figure(figsize=(10, 6))

        plt.bar(x, df["custo_alvo"], label="Custo total (real + projetado)", color="steelblue")

        plt.ylabel("Custo consolidado (R$)", fontsize=12)
        plt.xlabel("Cenários", fontsize=12)
        plt.xticks(x, [_formatar_rotulo_cenario(k) for k in x])

        best_row = df.iloc[df["custo_alvo"].argmin()]
        best_k = int(best_row["k_clusters"])
        rr_txt = f"{best_row['regret_relativo']:.2%}" if best_row["regret_relativo"] is not None else "n/a"

        plt.title(
            f"Custo consolidado por k fixo (≥{int(min_cobertura_parcial*100)}% cobertura)\n"
            f"{data_inicial} → {data_final} • melhor cenário={_formatar_rotulo_cenario(best_k)} • regret={rr_txt}",
            fontsize=14, pad=20
        )
        plt.gca().yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{int(x/1000)} mil"))
        plt.legend()
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig(png_path, bbox_inches="tight")
        plt.close()

        print(f"✅ Cenários selecionados: {len(df)} (FULL={len(df[df['modo']=='F'])}, PARCIAIS={len(df[df['modo']=='P'])})")
        print(f"✅ Gráfico: {png_path}")
        print(f"✅ CSV:     {csv_path}")
        return png_path, csv_path, df_export

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Gera gráfico e CSV de custo consolidado por k fixo"
    )
    parser.add_argument("--tenant_id", required=True, type=str)
    parser.add_argument("--data_inicial", required=True, type=str)
    parser.add_argument("--data_final", required=True, type=str)
    parser.add_argument("--output_dir", required=True)
    parser.add_argument("--min_cobertura_parcial", default=0.70, type=float)

    args = parser.parse_args()

    gerar_grafico_k_fixo(
        tenant_id=args.tenant_id,
        data_inicial=args.data_inicial,
        data_final=args.data_final,
        output_dir=args.output_dir,
        min_cobertura_parcial=args.min_cobertura_parcial,
    )
