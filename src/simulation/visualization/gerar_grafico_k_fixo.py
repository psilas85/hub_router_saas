# simulation/visualization/gerar_grafico_k_fixo.py
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import matplotlib.ticker as mtick

matplotlib.use("Agg")

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db


def gerar_grafico_k_fixo(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    output_dir: str = "exports/simulation/graphs",
    cobertura_min: float = 1.0,              # mantido para forçar 100% se quiser
    usar_media_em_vez_de_soma: bool = False, # (apenas para modo full)
    min_cenarios_full: int = 5,              # precisa de 5 ks com 100% para usar o modo full
    min_cobertura_parcial: float = 0.70      # fallback: cobertura mínima de 70%
):
    """
    Avalia k_clusters 'fixos' no período, seguindo as regras:
      1) Preferir cenários com 100% de cobertura (se houver >= min_cenarios_full).
      2) Caso contrário, usar cenários com cobertura >= min_cobertura_parcial
         e custo ajustado por regra de 3 para o total de dias.
    Também calcula benchmark do 'ótimo diário' e regret.
    """

    os.makedirs(os.path.join(output_dir, tenant_id), exist_ok=True)
    png_path = os.path.join(output_dir, tenant_id, f"k_fixo_{data_inicial}_{data_final}.png")
    csv_path = os.path.join(output_dir, tenant_id, f"k_fixo_{data_inicial}_{data_final}.csv")

    conn = conectar_simulation_db()
    try:
        # 1) Total de dias do período (para cobertura/diagnóstico)
        q_tot_dias = """
            SELECT COUNT(DISTINCT envio_data) AS total_dias
            FROM resultados_simulacao
            WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
        """
        tot = pd.read_sql(q_tot_dias, conn, params=(tenant_id, data_inicial, data_final))
        total_dias = int(tot["total_dias"].iat[0] or 0)
        if total_dias == 0:
            print("⚠️ Não há dados no período informado.")
            return None, None, pd.DataFrame()

        # 2) Cobertura por k (N dias presentes / total)
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
            q_cobertura, conn,
            params=(tenant_id, data_inicial, data_final,
                    tenant_id, data_inicial, data_final)
        )
        if cov.empty:
            print("⚠️ Não há cenários k no período.")
            return None, None, pd.DataFrame()

        # 3) Seleção de modo (FULL vs PARCIAL)
        ks_full = cov.loc[cov["cobertura_pct"] >= 0.999999, "k_clusters"].astype(int).tolist()
        modo = "FULL" if len(ks_full) >= min_cenarios_full else "PARCIAL"

        if modo == "FULL":
            k_list = ks_full
        else:
            # fallback: requer pelo menos 1 k com cobertura >= min_cobertura_parcial
            k_list = cov.loc[cov["cobertura_pct"] >= min_cobertura_parcial, "k_clusters"].astype(int).tolist()
            if not k_list:
                melhor = cov.sort_values("cobertura_pct", ascending=False).head(10).copy()
                melhor["cobertura_pct"] = melhor["cobertura_pct"].round(4)
                print("❌ ERRO: Nenhum cenário k_clusters com cobertura ≥ "
                      f"{min_cobertura_parcial:.0%} no período.")
                print("🔎 Top coberturas observadas:")
                print(melhor.to_string(index=False))
                return None, None, pd.DataFrame()

        # 4) Agregados por k (soma/média e contagem de dias)
        q_agregado = """
            SELECT
              k_clusters,
              SUM(custo_total)            AS soma_custo_total,
              AVG(custo_total)            AS media_custo_total,
              SUM(custo_transferencia)    AS soma_custo_transfer,
              SUM(custo_last_mile)        AS soma_custo_last_mile,
              COUNT(*)                    AS qtd_dias
            FROM resultados_simulacao
            WHERE tenant_id = %s
              AND envio_data BETWEEN %s AND %s
              AND k_clusters = ANY(%s)
            GROUP BY k_clusters
        """
        df = pd.read_sql(q_agregado, conn, params=(tenant_id, data_inicial, data_final, k_list))

        # 5) Benchmark ótimo diário e regret (sempre contra o ótimo por dia)
        q_otimo = """
            SELECT SUM(min_custo) AS soma_otimo_diario
            FROM (
              SELECT envio_data, MIN(custo_total) AS min_custo
              FROM resultados_simulacao
              WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
              GROUP BY envio_data
            ) x
        """
        bench = pd.read_sql(q_otimo, conn, params=(tenant_id, data_inicial, data_final))
        soma_otimo = float(bench["soma_otimo_diario"].iat[0] or 0.0)

        # 6) Merge com cobertura
        df = df.merge(
            cov[["k_clusters", "dias_presentes", "total_dias", "cobertura_pct"]],
            on="k_clusters", how="left"
        )

        # 7) Custo alvo para ordenação e gráfico
        if modo == "FULL":
            # usa custo total real (ou média se solicitado)
            df["custo_alvo"] = df["media_custo_total"] if usar_media_em_vez_de_soma else df["soma_custo_total"]
            criterio_txt = "MÉDIO" if usar_media_em_vez_de_soma else "TOTAL"
        else:
            # custo ajustado por regra de 3
            df["custo_ajustado"] = (df["soma_custo_total"] / df["dias_presentes"]) * total_dias
            df["custo_alvo"] = df["custo_ajustado"]
            criterio_txt = "AJUSTADO"

        # 8) Regret (sempre comparado ao ótimo diário)
        df["soma_otimo_diario"] = soma_otimo
        df["regret_absoluto"] = df["custo_alvo"] - soma_otimo
        df["regret_relativo"] = df.apply(
            lambda r: (float(r["custo_alvo"]) / soma_otimo - 1.0) if soma_otimo else None,
            axis=1
        )

        # 9) Ordenação + CSV
        df = df.sort_values(["custo_alvo", "k_clusters"]).reset_index(drop=True)
        df.to_csv(csv_path, index=False)

        # 10) Gráfico
        x = df["k_clusters"].astype(int)

        plt.figure(figsize=(10, 6))

        # Barras empilhadas
        plt.bar(x, df["soma_custo_last_mile"], label="Last-mile + Cluster", color="steelblue")
        plt.bar(x, df["soma_custo_transfer"],
                bottom=df["soma_custo_last_mile"], label="Transferência", color="gray")

        # Eixos e título
        plt.ylabel(f"Custo {criterio_txt} no período (R$)", fontsize=12)
        plt.xlabel("Clusters (k)", fontsize=12)

        best_row = df.iloc[0]
        best_k = int(best_row["k_clusters"])
        rr = best_row["regret_relativo"]
        rr_txt = f"{rr:.2%}" if rr is not None else "n/a"

        titulo_modo = "FULL (100%)" if modo == "FULL" else f"PARCIAL (≥{int(min_cobertura_parcial*100)}%)"
        plt.title(
            f"Custo consolidado por k fixo {titulo_modo}\n"
            f"{data_inicial} → {data_final} • melhor k={best_k} • regret={rr_txt}",
            fontsize=14, pad=20
        )

        # Formatar Y em mil
        plt.gca().yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{int(x/1000)} mil"))

        # Legenda
        plt.legend()

        # Grid discreto
        plt.grid(axis="y", linestyle="--", alpha=0.7)

        # Ajuste layout para evitar sobreposição
        plt.tight_layout(rect=[0, 0, 1, 0.95])

        plt.savefig(png_path, bbox_inches="tight")
        plt.close()

        print(f"✅ Modo: {modo} | Critério: {criterio_txt}")
        print(f"✅ Gráfico: {png_path}")
        print(f"✅ CSV:     {csv_path}")

        return png_path, csv_path, df

    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Gera gráfico para k fixo no período.")
    parser.add_argument("--tenant_id", required=True, type=str)
    parser.add_argument("--data_inicial", required=True, type=str)   # YYYY-MM-DD
    parser.add_argument("--data_final", required=True, type=str)     # YYYY-MM-DD
    parser.add_argument("--output_dir", default="exports/simulation/graphs")
    parser.add_argument("--cobertura_min", default=1.0, type=float)  # compatibilidade
    parser.add_argument("--usar_media", action="store_true")
    parser.add_argument("--min_cenarios_full", default=5, type=int)
    parser.add_argument("--min_cobertura_parcial", default=0.70, type=float)

    args = parser.parse_args()

    png, csv, df = gerar_grafico_k_fixo(
        tenant_id=args.tenant_id,
        data_inicial=args.data_inicial,
        data_final=args.data_final,
        output_dir=args.output_dir,
        cobertura_min=args.cobertura_min,
        usar_media_em_vez_de_soma=args.usar_media,
        min_cenarios_full=args.min_cenarios_full,
        min_cobertura_parcial=args.min_cobertura_parcial
    )
    print("PNG:", png)
    print("CSV:", csv)
    print("Linhas:", 0 if df is None else len(df))
