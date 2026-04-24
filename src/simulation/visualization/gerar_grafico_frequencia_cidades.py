#hub_router_1.0.1/src/simulation/visualization/gerar_grafico_frequencia_cidades.py

# hub_router_1.0.1/src/simulation/visualization/gerar_grafico_frequencia_cidades.py

import os
import pandas as pd
import matplotlib.pyplot as plt

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.utils.path_builder import build_output_path


def gerar_grafico_frequencia_cidades(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    base_dir: str = "exports/simulation",
    modo_forcar: bool = False
):
    """
    Gera gráfico de barras com a frequência das cidades centro (cluster_cidade)
    que aparecem em simulações ponto ótimo no período informado.

    - Contagem por DIA (DISTINCT envio_data)
    - Evita inflar cidades no cenário hub único
    """

    conn = conectar_simulation_db()

    query_contexto = """
        SELECT
            COUNT(DISTINCT r.envio_data) AS total_dias_otimos,
            COUNT(DISTINCT CASE WHEN r.k_clusters = 0 THEN r.envio_data END) AS dias_hub_unico,
            COUNT(DISTINCT CASE WHEN r.k_clusters <> 0 THEN r.envio_data END) AS dias_clusterizados
        FROM resultados_simulacao r
        WHERE r.tenant_id = %s
          AND r.envio_data BETWEEN %s AND %s
          AND r.is_ponto_otimo = TRUE
    """

    df_contexto = pd.read_sql(query_contexto, conn, params=(tenant_id, data_inicial, data_final))
    total_dias_otimos = int(df_contexto["total_dias_otimos"].iat[0] or 0)
    dias_hub_unico = int(df_contexto["dias_hub_unico"].iat[0] or 0)
    dias_clusterizados = int(df_contexto["dias_clusterizados"].iat[0] or 0)
    somente_hub_unico = total_dias_otimos > 0 and dias_hub_unico == total_dias_otimos

    query = """
        SELECT ec.cluster_cidade, COUNT(DISTINCT ec.envio_data) AS qtd
        FROM entregas_clusterizadas ec
        JOIN resultados_simulacao r
          ON ec.simulation_id = r.simulation_id
         AND ec.k_clusters = r.k_clusters
         AND ec.tenant_id = r.tenant_id
         AND ec.envio_data = r.envio_data
        WHERE ec.tenant_id = %s
          AND ec.envio_data BETWEEN %s AND %s
          AND r.is_ponto_otimo = TRUE
        GROUP BY ec.cluster_cidade
        ORDER BY qtd DESC
        LIMIT 30
    """

    df = pd.read_sql(query, conn, params=(tenant_id, data_inicial, data_final))
    conn.close()

    hub_cidade = None
    if somente_hub_unico and not df.empty:
        hub_cidade = str(df.iloc[0]["cluster_cidade"])

    contexto = {
        "total_dias_otimos": total_dias_otimos,
        "dias_hub_unico": dias_hub_unico,
        "dias_clusterizados": dias_clusterizados,
        "somente_hub_unico": somente_hub_unico,
        "hub_cidade": hub_cidade,
    }

    if df.empty:
        return {
            "status": "vazio",
            "data_inicial": data_inicial,
            "data_final": data_final,
            "grafico": None,
            "csv": None,
            "dados": [],
            "contexto": contexto,
        }

    # 🔥 PADRÃO NOVO
    periodo = f"{data_inicial}_{data_final}"

    graphs_dir = build_output_path(
        base_dir,
        tenant_id,
        periodo,
        "graphs"
    )

    filename_png = os.path.join(
        graphs_dir,
        f"frequencia_cidades_{periodo}.png"
    )

    filename_csv = os.path.join(
        graphs_dir,
        f"frequencia_cidades_{periodo}.csv"
    )

    # 🔥 controle de sobrescrita
    if not modo_forcar and not somente_hub_unico and os.path.exists(filename_png):
        return {
            "status": "ok",
            "data_inicial": data_inicial,
            "data_final": data_final,
            "grafico": filename_png,
            "csv": filename_csv if os.path.exists(filename_csv) else None,
            "dados": df.to_dict(orient="records"),
            "contexto": contexto,
        }

    # =========================
    # 🔹 GRÁFICO
    # =========================

    plt.figure(figsize=(10, 6))

    plt.barh(
        df["cluster_cidade"],
        df["qtd"]
    )

    if somente_hub_unico:
        plt.xlabel("Frequência de dias como Hub vencedor")
        plt.ylabel("Hub")
        plt.title(
            f"Hub Vencedor no Período ({data_inicial} → {data_final})"
        )
    else:
        plt.xlabel("Frequência de dias como Centro (ponto ótimo)")
        plt.ylabel("Cidade")
        plt.title(
            f"Frequência de Cidades em Pontos Ótimos ({data_inicial} → {data_final})"
        )

    plt.gca().invert_yaxis()

    plt.grid(axis="x", linestyle="--", alpha=0.7)

    plt.tight_layout()
    plt.savefig(filename_png, bbox_inches="tight")
    plt.close()

    # =========================
    # 🔹 CSV
    # =========================

    df.to_csv(filename_csv, index=False, encoding="utf-8-sig")

    print(f"✅ Gráfico salvo: {filename_png}")
    print(f"✅ CSV salvo: {filename_csv}")

    return {
        "status": "ok",
        "data_inicial": data_inicial,
        "data_final": data_final,
        "grafico": filename_png,
        "csv": filename_csv,
        "dados": df.to_dict(orient="records"),
        "contexto": contexto,
    }