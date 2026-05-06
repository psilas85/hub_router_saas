#hub_router_1.0.1/src/simulation/visualization/gerar_grafico_distribuicao_k.py

# hub_router_1.0.1/src/simulation/visualization/gerar_grafico_distribuicao_k.py

import os
import pandas as pd
import matplotlib.pyplot as plt

from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.utils.path_builder import build_output_path


DIAS_SEMANA_PT = {
    0: "Seg",
    1: "Ter",
    2: "Qua",
    3: "Qui",
    4: "Sex",
    5: "Sab",
    6: "Dom",
}


def _formatar_rotulo_cenario(k: int) -> str:
    return "Hub unico" if int(k) == 0 else str(int(k))


def gerar_grafico_distribuicao_k(
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    base_dir: str = "exports/simulation",
    modo_forcar: bool = False
):
    """
    Gera gráfico de barras com a frequência de k_clusters eleitos ponto ótimo
    no período informado.
    """

    conn = conectar_simulation_db()

    query = """
        SELECT envio_data, k_clusters
        FROM resultados_simulacao
        WHERE tenant_id = %s
          AND envio_data BETWEEN %s AND %s
          AND is_ponto_otimo = TRUE
        ORDER BY envio_data, k_clusters
    """

    df = pd.read_sql(query, conn, params=(tenant_id, data_inicial, data_final))
    conn.close()

    if df.empty:
        print("⚠️ Sem dados para gráfico de distribuição de k")
        return None, None, None

    df["envio_data"] = pd.to_datetime(df["envio_data"])
    df["k_clusters"] = df["k_clusters"].astype(int)

    df_distribuicao = (
        df.groupby("k_clusters", as_index=False)
        .size()
        .rename(columns={"size": "qtd"})
        .sort_values("k_clusters")
        .reset_index(drop=True)
    )

    df["dia_semana_ordem"] = df["envio_data"].dt.dayofweek
    df["dia_semana"] = df["dia_semana_ordem"].map(DIAS_SEMANA_PT)

    cenarios = [int(k) for k in df_distribuicao["k_clusters"].tolist()]
    dias_semana = []

    for ordem, rotulo in DIAS_SEMANA_PT.items():
        df_dia = df[df["dia_semana_ordem"] == ordem]
        contagem_por_k = df_dia["k_clusters"].value_counts().to_dict()
        contagens = [
            {"k_clusters": k, "qtd": int(contagem_por_k.get(k, 0))}
            for k in cenarios
        ]
        dias_semana.append(
            {
                "dia_semana_ordem": ordem,
                "dia_semana": rotulo,
                "total": int(sum(item["qtd"] for item in contagens)),
                "contagens": contagens,
            }
        )

    # 🔥 PADRÃO NOVO (sem gambiarra de path)
    graphs_dir = build_output_path(
        base_dir,
        tenant_id,
        f"{data_inicial}_{data_final}",
        "graphs"
    )

    filename = os.path.join(
        graphs_dir,
        f"distribuicao_k_{data_inicial}_{data_final}.png"
    )

    # 🔥 controle de sobrescrita
    if not modo_forcar and os.path.exists(filename):
        print(f"🟡 Arquivo já existe: {filename}")
        return filename, df_distribuicao.to_dict(orient="records"), dias_semana

    # 🎨 estilo
    plt.style.use("default")

    plt.figure(figsize=(8, 6))

    bars = plt.bar(
        df_distribuicao["k_clusters"],
        df_distribuicao["qtd"],
        edgecolor="black"
    )

    # 🔹 valores nas barras
    for bar in bars:
        yval = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width()/2,
            yval + 0.1,
            str(int(yval)),
            ha='center',
            va='bottom',
            fontsize=10,
            fontweight='bold'
        )

    plt.xticks(
        df_distribuicao["k_clusters"],
        [_formatar_rotulo_cenario(k) for k in df_distribuicao["k_clusters"]]
    )

    plt.xlabel("Cenários", fontsize=12, fontweight="bold")
    plt.ylabel("Frequência como Ponto Ótimo", fontsize=12, fontweight="bold")

    plt.title(
        f"Distribuição de cenários vencedores ({data_inicial} → {data_final})",
        fontsize=14,
        fontweight="bold"
    )

    plt.grid(axis="y", linestyle="--", alpha=0.6)

    plt.tight_layout()
    plt.savefig(filename, bbox_inches="tight", dpi=120)
    plt.close()

    print(f"✅ Gráfico salvo: {filename}")

    return filename, df_distribuicao.to_dict(orient="records"), dias_semana