#hub_router_1.0.1/src/simulation/visualization/gerar_tabela_lastmile.py

# hub_router_1.0.1/src/simulation/visualization/gerar_tabela_lastmile.py

import os
import pandas as pd
import matplotlib.pyplot as plt

from simulation.utils.path_builder import build_output_path


def salvar_tabela_lastmile_png(
    df: pd.DataFrame,
    tenant_id: str,
    envio_data: str,
    k_clusters: int
):
    """
    Gera tabela PNG de last-mile.

    Salva em:
    exports/simulation/{tenant_id}/{envio_data}/tables/
    """

    envio_data = str(envio_data)

    if df is None or df.empty:
        print("⚠️ DataFrame vazio - tabela lastmile não gerada")
        return None

    # 🔹 garante tipos numéricos
    numeric_cols = [
        "peso_total_kg",
        "qde_volumes",
        "distancia_total_km",
        "tempo_total_min",
        "qde_entregas"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # 🔹 ordenação
    if "rota_id" in df.columns:
        df = df.sort_values("rota_id")

    # 🔹 linha TOTAL
    totais = {}

    for col in df.columns:
        if col == "rota_id":
            totais[col] = "TOTAL"
        elif col in numeric_cols:
            totais[col] = df[col].sum()
        else:
            totais[col] = ""

    df = pd.concat([df, pd.DataFrame([totais])], ignore_index=True)

    # 🔹 path padrão
    pasta_destino = build_output_path(
        "exports/simulation",
        tenant_id,
        envio_data,
        "tables"
    )

    caminho = os.path.join(
        pasta_destino,
        f"tabela_lastmile_k{k_clusters}.png"
    )

    # 🔹 tamanho dinâmico com limite
    altura = min(0.6 * len(df), 25)

    fig, ax = plt.subplots(figsize=(12, altura))
    ax.axis('off')

    tabela = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='center',
        loc='center'
    )

    tabela.auto_set_font_size(False)
    tabela.set_fontsize(10)

    # 🔹 estilização
    total_row_index = len(df) - 1

    for (row, col), cell in tabela.get_celld().items():
        if row == 0:
            cell.set_text_props(weight='bold')
            cell.set_facecolor("#f2f2f2")
        elif row == total_row_index:
            cell.set_text_props(weight='bold')

    plt.tight_layout()
    fig.savefig(caminho, dpi=300)
    plt.close(fig)

    print(f"✅ Tabela last-mile salva: {caminho}")

    return caminho