#visualization/gerar_tabela_lastmile.py

import os
import pandas as pd
import matplotlib.pyplot as plt

def salvar_tabela_lastmile_png(df: pd.DataFrame, tenant_id: str, envio_data: str, k_clusters: int, output_dir="exports/simulation/tabelas_lastmile"):
    colunas = ["rota_id","tipo_veiculo","peso_total_kg","qde_volumes","distancia_total_km","tempo_total_min","qde_entregas"]
    df = df[colunas].sort_values(by="rota_id")
    df["peso_total_kg"] = df["peso_total_kg"].astype(float).round(2)
    df["tempo_total_min"] = df["tempo_total_min"].astype(float).round(2)

    totais = {
        "rota_id": "TOTAL",
        "tipo_veiculo": "",
        "peso_total_kg": round(df["peso_total_kg"].sum(), 2),
        "qde_volumes": df["qde_volumes"].sum(),
        "distancia_total_km": "",
        "tempo_total_min": "",
        "qde_entregas": df["qde_entregas"].sum()
    }
    df = pd.concat([df, pd.DataFrame([totais])], ignore_index=True)

    pasta_destino = os.path.join(output_dir, tenant_id, envio_data)
    os.makedirs(pasta_destino, exist_ok=True)
    caminho_arquivo = os.path.join(pasta_destino, f"tabela_lastmile_k{k_clusters}.png")

    fig, ax = plt.subplots(figsize=(12, 0.6 * len(df)))
    ax.axis('off')
    tabela = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    tabela.auto_set_font_size(False)
    tabela.set_fontsize(11)
    tabela.scale(1.0, 1.2)

    total_row_index = len(df) - 1
    for (row, col), cell in tabela.get_celld().items():
        if row == 0:
            cell.set_fontsize(12)
            cell.set_text_props(weight='bold')
            cell.set_facecolor("#f2f2f2")
        elif row == total_row_index:
            cell.set_text_props(weight='bold')

    plt.tight_layout()
    fig.savefig(caminho_arquivo, dpi=300)
    plt.close(fig)

    return caminho_arquivo
