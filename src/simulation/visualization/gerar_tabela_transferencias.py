#visualization/gerar_tabela_transferencias.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import math

def salvar_tabela_transferencias_png(df: pd.DataFrame, tenant_id: str, envio_data: str, k_clusters: int, output_dir="exports/simulation/tabelas_transferencias"):
    colunas = ["rota_id","tipo_veiculo","peso_total_kg","qde_volumes","distancia_total_km","tempo_total_min","qde_entregas"]
    df = df[colunas].sort_values(by="rota_id")
    df["peso_total_kg"] = df["peso_total_kg"].astype(float).round(2)

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

    imagens_salvas = []
    linhas_por_pagina = 30
    paginas = math.ceil(len(df) / linhas_por_pagina)

    for i in range(paginas):
        df_pagina = df.iloc[i * linhas_por_pagina:(i + 1) * linhas_por_pagina]
        fig, ax = plt.subplots(figsize=(12, 0.5 * len(df_pagina)))
        ax.axis('off')

        tabela = ax.table(cellText=df_pagina.values, colLabels=df_pagina.columns, cellLoc='center', loc='center')
        tabela.auto_set_font_size(False)
        tabela.set_fontsize(10)
        tabela.scale(1.0, 1.1)

        for (row, col), cell in tabela.get_celld().items():
            if row == 0:
                cell.set_fontsize(11)
                cell.set_text_props(weight='bold')
                cell.set_facecolor("#f2f2f2")
            elif df_pagina.iloc[row - 1, 0] == "TOTAL":
                cell.set_text_props(weight='bold')

        nome_arquivo = f"tabela_transferencias_k{k_clusters}_p{i+1}.png"
        caminho_arquivo = os.path.join(pasta_destino, nome_arquivo)
        fig.savefig(caminho_arquivo, dpi=300)
        plt.close(fig)

        imagens_salvas.append(caminho_arquivo)

    return imagens_salvas
