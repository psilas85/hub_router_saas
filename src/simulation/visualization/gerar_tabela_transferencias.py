#hub_router_1.0.1/src/simulation/visualization/gerar_tabela_transferencias.py

# hub_router_1.0.1/src/simulation/visualization/gerar_tabela_transferencias.py

import os
import math
import pandas as pd
import matplotlib.pyplot as plt

from simulation.utils.path_builder import build_output_path


def salvar_tabela_transferencias_png(
    df: pd.DataFrame,
    tenant_id: str,
    envio_data: str,
    k_clusters: int
):
    """
    Gera tabelas PNG paginadas de transferências.

    Saída:
    exports/simulation/{tenant_id}/{envio_data}/tables/
    """

    envio_data = str(envio_data)

    if df is None or df.empty:
        print("⚠️ DataFrame vazio - tabela de transferências não gerada")
        return []

    # 🔹 colunas esperadas
    colunas = [
        "rota_id",
        "tipo_veiculo",
        "peso_total_kg",
        "qde_volumes",
        "distancia_total_km",
        "tempo_total_min",
        "qde_entregas"
    ]

    # 🔹 garantir colunas existentes
    colunas_existentes = [c for c in colunas if c in df.columns]
    df = df[colunas_existentes].copy()

    # 🔹 tipagem segura
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
        df = df.sort_values(by="rota_id")

    # 🔹 TOTAL
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

    imagens = []

    linhas_por_pagina = 30
    paginas = math.ceil(len(df) / linhas_por_pagina)

    for i in range(paginas):

        df_pagina = df.iloc[
            i * linhas_por_pagina:(i + 1) * linhas_por_pagina
        ]

        altura = min(0.5 * len(df_pagina), 25)

        fig, ax = plt.subplots(figsize=(12, altura))
        ax.axis('off')

        tabela = ax.table(
            cellText=df_pagina.values,
            colLabels=df_pagina.columns,
            cellLoc='center',
            loc='center'
        )

        tabela.auto_set_font_size(False)
        tabela.set_fontsize(10)

        # 🔹 estilização
        total_row_index = len(df_pagina) - 1

        for (row, col), cell in tabela.get_celld().items():
            if row == 0:
                cell.set_text_props(weight='bold')
                cell.set_facecolor("#f2f2f2")
            elif row == total_row_index and df_pagina.iloc[row - 1, 0] == "TOTAL":
                cell.set_text_props(weight='bold')

        nome_arquivo = f"tabela_transferencias_k{k_clusters}_p{i+1}.png"

        caminho_arquivo = os.path.join(pasta_destino, nome_arquivo)

        plt.tight_layout()
        fig.savefig(caminho_arquivo, dpi=300)
        plt.close(fig)

        imagens.append(caminho_arquivo)

    print(f"✅ {len(imagens)} páginas de tabela de transferências geradas")

    return imagens