#clusterization/visualization/gerar_resumo_clusterizacao.py

import pandas as pd
import matplotlib.pyplot as plt
import os

BASE_OUTPUT_DIR = "/app/output"

def gerar_graficos_resumo_clusterizacao(df_resumo: pd.DataFrame, envio_data: str, tenant_id: str, output_path: str = None):
    if df_resumo.empty:
        raise ValueError("DataFrame de resumo da clusterização está vazio.")

    output_path = output_path or os.path.join(BASE_OUTPUT_DIR, "graphs", tenant_id)
    output_path = os.path.abspath(output_path)
    os.makedirs(output_path, exist_ok=True)

    df_resumo["peso_total_kg"] = df_resumo["peso_total_kg"].astype(float).round(2)
    df_resumo["quantidade_entregas"] = df_resumo["quantidade_entregas"].astype(int)
    df_resumo["quantidade_volumes"] = df_resumo["quantidade_volumes"].astype(int)

    # Gráfico 1
    plt.figure(figsize=(10, 5))
    df_resumo.sort_values('cluster').plot.bar(x='cluster', y='quantidade_entregas', legend=False, color='skyblue')
    plt.title("Quantidade de Entregas por Cluster")
    plt.xlabel("Cluster")
    plt.ylabel("Qtde Entregas")
    plt.tight_layout()
    caminho_1 = os.path.join(output_path, "grafico_qtde_entregas.png")
    plt.savefig(caminho_1)
    plt.close()

    # Gráfico 2
    plt.figure(figsize=(10, 5))
    df_resumo.sort_values('cluster').plot.bar(x='cluster', y='peso_total_kg', legend=False, color='salmon')
    plt.title("Peso Total (kg) por Cluster")
    plt.xlabel("Cluster")
    plt.ylabel("Peso (kg)")
    plt.tight_layout()
    caminho_2 = os.path.join(output_path, "grafico_peso_total.png")
    plt.savefig(caminho_2)
    plt.close()

    # Gráfico 3
    plt.figure(figsize=(10, 5))
    df_resumo.sort_values('cluster').plot.bar(x='cluster', y='quantidade_volumes', legend=False, color='lightgreen')
    plt.title("Quantidade de Volumes por Cluster")
    plt.xlabel("Cluster")
    plt.ylabel("Volumes")
    plt.tight_layout()
    caminho_3 = os.path.join(output_path, "grafico_volumes.png")
    plt.savefig(caminho_3)
    plt.close()

    print(f"✅ Gráficos salvos em:\n{caminho_1}\n{caminho_2}\n{caminho_3}")
    return [caminho_1, caminho_2, caminho_3]
