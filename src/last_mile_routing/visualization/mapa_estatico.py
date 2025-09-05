# last_mile_routing/visualization/mapa_estatico.py

import os
import matplotlib.pyplot as plt
import pandas as pd
import random

from last_mile_routing.infrastructure.database_connection import (
    conectar_banco_routing,
    fechar_conexao
)


def gerar_cor():
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))


def gerar_mapa_estatico(tenant_id: str, envio_data: str):
    """
    Gera PNG estático com Matplotlib a partir das rotas do last mile,
    salvando em /app/exports/maps/{tenant_id}.
    """
    conn = conectar_banco_routing()

    query_resumo = """
        SELECT *
        FROM last_mile_rotas
        WHERE tenant_id = %s
        AND envio_data = %s;
    """

    query_detalhes = """
        SELECT *
        FROM detalhes_rotas
        WHERE tenant_id = %s
        AND envio_data = %s;
    """

    df_resumo = pd.read_sql(query_resumo, conn, params=(tenant_id, envio_data))
    df_detalhes = pd.read_sql(query_detalhes, conn, params=(tenant_id, envio_data))

    fechar_conexao(conn)

    if df_resumo.empty or df_detalhes.empty:
        print(f"❌ Nenhuma rota encontrada para {envio_data}.")
        return None

    # Definir cores
    cores_clusters = {c: gerar_cor() for c in df_resumo["cluster"].unique()}
    cores_rotas = {r: gerar_cor() for r in df_resumo["rota_id"].unique()}

    # Criar figura
    fig, ax = plt.subplots(figsize=(12, 8))

    # Plotar rotas e pontos
    for _, rota in df_resumo.iterrows():
        rota_id = rota["rota_id"]
        cluster = rota["cluster"]
        cor_linha = cores_rotas[rota_id]
        cor_cluster = cores_clusters[cluster]

        sub = df_detalhes[df_detalhes["rota_id"] == rota_id].sort_values("ordem_entrega")

        # Linha da rota (se disponível)
        if isinstance(rota.get("rota_coord"), list):
            coords = [(p["lat"], p["lon"]) for p in rota["rota_coord"]]
            if coords:
                lats, lons = zip(*coords)
                ax.plot(lons, lats, color=cor_linha, linewidth=1, alpha=0.7)

        # Pontos de entrega
        ax.scatter(
            sub["destino_longitude"],
            sub["destino_latitude"],
            color=cor_cluster,
            edgecolor="black",
            s=20,
            zorder=2
        )

        # Centro do cluster
        ax.scatter(
            rota["centro_lon"],
            rota["centro_lat"],
            color="red",
            edgecolor="black",
            s=80,
            marker="o",
            zorder=3
        )

    # Legenda
    for cluster, cor in cores_clusters.items():
        ax.scatter([], [], color=cor, label=f"Cluster {cluster}", edgecolor="black", s=50)

    ax.legend(title="Clusters", loc="upper right")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"Mapa de Rotas - {envio_data}", fontsize=14)

    # Salvar

    # 🔧 Corrigido para /app/exports
    output_folder = f"/app/exports/last_mile_routing/maps/{tenant_id}"
    os.makedirs(output_folder, exist_ok=True)
    caminho_png = f"{output_folder}/mapa_rotas_{envio_data}.png"
    plt.savefig(caminho_png, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"✅ PNG gerado: {caminho_png}")
    return caminho_png

