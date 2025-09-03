# transfer_routing/visualization/mapa_estatico.py

import os
import json
import random
import matplotlib.pyplot as plt
import pandas as pd
from transfer_routing.infrastructure.database_connection import conectar_banco_routing, fechar_conexao
from transfer_routing.infrastructure.cache import obter_rota_do_cache


def gerar_cor():
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))


def decode_polyline(polyline_str):
    """Decodifica polyline codificada (Google/OSRM) para lista de coordenadas (lat, lon)."""
    index, lat, lng = 0, 0, 0
    coordinates = []
    while index < len(polyline_str):
        for coord in (lat, lng):
            shift, result = 0, 0
            while True:
                b = ord(polyline_str[index]) - 63
                index += 1
                result |= (b & 0x1f) << shift
                shift += 5
                if b < 0x20:
                    break
            dlat = ~(result >> 1) if result & 1 else (result >> 1)
            if coord is lat:
                lat += dlat
            else:
                lng += dlat
        coordinates.append((lat / 1e5, lng / 1e5))
    return coordinates


def plotar_rota_completa(ax, pontos, cor_linha, rota_id, conn):
    """Plota no Matplotlib a rota completa usando dados do cache (polyline ou coordenadas)."""
    for i in range(len(pontos) - 1):
        origem = pontos[i]
        destino = pontos[i + 1]

        origem_str = f"{round(origem[0], 6)},{round(origem[1], 6)}"
        destino_str = f"{round(destino[0], 6)},{round(destino[1], 6)}"

        distancia, tempo, rota_json = obter_rota_do_cache(origem_str, destino_str, conn)

        if rota_json:
            if "overview_polyline" in rota_json:
                coords = decode_polyline(rota_json["overview_polyline"])
            elif "coordenadas" in rota_json and rota_json["coordenadas"]:
                coords = rota_json["coordenadas"]
            else:
                coords = [origem, destino]
        else:
            coords = [origem, destino]

        if coords:
            lats, lons = zip(*coords)
            ax.plot(lons, lats, color=cor_linha, linewidth=1, alpha=0.7)


def gerar_mapa_estatico_transferencias(tenant_id: str, data_inicial: str, data_final: str = None, output_path: str = None):
    """
    Gera PNG estático com Matplotlib a partir das rotas de transferências usando dados do cache.
    Pode receber um intervalo (data_inicial até data_final). Se data_final não for informado, usa apenas data_inicial.
    Se output_path for informado, salva nesse diretório. Caso contrário, salva em output/maps/{tenant_id}.
    """
    if data_final is None:
        data_final = data_inicial

    conn = conectar_banco_routing()

    # Consulta resumo
    query_resumo = """
        SELECT
            rota_transf,
            hub_central_latitude,
            hub_central_longitude,
            rota_coord,
            envio_data
        FROM transferencias_resumo
        WHERE tenant_id = %s
        AND envio_data BETWEEN %s AND %s;
    """

    # Consulta detalhes
    query_detalhes = """
        SELECT
            rota_transf,
            cluster,
            centro_lat AS destino_latitude,
            centro_lon AS destino_longitude,
            envio_data
        FROM transferencias_detalhes
        WHERE tenant_id = %s
        AND envio_data BETWEEN %s AND %s;
    """

    df_resumo = pd.read_sql(query_resumo, conn, params=(tenant_id, data_inicial, data_final))
    df_detalhes = pd.read_sql(query_detalhes, conn, params=(tenant_id, data_inicial, data_final))

    if df_resumo.empty or df_detalhes.empty:
        fechar_conexao(conn)
        print(f"❌ Nenhuma transferência encontrada para {data_inicial} até {data_final}.")
        return None

    # Cores por cluster e rota
    cores_clusters = {c: gerar_cor() for c in df_detalhes["cluster"].unique()}
    cores_rotas = {r: gerar_cor() for r in df_resumo["rota_transf"].unique()}

    fig, ax = plt.subplots(figsize=(12, 8))

    for _, rota in df_resumo.iterrows():
        rota_id = rota["rota_transf"]
        cor_linha = cores_rotas[rota_id]

        sub = df_detalhes[df_detalhes["rota_transf"] == rota_id]

        rota_coord = rota["rota_coord"]
        if isinstance(rota_coord, list) and len(rota_coord) > 0:
            pontos = [(p["lat"], p["lon"]) for p in rota_coord if "lat" in p and "lon" in p]
            plotar_rota_completa(ax, pontos, cor_linha, rota_id, conn)

        # Pontos de destino
        ax.scatter(
            sub["destino_longitude"],
            sub["destino_latitude"],
            color=[cores_clusters[c] for c in sub["cluster"]],
            edgecolor="black",
            s=20,
            zorder=2
        )

        # HUB central
        ax.scatter(
            rota["hub_central_longitude"],
            rota["hub_central_latitude"],
            color="red",
            edgecolor="black",
            s=80,
            marker="o",
            zorder=3
        )

    fechar_conexao(conn)

    # Legenda
    for cluster, cor in cores_clusters.items():
        ax.scatter([], [], color=cor, label=f"Cluster {cluster}", edgecolor="black", s=50)

    ax.legend(title="Clusters", loc="upper right")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"Mapa de Transferências - {data_inicial} até {data_final}", fontsize=14)

    # Diretório de saída
    if output_path is None:
        output_path = f"output/maps/{tenant_id}"

    os.makedirs(output_path, exist_ok=True)
    caminho_png = os.path.join(output_path, f"mapa_transferencias_{data_inicial}_{data_final}.png")

    plt.savefig(caminho_png, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"✅ PNG gerado: {caminho_png}")
    return caminho_png
