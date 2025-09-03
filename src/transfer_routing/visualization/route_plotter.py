# transfer_routing/visualization/route_plotter.py

import os
import json
import folium
import pandas as pd
from folium import Element
from folium.plugins import MarkerCluster
from transfer_routing.infrastructure.database_connection import conectar_banco_routing, fechar_conexao
from transfer_routing.infrastructure.cache import obter_rota_do_cache


def decode_polyline(polyline_str):
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


def gerar_mapa(tenant_id, data_inicial, data_final=None, output_path=None):
    """
    Gera mapa de transferências para um tenant e período.
    Se data_final não for informado, assume igual a data_inicial.
    Se output_path for informado, salva o arquivo lá e retorna o caminho.
    """

    if data_final is None:
        data_final = data_inicial

    conn = conectar_banco_routing()
    query = """
        SELECT rota_transf, rota_coord, hub_central_latitude, hub_central_longitude
        FROM transferencias_resumo
        WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
    """
    df = pd.read_sql(query, conn, params=(tenant_id, data_inicial, data_final))
    fechar_conexao(conn)

    if df.empty:
        print("Nenhuma rota encontrada para os parâmetros informados.")
        return None

    def parse_rota(x):
        if isinstance(x, str):
            return json.loads(x)
        return x

    df["rota_coord"] = df["rota_coord"].apply(parse_rota)

    hub_lat = df.iloc[0]["hub_central_latitude"]
    hub_lon = df.iloc[0]["hub_central_longitude"]

    mapa = folium.Map(location=[hub_lat, hub_lon], zoom_start=7)
    marker_cluster = MarkerCluster().add_to(mapa)

    cores = [
        "red", "blue", "green", "purple", "orange", "darkred", "lightred",
        "beige", "darkblue", "darkgreen", "cadetblue", "darkpurple", "white",
        "pink", "lightblue", "lightgreen", "gray", "black", "lightgray"
    ]

    todos_pontos = []

    for idx, row in df.iterrows():
        rota_id = row["rota_transf"]
        rota_coords = row["rota_coord"]

        hub = (row["hub_central_latitude"], row["hub_central_longitude"])

        pontos = []
        for p in rota_coords:
            try:
                lat = float(p["lat"])
                lon = float(p["lon"])
                pontos.append((lat, lon))
            except (ValueError, KeyError, TypeError) as e:
                print(f"⚠️ Ponto inválido na rota {rota_id}: {p} ({e})")

        pontos_rota = [hub] + pontos + [hub]
        cor = cores[idx % len(cores)]
        todos_pontos.extend(pontos_rota)

        # Hub central
        folium.Marker(
            location=hub,
            icon=folium.Icon(color="red", icon="home"),
            tooltip="Hub Central"
        ).add_to(marker_cluster)

        # Clusters
        for p in pontos:
            folium.Marker(
                location=p,
                icon=folium.Icon(color="green", icon="truck"),
                tooltip=f"Cluster {p}"
            ).add_to(marker_cluster)

        desenhar_rota(tenant_id, pontos_rota, mapa, cor, rota_id)

    # 🔥 Legenda dinâmica
    legenda_html = """
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 220px;
        height: auto;
        z-index:9999;
        background-color:white;
        padding:10px;
        border:2px solid grey;
        border-radius:8px;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
    ">
    <h4 style="margin-top:0;">Legenda - Rotas</h4>
    """
    for idx, row in df.iterrows():
        cor = cores[idx % len(cores)]
        rota_id = row["rota_transf"]
        legenda_html += f"""<p style="margin:2px;">
            <span style='background-color:{cor};padding:3px 8px;border-radius:5px;'>&nbsp;</span>
            Rota {rota_id}
        </p>"""

    legenda_html += "</div>"
    mapa.get_root().html.add_child(Element(legenda_html))

    # 🔥 Zoom automático com margem
    if todos_pontos:
        latitudes = [p[0] for p in todos_pontos]
        longitudes = [p[1] for p in todos_pontos]

        sw = (min(latitudes) - 0.5, min(longitudes) - 0.5)
        ne = (max(latitudes) + 0.5, max(longitudes) + 0.5)
        mapa.fit_bounds([sw, ne])

    # Diretório de saída
    if output_path is None:
        output_path = f"output/maps/{tenant_id}"

    os.makedirs(output_path, exist_ok=True)
    nome_arquivo = os.path.join(
        output_path,
        f"mapa_transferencias_{data_inicial}_{data_final}.html"
    )

    mapa.save(nome_arquivo)
    print(f"✅ Mapa salvo em {nome_arquivo}")
    return nome_arquivo



def desenhar_rota(tenant_id, pontos, mapa, cor, rota_id):
    conn = conectar_banco_routing()

    for i in range(len(pontos) - 1):
        try:
            origem = (float(pontos[i][0]), float(pontos[i][1]))
            destino = (float(pontos[i + 1][0]), float(pontos[i + 1][1]))
        except (ValueError, TypeError) as e:
            print(f"⚠️ Coordenada inválida na rota {rota_id}, índice {i}: {pontos[i]} -> {pontos[i+1]} ({e})")
            continue

        origem_str = f"{round(origem[0], 6)},{round(origem[1], 6)}"
        destino_str = f"{round(destino[0], 6)},{round(destino[1], 6)}"

        distancia, tempo, rota_json = obter_rota_do_cache(origem_str, destino_str, conn)

        if rota_json:
            polyline = rota_json.get("overview_polyline")
            if polyline:
                pontos_polyline = decode_polyline(polyline)
                folium.PolyLine(
                    locations=pontos_polyline,
                    color=cor,
                    weight=3,
                    opacity=0.7,
                    tooltip=f"Rota {rota_id}"
                ).add_to(mapa)
            elif "coordenadas" in rota_json and rota_json["coordenadas"]:
                try:
                    pontos_polyline = []
                    for p in rota_json["coordenadas"]:
                        if isinstance(p, (list, tuple)) and len(p) >= 2:
                            pontos_polyline.append((float(p[0]), float(p[1])))
                        elif isinstance(p, dict) and "lat" in p and "lon" in p:
                            pontos_polyline.append((float(p["lat"]), float(p["lon"])))

                    if pontos_polyline:
                        folium.PolyLine(
                            locations=pontos_polyline,
                            color=cor,
                            weight=3,
                            opacity=0.7,
                            tooltip=f"Rota {rota_id}"
                        ).add_to(mapa)
                    else:
                        raise ValueError("Lista de coordenadas vazia")
                except Exception as e:
                    print(f"⚠️ Erro ao converter coordenadas de rota {rota_id}: {e}")
                    folium.PolyLine(
                        locations=[origem, destino],
                        color="gray",
                        weight=1,
                        opacity=0.5,
                        tooltip="Linha Reta"
                    ).add_to(mapa)
            else:
                folium.PolyLine(
                    locations=[origem, destino],
                    color="gray",
                    weight=1,
                    opacity=0.5,
                    tooltip="Linha Reta"
                ).add_to(mapa)
        else:
            folium.PolyLine(
                locations=[origem, destino],
                color="gray",
                weight=1,
                opacity=0.5,
                tooltip="Linha Reta"
            ).add_to(mapa)

    fechar_conexao(conn)
