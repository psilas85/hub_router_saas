#last_mile_routing/visualization/route_plotter.py

import os
import folium
import pandas as pd
import random
from folium import Map, CircleMarker, PolyLine
from branca.element import Template, MacroElement

from last_mile_routing.infrastructure.database_connection import (
    conectar_banco_routing,
    fechar_conexao
)


def gerar_cor():
    return "#{:06x}".format(random.randint(0, 0xFFFFFF))


def plotar_rotas(tenant_id, envio_data):
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
        return

    lat_centro = df_resumo["centro_lat"].mean()
    lon_centro = df_resumo["centro_lon"].mean()

    mapa = Map(location=[lat_centro, lon_centro], zoom_start=8)

    cores_clusters = {
        cluster: gerar_cor()
        for cluster in df_resumo["cluster"].unique()
    }

    cores_rotas = {
        rota_id: gerar_cor()
        for rota_id in df_resumo["rota_id"].unique()
    }

    print(f"🗺️ Plotando {len(df_resumo)} rotas para {envio_data}...")

    for _, rota in df_resumo.iterrows():
        rota_id = rota["rota_id"]
        cluster = rota["cluster"]
        cor_linha = cores_rotas[rota_id]
        cor_cluster = cores_clusters[cluster]

        sub = df_detalhes[df_detalhes["rota_id"] == rota_id].sort_values("ordem_entrega")

        coordenadas_seq = rota.get("rota_coord")
        if coordenadas_seq:
            coords = [(p["lat"], p["lon"]) for p in coordenadas_seq]
            PolyLine(
                locations=coords,
                color=cor_linha,
                weight=3,
                opacity=0.7
            ).add_to(mapa)
        else:
            print(f"⚠️ Atenção: rota {rota_id} não possui polyline salva no banco.")

        for _, entrega in sub.iterrows():
            CircleMarker(
                location=[entrega["destino_latitude"], entrega["destino_longitude"]],
                radius=3,
                color=cor_cluster,
                fill=True,
                fill_color='black',
                fill_opacity=1,
                weight=1
            ).add_to(mapa)

        CircleMarker(
            location=[rota["centro_lat"], rota["centro_lon"]],
            radius=6,
            color="red",
            fill=True,
            fill_color="red",
            fill_opacity=0.9,
            weight=2,
            popup=f"<b>Centro Cluster {cluster}</b><br>Rota: {rota_id}"
        ).add_to(mapa)

    # Legenda
    legenda_html = """
    {% macro html(this, kwargs) %}
    <div style="
        position: fixed;
        bottom: 50px;
        right: 50px;
        z-index:9999;
        background-color: white;
        padding: 10px;
        border:2px solid grey;
        border-radius:6px;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
        ">
    <h4 style="margin-top: 0;">Legenda - Clusters</h4>
    <ul style="list-style: none; padding: 0; margin: 0;">
    """
    for cluster, cor in cores_clusters.items():
        legenda_html += f"""
        <li>
            <span style="background:{cor};width:12px;height:12px;display:inline-block;
            border-radius:50%;margin-right:6px;border:1px solid black;"></span>
            Cluster {cluster}
        </li>
        """

    legenda_html += """
    </ul>
    </div>
    {% endmacro %}
    """

    legenda = MacroElement()
    legenda._template = Template(legenda_html)
    mapa.get_root().add_child(legenda)


    output_folder = f"exports/last_mile_routing/maps/{tenant_id}"
    os.makedirs(output_folder, exist_ok=True)

    output_path = f"{output_folder}/mapa_rotas_{envio_data}.html"
    mapa.save(output_path)

    print(f"✅ Mapa salvo em {output_path}")

