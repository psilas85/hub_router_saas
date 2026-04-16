#hub_router_1.0.1/src/simulation/visualization/plot_simulation_last_mile.py

# hub_router_1.0.1/src/simulation/visualization/plot_simulation_last_mile.py

import os
import json
import folium
import pandas as pd
import matplotlib.pyplot as plt

from folium.plugins import Fullscreen
from folium import FeatureGroup, GeoJson, Tooltip, Popup
import geojson

from simulation.utils.path_builder import build_output_path

from simulation.infrastructure.simulation_database_reader import (
    carregar_rotas_last_mile,
    carregar_resumo_clusters,
)


# 🔥 parse robusto
def parse_coords(coords):
    pontos = []

    if not isinstance(coords, list):
        return pontos

    for p in coords:
        try:
            if isinstance(p, dict) and "lat" in p and "lon" in p:
                pontos.append((float(p["lat"]), float(p["lon"])))

            elif isinstance(p, (list, tuple)) and len(p) == 2:
                pontos.append((float(p[0]), float(p[1])))

        except Exception:
            continue

    return pontos


def plotar_mapa_last_mile(
    simulation_db,
    clusterization_db,
    tenant_id: str,
    envio_data: str,
    k_clusters: int,
    simulation_id=None,
    modo_forcar=False,
    logger=None
):

    envio_data = str(envio_data)

    # 🔥 PADRÃO NOVO
    maps_dir = build_output_path(
        "exports/simulation",
        tenant_id,
        envio_data,
        "maps"
    )

    mapa_path = os.path.join(
        maps_dir,
        f"{tenant_id}_mapa_lastmile_{envio_data}_k{k_clusters}.html"
    )

    png_path = os.path.join(
        maps_dir,
        f"{tenant_id}_mapa_lastmile_{envio_data}_k{k_clusters}.png"
    )

    # 🔥 controle overwrite
    if not modo_forcar and (
        os.path.exists(mapa_path) or os.path.exists(png_path)
    ):
        if logger:
            logger.info(f"🟡 Mapa já existe: {mapa_path}")
        return

    # =============================
    # 🔹 dados
    # =============================
    df_rotas = carregar_rotas_last_mile(
        simulation_db,
        tenant_id,
        envio_data,
        k_clusters,
        simulation_id
    )

    if df_rotas.empty:
        if logger:
            logger.warning(f"⚠️ Sem dados last-mile (k={k_clusters})")
        return

    df_clusters = carregar_resumo_clusters(
        simulation_db,
        tenant_id,
        envio_data,
        k_clusters
    )

    # 🔥 filtro robusto
    df_rotas_validas = df_rotas[
        df_rotas["rota_id"].notna() &
        (df_rotas["rota_id"] != "") &
        (df_rotas["rota_id"] != "fallback_manual")
    ].copy()

    # =============================
    # 🔹 centro mapa
    # =============================
    all_lats = (
        list(df_rotas_validas["latitude"].dropna()) +
        list(df_clusters["centro_lat"].dropna())
    )

    all_lons = (
        list(df_rotas_validas["longitude"].dropna()) +
        list(df_clusters["centro_lon"].dropna())
    )

    if all_lats and all_lons:
        center_lat = sum(all_lats) / len(all_lats)
        center_lon = sum(all_lons) / len(all_lons)
    else:
        center_lat, center_lon = -23.55, -46.63

    mapa = folium.Map(location=[center_lat, center_lon], zoom_start=10)
    Fullscreen().add_to(mapa)

    # =============================
    # 🔹 centros
    # =============================
    for _, row in df_clusters.iterrows():
        folium.Marker(
            location=[row["centro_lat"], row["centro_lon"]],
            icon=folium.Icon(color="black", icon="home"),
            popup=f"Cluster {row['cluster']}"
        ).add_to(mapa)

    cores = [
        "red", "green", "blue", "orange",
        "purple", "darkred", "cadetblue"
    ]

    # =============================
    # 🔹 rotas
    # =============================
    for idx, rota_id in enumerate(df_rotas_validas["rota_id"].unique()):

        cor = cores[idx % len(cores)]

        df_rota = df_rotas_validas[
            df_rotas_validas["rota_id"] == rota_id
        ].sort_values("ordem_entrega")

        fg = FeatureGroup(name=f"Rota {rota_id}")

        # 🔹 linha
        try:
            raw_coords = df_rota["coordenadas_seq"].dropna().iloc[0]

            if isinstance(raw_coords, str):
                try:
                    coords = json.loads(raw_coords)
                except:
                    coords = []
            else:
                coords = raw_coords

            rota_coords = parse_coords(coords)

            if len(rota_coords) > 1:
                gj = geojson.LineString(
                    [(lon, lat) for lat, lon in rota_coords]
                )

                GeoJson(
                    data=gj,
                    style_function=lambda x, cor=cor: {
                        "color": cor,
                        "weight": 4
                    },
                    tooltip=Tooltip(f"Rota {rota_id}")
                ).add_to(fg)

        except Exception:
            pass

        # 🔹 pontos
        for _, row in df_rota.iterrows():
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])

                folium.CircleMarker(
                    location=[lat, lon],
                    radius=5,
                    color="black",
                    fill=True,
                    fill_color=cor,
                    fill_opacity=0.7,
                    popup=f"{rota_id} - {row.get('cte_numero','')}"
                ).add_to(fg)

            except:
                continue

        fg.add_to(mapa)

    folium.LayerControl().add_to(mapa)
    mapa.save(mapa_path)

    # =============================
    # 🔹 PNG
    # =============================
    plt.figure(figsize=(10, 8))

    for idx, rota_id in enumerate(df_rotas_validas["rota_id"].unique()):

        cor = cores[idx % len(cores)]

        df_rota = df_rotas_validas[
            df_rotas_validas["rota_id"] == rota_id
        ]

        try:
            raw_coords = df_rota["coordenadas_seq"].dropna().iloc[0]

            if isinstance(raw_coords, str):
                try:
                    coords = json.loads(raw_coords)
                except:
                    coords = []
            else:
                coords = raw_coords

            rota_coords = parse_coords(coords)

            if len(rota_coords) > 1:
                lats, lons = zip(*rota_coords)
                plt.plot(lons, lats, color=cor)

        except:
            pass

    plt.scatter(
        df_clusters["centro_lon"],
        df_clusters["centro_lat"],
        c="black"
    )

    plt.grid(True)
    plt.savefig(png_path, dpi=150)
    plt.close()

    if logger:
        logger.info(f"✅ Mapa last-mile salvo: {mapa_path}")