# hub_router_1.0.1/src/simulation/visualization/plot_simulation_transfer.py

# hub_router_1.0.1/src/simulation/visualization/plot_simulation_transfer.py

import os
import json
import folium
import requests
import pandas as pd

from folium import GeoJson, Tooltip
from folium.plugins import Fullscreen
import geojson

from simulation.utils.path_builder import build_output_path

from simulation.infrastructure.simulation_database_reader import (
    carregar_rotas_transferencias,
    carregar_resumo_clusters,
    carregar_hubs
)

# ==============================
# CONFIG
# ==============================

OSRM_BASE_URL = f"http://{os.getenv('OSRM_HOST')}:{os.getenv('OSRM_PORT')}"


# ==============================
# UTILS
# ==============================

def parse_coords(coords):
    pontos = []

    if not isinstance(coords, list):
        return pontos

    for p in coords:
        try:
            if isinstance(p, dict):
                pontos.append((float(p["lat"]), float(p["lon"])))
            elif isinstance(p, (list, tuple)) and len(p) == 2:
                pontos.append((float(p[0]), float(p[1])))
        except:
            continue

    return pontos


def consultar_osrm(origem, destino):
    url = (
        f"{OSRM_BASE_URL}/route/v1/driving/"
        f"{origem[1]},{origem[0]};{destino[1]},{destino[0]}"
        f"?overview=full&geometries=geojson"
    )

    try:
        r = requests.get(url, timeout=10)
        data = r.json()

        if data.get("code") != "Ok":
            return []

        return [(lat, lon) for lon, lat in data["routes"][0]["geometry"]["coordinates"]]

    except:
        return []


def reconstruir_rota_osrm(hub, clusters):
    rota = []
    prev = hub

    for c in clusters:
        seg = consultar_osrm(prev, c)
        if seg:
            rota.extend(seg if not rota else seg[1:])
        prev = c

    seg = consultar_osrm(prev, hub)
    if seg:
        rota.extend(seg[1:])

    return rota


# ==============================
# MAIN
# ==============================

def plotar_mapa_transferencias(
    simulation_db,
    tenant_id,
    envio_data,
    k_clusters,
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
        f"{tenant_id}_mapa_transfer_{envio_data}_k{k_clusters}.html"
    )

    png_path = os.path.join(
        maps_dir,
        f"{tenant_id}_mapa_transfer_{envio_data}_k{k_clusters}.png"
    )

    # 🔥 controle overwrite
    if not modo_forcar and os.path.exists(mapa_path):
        if logger:
            logger.info(f"🟡 Mapa já existe: {mapa_path}")
        return

    # =============================
    # 🔹 dados
    # =============================
    df_rotas = carregar_rotas_transferencias(
        simulation_db, tenant_id, envio_data, k_clusters
    )

    df_clusters = carregar_resumo_clusters(
        simulation_db, tenant_id, envio_data, k_clusters
    )

    hubs = carregar_hubs(simulation_db, tenant_id)

    if df_clusters.empty or not hubs:
        if logger:
            logger.warning("⚠️ Dados insuficientes")
        return

    # 🔥 remove cluster HUB fake
    df_clusters = df_clusters[
        ~df_clusters["cluster"].astype(str).str.startswith("9999")
    ]

    hub = (
        float(hubs[0]["latitude"]),
        float(hubs[0]["longitude"])
    )

    # =============================
    # 🔹 mapa base
    # =============================
    all_lats = list(df_clusters["centro_lat"]) + [hub[0]]
    all_lons = list(df_clusters["centro_lon"]) + [hub[1]]

    mapa = folium.Map(
        location=[sum(all_lats)/len(all_lats), sum(all_lons)/len(all_lons)],
        zoom_start=10
    )

    Fullscreen().add_to(mapa)

    # HUB
    folium.Marker(
        location=hub,
        icon=folium.Icon(color="red", icon="home"),
        popup="Hub"
    ).add_to(mapa)

    # CLUSTERS
    for _, row in df_clusters.iterrows():
        folium.Marker(
            location=[row["centro_lat"], row["centro_lon"]],
            icon=folium.Icon(color="green"),
            popup=f"Cluster {row['cluster']}"
        ).add_to(mapa)

    cores = ["red", "blue", "green", "purple", "orange"]

    # =============================
    # 🔹 rotas
    # =============================
    rotas_ids = df_rotas["rota_id"].unique() if not df_rotas.empty else [None]

    for idx, rota_id in enumerate(rotas_ids):

        cor = cores[idx % len(cores)]
        rota_coords = []

        if rota_id:
            df_rota = df_rotas[df_rotas["rota_id"] == rota_id]

            try:
                raw = df_rota["rota_completa_json"].iloc[0]
                coords = json.loads(raw) if isinstance(raw, str) else raw
                rota_coords = parse_coords(coords)
            except:
                rota_coords = []

        # 🔥 fallback OSRM
        if len(rota_coords) <= 1:
            if logger:
                logger.warning(f"Fallback OSRM rota {rota_id}")

            clusters_coords = [
                (row["centro_lat"], row["centro_lon"])
                for _, row in df_clusters.iterrows()
            ]

            rota_coords = reconstruir_rota_osrm(hub, clusters_coords)

        # 🔥 fallback final
        if len(rota_coords) <= 1:
            rota_coords = [hub] + [
                (row["centro_lat"], row["centro_lon"])
                for _, row in df_clusters.iterrows()
            ] + [hub]

        # 🔹 desenhar linha
        gj = geojson.LineString([(lon, lat) for lat, lon in rota_coords])

        GeoJson(
            data=gj,
            style_function=lambda x, cor=cor: {
                "color": cor,
                "weight": 4
            },
            tooltip=Tooltip(f"Rota {rota_id}")
        ).add_to(mapa)

    mapa.save(mapa_path)

    if logger:
        logger.info(f"✅ Mapa transfer salvo: {mapa_path}")