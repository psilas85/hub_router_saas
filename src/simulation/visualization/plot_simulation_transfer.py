#simulation/visualization/plot_simulation_transfer.py

import os
import folium
import json
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

from folium.plugins import MarkerCluster
from simulation.infrastructure.simulation_database_reader import (
    carregar_resumo_clusters,
    carregar_entregas_clusterizadas,
    carregar_rotas_transferencias,
    carregar_hubs
)

def plotar_mapa_transferencias(
    simulation_db,
    clusterization_db,
    tenant_id,
    envio_data,
    k_clusters,
    output_dir="output/maps",
    modo_forcar=False,
    logger=None
):
    output_path = f"{output_dir}/{tenant_id}"
    os.makedirs(output_path, exist_ok=True)
    mapa_path = f"{output_path}/{tenant_id}_mapa_transferencias_{envio_data}_k{k_clusters}.html"

    if os.path.exists(mapa_path) and not modo_forcar:
        return

    df_entregas = carregar_entregas_clusterizadas(simulation_db, clusterization_db, tenant_id, envio_data, k_clusters)
    df_clusters = carregar_resumo_clusters(simulation_db, tenant_id, envio_data, k_clusters)
    df_rotas = carregar_rotas_transferencias(simulation_db, tenant_id, envio_data, k_clusters)
    hubs = carregar_hubs(simulation_db, tenant_id)

    if df_entregas.empty or df_clusters.empty or df_rotas.empty or not hubs:
        raise ValueError("❌ Dados ausentes para plotagem: entregas, clusters, rotas ou hubs.")

    latitudes = [hub["latitude"] for hub in hubs]
    longitudes = [hub["longitude"] for hub in hubs]
    mapa = folium.Map(location=[sum(latitudes) / len(latitudes), sum(longitudes) / len(longitudes)], zoom_start=7)

    for hub in hubs:
        folium.Marker(
            location=[hub["latitude"], hub["longitude"]],
            popup=f"Hub: {hub['nome']}",
            icon=folium.Icon(color="red", icon="truck", prefix="fa")
        ).add_to(mapa)

    for _, row in df_clusters.iterrows():
        folium.Marker(
            location=[row["centro_lat"], row["centro_lon"]],
            popup=f"Cluster {row['cluster']} - {row['cluster_cidade']}",
            icon=folium.Icon(color="green", icon="building", prefix="fa")
        ).add_to(mapa)

    cores_disponiveis = list(mcolors.TABLEAU_COLORS.values())
    rota_color_map = {}

    for idx, row in df_rotas.iterrows():
        rota_id = row.get("rota_id", f"linha_{idx}")
        cor = cores_disponiveis[idx % len(cores_disponiveis)]
        rota_color_map[rota_id] = cor

        rota_raw = row["rota_completa_json"]
        if isinstance(rota_raw, str):
            rota = json.loads(rota_raw)
        else:
            rota = rota_raw

        if isinstance(rota[0], dict):
            coordenadas = [(p["lat"], p["lon"]) for p in rota]
        elif isinstance(rota[0], list) and len(rota[0]) == 2:
            coordenadas = [(p[0], p[1]) for p in rota]
        else:
            continue

        folium.PolyLine(
            locations=coordenadas,
            color=cor,
            weight=4,
            opacity=0.8,
            tooltip=f"Rota {rota_id}"
        ).add_to(mapa)

    marker_cluster = MarkerCluster().add_to(mapa)
    for _, row in df_entregas.iterrows():
        folium.CircleMarker(
            location=[row["centro_lat"], row["centro_lon"]],
            radius=3,
            color="gray",
            fill=True,
            fill_opacity=0.5,
            popup=row["cte_numero"]
        ).add_to(marker_cluster)

    mapa.save(mapa_path)

    # PNG estático
    png_path = mapa_path.replace(".html", ".png")
    plt.figure(figsize=(10, 8))
    for rota_id, cor in rota_color_map.items():
        df_rota = df_rotas[df_rotas["rota_id"] == rota_id]
        try:
            rota_raw = df_rota.iloc[0]["rota_completa_json"]
            if isinstance(rota_raw, str):
                rota = json.loads(rota_raw)
            else:
                rota = rota_raw
            coords = [(p["lon"], p["lat"]) if isinstance(p, dict) else (p[1], p[0]) for p in rota]
            xs, ys = zip(*coords)
            plt.plot(xs, ys, marker="o", color=cor, label=f"Rota {rota_id}")
        except:
            pass
    plt.scatter(df_clusters["centro_lon"], df_clusters["centro_lat"], c="black", marker="x", s=80, label="Centros")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_path, dpi=150)
    plt.close()
