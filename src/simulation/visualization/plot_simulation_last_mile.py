#simulation/visualization/plot_simulation_last_mile.py
import os
import folium
import json
import pandas as pd
import matplotlib.pyplot as plt

from folium.plugins import Fullscreen
from simulation.infrastructure.simulation_database_reader import (
    carregar_rotas_last_mile,
    carregar_detalhes_last_mile,
    carregar_resumo_clusters,
    buscar_latlon_ctes
)


def plotar_mapa_last_mile(
    simulation_db,
    clusterization_db,
    tenant_id: str,
    envio_data: str,
    k_clusters: int,
    output_dir="output/maps",
    modo_forcar=False,
    logger=None
):
    output_path = os.path.join(output_dir, tenant_id)
    os.makedirs(output_path, exist_ok=True)
    mapa_path = os.path.join(output_path, f"{tenant_id}_mapa_lastmile_{envio_data}_k{k_clusters}.html")

    if os.path.exists(mapa_path) and not modo_forcar:
        if logger:
            logger.info(f"🟡 Mapa de last-mile já existe para {envio_data}, k={k_clusters}. Use --modo_forcar para sobrescrever.")
        return

    df_rotas = carregar_rotas_last_mile(simulation_db, tenant_id, envio_data, k_clusters)
    df_detalhes = carregar_detalhes_last_mile(simulation_db, clusterization_db, tenant_id, envio_data, k_clusters)
    df_clusters = carregar_resumo_clusters(simulation_db, tenant_id, envio_data, k_clusters)

    if df_rotas.empty or df_detalhes.empty:
        raise ValueError("❌ Dados ausentes para plotagem de last-mile.")

    lista_ctes = df_detalhes['cte_numero'].astype(str).unique().tolist()
    try:
        buscar_latlon_ctes(
            clusterization_db=clusterization_db,
            simulation_db=simulation_db,
            tenant_id=tenant_id,
            envio_data=envio_data,
            lista_ctes=lista_ctes,
            k_clusters=k_clusters,
            logger=logger
        )
    except:
        pass

    # === Paletas de cores ===
    # Folium aceita cores extras como "darkpurple", "lightred", etc.
    cores_folium = [
        "red", "green", "blue", "orange", "purple", "darkred", "cadetblue",
        "darkgreen", "pink", "lightblue", "lightgreen", "black", "beige",
        "darkpurple", "lightred", "gray", "darkblue", "lightgray"
    ]

    # Matplotlib aceita apenas nomes básicos e hex
    cores_matplotlib = [
        "red", "green", "blue", "orange", "purple",
        "#8B0000",  # darkred
        "#4682B4",  # cadetblue
        "#006400",  # darkgreen
        "pink", "cyan", "lime", "black", "beige",
        "#800080",  # darkpurple
        "#FF6347",  # lightred → Tomato
        "gray", "navy", "lightgray"
    ]

    rota_color_map = {}

    # === Mapa HTML com Folium ===
    mapa = folium.Map(location=[-3.85632, -38.498522], zoom_start=7)
    Fullscreen().add_to(mapa)

    # Centros
    for _, row in df_clusters.iterrows():
        folium.Marker(
            location=[row["centro_lat"], row["centro_lon"]],
            icon=folium.Icon(color="black", icon="home", prefix="fa"),
            popup=f"Cluster {row['cluster']}"
        ).add_to(mapa)

    # Rotas HTML
    for idx, rota_id in enumerate(df_rotas["rota_id"].unique()):
        cor = cores_folium[idx % len(cores_folium)]
        rota_color_map[rota_id] = cor
        df_rota = df_detalhes[df_detalhes["rota_id"] == rota_id].sort_values("ordem_entrega")

        coordenadas_seq = None
        serie_coords = df_rota["coordenadas_seq"].dropna()
        if not serie_coords.empty:
            coordenadas_seq = serie_coords.iloc[0]

        rota_coords = []
        if isinstance(coordenadas_seq, str):
            try:
                rota_coords = json.loads(coordenadas_seq)
            except:
                rota_coords = []
        elif isinstance(coordenadas_seq, (list, tuple)):
            rota_coords = coordenadas_seq

        if isinstance(rota_coords, list) and all(isinstance(p, dict) for p in rota_coords):
            rota_coords = [(p["lat"], p["lon"]) for p in rota_coords]

        if len(rota_coords) > 1:
            folium.PolyLine(
                locations=rota_coords,
                color=cor,
                weight=3,
                opacity=0.8,
                tooltip=rota_id
            ).add_to(mapa)

        for _, row in df_rota.iterrows():
            try:
                folium.CircleMarker(
                    location=[float(row["centro_lat"]), float(row["centro_lon"])],
                    radius=5,
                    color=cor,
                    fill=True,
                    fill_color=cor,
                    fill_opacity=0.6,
                    popup=f"Rota: {rota_id}"
                ).add_to(mapa)
            except:
                pass

    folium.LayerControl().add_to(mapa)
    mapa.save(mapa_path)

    # === PNG com Matplotlib ===
    png_path = mapa_path.replace(".html", ".png")
    plt.figure(figsize=(10, 8))

    for idx, (rota_id, _) in enumerate(rota_color_map.items()):
        cor = cores_matplotlib[idx % len(cores_matplotlib)]
        df_rota = df_detalhes[df_detalhes["rota_id"] == rota_id].sort_values("ordem_entrega")

        coordenadas_seq = None
        serie_coords = df_rota["coordenadas_seq"].dropna()
        if not serie_coords.empty:
            coordenadas_seq = serie_coords.iloc[0]

        rota_coords = []
        if isinstance(coordenadas_seq, str):
            try:
                rota_coords = json.loads(coordenadas_seq)
            except:
                rota_coords = []
        elif isinstance(coordenadas_seq, (list, tuple)):
            rota_coords = coordenadas_seq

        if isinstance(rota_coords, list) and all(isinstance(p, dict) for p in rota_coords):
            rota_coords = [(p["lat"], p["lon"]) for p in rota_coords]

        if len(rota_coords) > 1:
            lats, lons = zip(*rota_coords)
            plt.plot(lons, lats, marker="o", color=cor, linewidth=1.5, label=f"Rota {rota_id}")

    # Centros
    plt.scatter(df_clusters["centro_lon"], df_clusters["centro_lat"], c="black", marker="x", s=80, label="Centros")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_path, dpi=150)
    plt.close()
