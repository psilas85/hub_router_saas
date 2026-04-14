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

from simulation.infrastructure.simulation_database_reader import (
    carregar_rotas_last_mile,
    carregar_resumo_clusters,
)


# 🔥 FUNÇÃO CENTRAL (CORREÇÃO DEFINITIVA)
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
    output_dir="exports/simulation/maps",
    modo_forcar=False,
    logger=None
):

    os.makedirs(output_dir, exist_ok=True)

    mapa_path = os.path.join(
        output_dir,
        f"{tenant_id}_mapa_lastmile_{envio_data}_k{k_clusters}.html"
    )
    png_path = mapa_path.replace(".html", ".png")

    # overwrite
    if modo_forcar:
        for path in [mapa_path, png_path]:
            if path and os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass
    elif mapa_path and os.path.exists(mapa_path):
        if logger:
            logger.info(f"🟡 Mapa já existe: {mapa_path}")
        return

    # 🔥 carregar dados
    df_rotas = carregar_rotas_last_mile(
        simulation_db,
        tenant_id,
        envio_data,
        k_clusters,
        simulation_id
    )

    # 🔥 FILTRO CORRETO (SEM DEPENDER DE FLAG)
    df_rotas_validas = df_rotas[
        df_rotas["rota_id"].notna() &
        (df_rotas["rota_id"] != "")
    ].copy()

    # 🔥 DEBUG CRÍTICO — NÃO PODE TER ROTA NULL
    print("📊 TOTAL ROTAS:", len(df_rotas))
    print("📊 VALIDAS:", len(df_rotas_validas))

    removidas = df_rotas[~df_rotas.index.isin(df_rotas_validas.index)]

    if not removidas.empty:
        print("🚨 LINHAS REMOVIDAS DO PLOT:")
        print(removidas[["cte_numero", "rota_id", "entrega_com_rota"]].head(10))



    df_clusters = carregar_resumo_clusters(
        simulation_db, tenant_id, envio_data, k_clusters
    )

    if df_rotas.empty:
        if logger:
            logger.warning(f"⚠️ Sem dados de rotas para plot (k={k_clusters})")
        return

    # 🎯 centro do mapa
    all_lats = list(df_rotas_validas["latitude"].dropna()) + list(df_clusters["centro_lat"].dropna())
    all_lons = list(df_rotas_validas["longitude"].dropna()) + list(df_clusters["centro_lon"].dropna())

    if all_lats and all_lons:
        center_lat = sum(all_lats) / len(all_lats)
        center_lon = sum(all_lons) / len(all_lons)
    else:
        center_lat, center_lon = -23.55, -46.63

    mapa = folium.Map(location=[center_lat, center_lon], zoom_start=10)
    Fullscreen().add_to(mapa)

    # 🎯 centros
    for _, row in df_clusters.iterrows():
        folium.Marker(
            location=[row["centro_lat"], row["centro_lon"]],
            icon=folium.Icon(color="black", icon="home", prefix="fa"),
            popup=f"Cluster {row['cluster']}"
        ).add_to(mapa)

    cores = [
        "red", "green", "blue", "orange", "purple",
        "darkred", "cadetblue", "darkgreen", "black"
    ]

    # 🔥 rotas
    for idx, rota_id in enumerate(df_rotas_validas["rota_id"].unique()):

        cor = cores[idx % len(cores)]

        df_rota = df_rotas_validas[
            df_rotas_validas["rota_id"] == rota_id
        ].sort_values("ordem_entrega")

        fg = FeatureGroup(name=f"Rota {rota_id}")

        coordenadas_seq = df_rota["coordenadas_seq"].dropna()

        if not coordenadas_seq.empty:
            try:
                raw_coords = coordenadas_seq.iloc[0]

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
                        [(float(lon), float(lat)) for lat, lon in rota_coords]
                    )

                    GeoJson(
                        data=gj,
                        style_function=lambda x, cor=cor: {
                            "color": cor,
                            "weight": 4,
                            "opacity": 0.8
                        },
                        highlight_function=lambda x: {
                            "color": "yellow",
                            "weight": 6
                        },
                        tooltip=Tooltip(f"Rota {rota_id}"),
                        popup=Popup(f"Rota {rota_id}")
                    ).add_to(fg)

            except Exception as e:
                if logger:
                    logger.warning(f"Erro coordenadas_seq rota {rota_id}: {e}")

        # 🔥 pontos das entregas
        for _, row in df_rota.iterrows():
            try:
                lat = row.get("latitude")
                lon = row.get("longitude")

                if pd.isna(lat) or pd.isna(lon):
                    continue

                lat = float(lat)
                lon = float(lon)

                popup_html = f"""
                <b>Rota:</b> {rota_id}<br>
                <b>CTE:</b> {row.get('cte_numero','')}<br>
                <b>Ordem:</b> {row.get('ordem_entrega','')}<br>
                <b>Peso:</b> {row.get('cte_peso', 0)} kg
                """

                folium.CircleMarker(
                    location=[lat, lon],
                    radius=5,
                    color="black",
                    fill=True,
                    fill_color=cor,
                    fill_opacity=0.7,
                    popup=Popup(popup_html, max_width=300)
                ).add_to(fg)

            except Exception as e:
                if logger:
                    logger.warning(f"Erro ao plotar ponto: {e}")

        fg.add_to(mapa)

    folium.LayerControl().add_to(mapa)

    mapa.save(mapa_path)

    # 🔥 PNG
    plt.figure(figsize=(10, 8))

    for idx, rota_id in enumerate(df_rotas_validas["rota_id"].unique()):
        cor = cores[idx % len(cores)]

        df_rota = df_rotas_validas[
            df_rotas_validas["rota_id"] == rota_id
        ].sort_values("ordem_entrega")
        if "coordenadas_seq" in df_rota.columns:
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
                    plt.plot(lons, lats, marker="o", color=cor)

            except:
                pass

    plt.scatter(df_clusters["centro_lon"], df_clusters["centro_lat"], c="black")

    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.grid(True)

    plt.savefig(png_path, dpi=150)
    plt.close()

    if logger:
        logger.info(f"✅ Mapa last-mile gerado: {mapa_path}")