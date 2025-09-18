# simulation/visualization/plot_simulation_transfer.py

import os
import folium
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

from folium.plugins import MarkerCluster
from simulation.infrastructure.simulation_database_reader import (
    carregar_resumo_clusters,
    carregar_entregas_clusterizadas,
    carregar_rotas_transferencias,
    carregar_hubs
)

# 🔹 Importando utilitários do transfer_routing
from transfer_routing.infrastructure.database_connection import conectar_banco_routing, fechar_conexao
from transfer_routing.infrastructure.cache import obter_rota_do_cache


def decode_polyline(polyline_str):
    """Decodifica polyline do Google/OSRM em lista de coordenadas (lat, lon)."""
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


def desenhar_rota(mapa, pontos, cor, rota_id, logger=None):
    """
    Desenha uma rota no mapa passando pelo cache de rotas.
    Fallback: linha reta se não houver polyline.
    """
    conn = conectar_banco_routing()

    for i in range(len(pontos) - 1):
        try:
            origem = (float(pontos[i][0]), float(pontos[i][1]))
            destino = (float(pontos[i + 1][0]), float(pontos[i + 1][1]))
        except (ValueError, TypeError) as e:
            if logger: logger.warning(f"⚠️ Coordenada inválida na rota {rota_id}, índice {i}: {e}")
            continue

        origem_str = f"{round(origem[0], 6)},{round(origem[1], 6)}"
        destino_str = f"{round(destino[0], 6)},{round(destino[1], 6)}"

        distancia, tempo, rota_json = obter_rota_do_cache(origem_str, destino_str, conn)

        if rota_json:
            polyline = rota_json.get("overview_polyline")
            if polyline:
                coords = decode_polyline(polyline)
                folium.PolyLine(
                    locations=coords,
                    color=cor,
                    weight=3,
                    opacity=0.7,
                    tooltip=f"Rota {rota_id}"
                ).add_to(mapa)
            elif "coordenadas" in rota_json and rota_json["coordenadas"]:
                coords = []
                for p in rota_json["coordenadas"]:
                    if isinstance(p, (list, tuple)) and len(p) >= 2:
                        coords.append((float(p[0]), float(p[1])))
                    elif isinstance(p, dict) and "lat" in p and "lon" in p:
                        coords.append((float(p["lat"]), float(p["lon"])))
                if coords:
                    folium.PolyLine(
                        locations=coords,
                        color=cor,
                        weight=3,
                        opacity=0.7,
                        tooltip=f"Rota {rota_id}"
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
            # fallback
            folium.PolyLine(
                locations=[origem, destino],
                color="gray",
                weight=1,
                opacity=0.5,
                tooltip="Linha Reta"
            ).add_to(mapa)

    fechar_conexao(conn)


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
    try:
        output_path = os.path.join(output_dir, tenant_id)
        os.makedirs(output_path, exist_ok=True)

        mapa_path = os.path.join(output_path, f"{tenant_id}_mapa_transferencias_{envio_data}_k{k_clusters}.html")
        png_path = mapa_path.replace(".html", ".png")

        # 🔄 Se modo_forcar=True, remove antes da sobrescrita
        if modo_forcar:
            for path in [mapa_path, png_path]:
                if os.path.exists(path):
                    os.remove(path)
        elif os.path.exists(mapa_path) or os.path.exists(png_path):
            if logger: logger.info(f"🟡 Mapas já existem ({envio_data}, k={k_clusters}). Use --modo_forcar para sobrescrever.")
            return

        # 🔹 Carregar dados
        df_entregas = carregar_entregas_clusterizadas(simulation_db, clusterization_db, tenant_id, envio_data, k_clusters)
        df_clusters = carregar_resumo_clusters(simulation_db, tenant_id, envio_data, k_clusters)
        df_rotas = carregar_rotas_transferencias(simulation_db, tenant_id, envio_data, k_clusters)
        hubs = carregar_hubs(simulation_db, tenant_id)

        if df_entregas.empty or df_clusters.empty or df_rotas.empty or not hubs:
            raise ValueError("❌ Dados ausentes para plotagem: entregas, clusters, rotas ou hubs.")

        # mapa inicial centralizado nos hubs
        mapa = folium.Map(
            location=[sum([h["latitude"] for h in hubs]) / len(hubs),
                      sum([h["longitude"] for h in hubs]) / len(hubs)],
            zoom_start=7
        )

        # Hubs
        for hub in hubs:
            folium.Marker(
                location=[hub["latitude"], hub["longitude"]],
                popup=f"Hub: {hub['nome']}",
                icon=folium.Icon(color="red", icon="truck", prefix="fa")
            ).add_to(mapa)

        # Clusters
        for _, row in df_clusters.iterrows():
            folium.Marker(
                location=[row["centro_lat"], row["centro_lon"]],
                popup=f"Cluster {row['cluster']} - {row['cluster_cidade']}",
                icon=folium.Icon(color="green", icon="building", prefix="fa")
            ).add_to(mapa)

        # Rotas (cada cluster vira ponto de rota)
        cores_disponiveis = list(mcolors.TABLEAU_COLORS.values())
        rota_color_map = {}

        for idx, row in df_rotas.iterrows():
            rota_id = row.get("rota_id", f"linha_{idx}")
            cor = cores_disponiveis[idx % len(cores_disponiveis)]
            rota_color_map[rota_id] = cor

            # Monta sequência de pontos da rota
            pontos = []
            try:
                rota_coords = row.get("rota_completa_json")
                if isinstance(rota_coords, str):
                    import json
                    rota_coords = json.loads(rota_coords)
                if isinstance(rota_coords, list):
                    for p in rota_coords:
                        if isinstance(p, dict) and "lat" in p and "lon" in p:
                            pontos.append((p["lat"], p["lon"]))
                        elif isinstance(p, (list, tuple)) and len(p) == 2:
                            pontos.append((p[0], p[1]))
            except Exception as e:
                if logger: logger.warning(f"⚠️ Erro ao ler rota {rota_id}: {e}")

            if len(pontos) >= 2:
                desenhar_rota(mapa, pontos, cor, rota_id, logger)

        # Entregas
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

        # salva HTML
        mapa.save(mapa_path)

        # Gera PNG simplificado (sem cache, só clusters)
        plt.figure(figsize=(10, 8))
        plt.scatter(df_clusters["centro_lon"], df_clusters["centro_lat"], c="black", marker="x", s=80, label="Centros")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(png_path, dpi=150)
        plt.close()

        if logger: logger.info(f"✅ Mapas de transferências salvos: {mapa_path}, {png_path}")

    except Exception as erro:
        if logger: logger.exception(f"❌ Erro geral: {erro}")
        else: print(f"❌ Erro geral: {erro}")
