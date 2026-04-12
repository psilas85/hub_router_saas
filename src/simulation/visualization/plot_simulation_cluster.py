#simulation/visualization/plot_simulation_cluster.py

import os
import folium
import pandas as pd
import matplotlib.pyplot as plt

from simulation.infrastructure.simulation_database_reader import (
    carregar_resumo_clusters,
    carregar_entregas_clusterizadas,
    carregar_hubs
)


def plotar_mapa_clusterizacao_simulation(
    simulation_db,
    clusterization_db,
    tenant_id,
    envio_data,
    k_clusters,
    output_dir="exports/simulation/entregas",
    modo_forcar=False,
    logger=None
):
    try:
        output_path = os.path.join(output_dir, tenant_id)
        os.makedirs(output_path, exist_ok=True)

        mapa_path = os.path.join(output_path, f"{tenant_id}_mapa_clusterizacao_{envio_data}_k{k_clusters}.html")
        png_path = mapa_path.replace(".html", ".png")

        # 🔄 Se modo_forcar=True, remove antes de salvar
        if modo_forcar:
            for path in [mapa_path, png_path]:
                if os.path.exists(path):
                    try:
                        os.remove(path)
                        if logger:
                            logger.info(f"🗑️ Arquivo removido antes da sobrescrita: {path}")
                    except Exception as e:
                        if logger:
                            logger.error(f"❌ Falha ao remover {path}: {e}")
        # 🟡 Se já existe e não é modo_forcar, não sobrescreve
        elif os.path.exists(mapa_path) or os.path.exists(png_path):
            if logger:
                logger.info(
                    f"🟡 Mapas de clusterização já existem ({envio_data}, k={k_clusters}). "
                    f"Use --modo_forcar para sobrescrever."
                )
            return

        # 🔹 Carregar dados
        df_clusters = carregar_resumo_clusters(simulation_db, tenant_id, envio_data, k_clusters)
        df_entregas_clusterizadas = carregar_entregas_clusterizadas(
            simulation_db, clusterization_db, tenant_id, envio_data, k_clusters
        )
        hubs = carregar_hubs(simulation_db, tenant_id)

        if df_clusters.empty or df_entregas_clusterizadas.empty or not hubs:
            mensagem = "❌ Dados ausentes para plotagem: clusters, entregas ou hubs."
            if logger:
                logger.warning(mensagem)
            else:
                print(mensagem)
            return

        df_entregas_clusterizadas["cte_numero"] = df_entregas_clusterizadas["cte_numero"].astype(str)

        df_coord = pd.read_sql(
            """
            SELECT cte_numero, destino_latitude AS latitude, destino_longitude AS longitude
            FROM entregas
            WHERE tenant_id = %s AND envio_data = %s
            """,
            clusterization_db,
            params=(tenant_id, envio_data)
        )
        df_coord["cte_numero"] = df_coord["cte_numero"].astype(str)
        df_coord = df_coord.rename(
            columns={"latitude": "latitude_original", "longitude": "longitude_original"}
        )

        df_entregas = pd.merge(df_entregas_clusterizadas, df_coord, on="cte_numero", how="left")
        df_entregas["latitude"] = df_entregas["latitude"].fillna(df_entregas["latitude_original"])
        df_entregas["longitude"] = df_entregas["longitude"].fillna(df_entregas["longitude_original"])

        # 🌍 Geração do mapa interativo (HTML)
        # Junta todas as coordenadas relevantes para ajustar o zoom
        all_lats = list(df_entregas["latitude"].dropna()) + [h["latitude"] for h in hubs] + list(df_clusters["centro_lat"].dropna())
        all_lons = list(df_entregas["longitude"].dropna()) + [h["longitude"] for h in hubs] + list(df_clusters["centro_lon"].dropna())
        if all_lats and all_lons:
            min_lat, max_lat = min(all_lats), max(all_lats)
            min_lon, max_lon = min(all_lons), max(all_lons)
            center_lat = (min_lat + max_lat) / 2
            center_lon = (min_lon + max_lon) / 2
            mapa = folium.Map(location=[center_lat, center_lon], zoom_start=7)
        else:
            mapa = folium.Map(location=[hubs[0]['latitude'], hubs[0]['longitude']], zoom_start=7)
        cores = [
            "red", "blue", "green", "purple", "orange", "darkred", "lightred",
            "beige", "darkblue", "darkgreen", "cadetblue", "darkpurple", "pink",
            "lightblue", "lightgreen", "gray", "black", "lightgray"
        ]

        folium.Marker(
            location=[hubs[0]['latitude'], hubs[0]['longitude']],
            popup=f"HUB: {hubs[0]['nome']}",
            icon=folium.Icon(color="black", icon="star")
        ).add_to(mapa)

        for _, row in df_clusters.iterrows():
            cor = cores[int(row['cluster']) % len(cores)]
            folium.Marker(
                location=[row['centro_lat'], row['centro_lon']],
                popup=f"Centro Cluster {row['cluster']}",
                icon=folium.Icon(color=cor, icon="home")
            ).add_to(mapa)

        for _, row in df_entregas.iterrows():
            if pd.notnull(row['latitude']) and pd.notnull(row['longitude']):
                cor = cores[int(row['cluster']) % len(cores)]
                peso = float(row.get("cte_peso", 0))
                raio = max(2, min(6, peso / 200.0))
                folium.CircleMarker(
                    location=[row["latitude"], row["longitude"]],
                    radius=raio,
                    color=cor,
                    fill=True,
                    fill_color=cor,
                    fill_opacity=0.6,
                    popup=f"CTE: {row['cte_numero']}<br>Cluster: {row['cluster']}<br>Peso: {peso:.1f} kg"
                ).add_to(mapa)

        # Ajusta o zoom para englobar todos os pontos
        if all_lats and all_lons:
            mapa.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])
        mapa.save(mapa_path)

        # 📊 Geração do PNG estático
        plt.figure(figsize=(10, 8))
        for cluster_id, grupo in df_entregas.groupby("cluster"):
            plt.scatter(grupo["longitude"], grupo["latitude"], label=f"Cluster {cluster_id}", s=15)
        plt.scatter(
            df_clusters["centro_lon"], df_clusters["centro_lat"],
            c="black", marker="x", s=80, label="Centros"
        )
        plt.scatter(
            hubs[0]['longitude'], hubs[0]['latitude'],
            c="red", marker="*", s=150, label="HUB"
        )
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(png_path, dpi=150)
        plt.close()

        if logger:
            logger.info(f"✅ Mapas de clusterização salvos: {mapa_path}, {png_path}")

    except Exception as erro:
        if logger:
            logger.exception(f"❌ Erro geral: {erro}")
        else:
            print(f"❌ Erro geral: {erro}")
