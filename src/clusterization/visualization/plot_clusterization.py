#clusterization/visualization/plot_clusterization.py

import folium
import pandas as pd
import os
import matplotlib.pyplot as plt

BASE_OUTPUT_DIR = "/app/output"

def gerar_mapa_clusters(df_clusterizado: pd.DataFrame, envio_data: str, tenant_id: str, output_path: str = None):
    if df_clusterizado.empty:
        raise ValueError("DataFrame de clusterização está vazio.")

    output_path = output_path or os.path.join(BASE_OUTPUT_DIR, "maps", tenant_id)
    output_path = os.path.abspath(output_path)
    os.makedirs(output_path, exist_ok=True)

    lat_centro = df_clusterizado['destino_latitude'].mean()
    lon_centro = df_clusterizado['destino_longitude'].mean()
    mapa = folium.Map(location=[lat_centro, lon_centro], zoom_start=7)

    cores = ["red", "blue", "green", "orange", "purple", "darkred", "lightblue", "darkgreen", "cadetblue", "black"]
    df_clusterizado['cte_valor_nf'] = pd.to_numeric(df_clusterizado['cte_valor_nf'], errors='coerce').fillna(0)
    clusters_unicos = sorted(df_clusterizado['cluster'].unique())

    for idx, cluster_id in enumerate(clusters_unicos):
        cor = cores[idx % len(cores)]
        grupo = df_clusterizado[df_clusterizado['cluster'] == cluster_id]

        for _, row in grupo.iterrows():
            valor_nf = row['cte_valor_nf']
            radius = max(4, min(valor_nf / 1000, 15))
            folium.CircleMarker(
                location=(row['destino_latitude'], row['destino_longitude']),
                radius=radius,
                color=cor,
                fill=True,
                fill_opacity=0.7,
                popup=f"""
                📦 CT-e: {row['cte_numero']}<br>
                🚚 Cluster: {cluster_id}<br>
                💰 Valor NF: R$ {valor_nf:,.2f}
                """.replace(",", "X").replace(".", ",").replace("X", ".")
            ).add_to(mapa)

        folium.Marker(
            location=[grupo['centro_lat'].iloc[0], grupo['centro_lon'].iloc[0]],
            icon=folium.Icon(color="black", icon="home", prefix="fa"),
            tooltip=f"🏠 Centro Cluster {cluster_id}"
        ).add_to(mapa)

    # Legenda
    legend_html = "<div style='position: fixed; bottom: 50px; left: 50px; width: 250px; height: auto; background-color: white; z-index:9999; border:2px solid grey; padding: 10px;'>"
    legend_html += "<h4>Legenda Clusters</h4>"
    for idx, cluster_id in enumerate(clusters_unicos):
        color = cores[idx % len(cores)]
        legend_html += f"<i style='background:{color};width:10px;height:10px;display:inline-block'></i> Cluster {cluster_id}<br>"
    legend_html += "</div>"
    mapa.get_root().html.add_child(folium.Element(legend_html))

    caminho_saida = os.path.join(output_path, "mapa_clusters.html")
    mapa.save(caminho_saida)
    print(f"✅ Mapa salvo em: {caminho_saida}")
    return caminho_saida


def gerar_mapa_estatico(df_clusterizado: pd.DataFrame, envio_data: str, tenant_id: str, output_path: str = None):
    if df_clusterizado.empty:
        raise ValueError("DataFrame de clusterização está vazio.")

    output_path = output_path or os.path.join(BASE_OUTPUT_DIR, "maps", tenant_id)
    output_path = os.path.abspath(output_path)
    os.makedirs(output_path, exist_ok=True)

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.set_facecolor("white")

    cores = ["red", "blue", "green", "orange", "purple", "darkred", "lightblue", "darkgreen", "cadetblue", "black"]
    clusters_unicos = sorted(df_clusterizado['cluster'].unique())

    for idx, cluster_id in enumerate(clusters_unicos):
        cor = cores[idx % len(cores)]
        grupo = df_clusterizado[df_clusterizado['cluster'] == cluster_id]
        ax.scatter(grupo['destino_longitude'], grupo['destino_latitude'], s=50, color=cor, label=f"Cluster {cluster_id}", alpha=0.7, edgecolors='k')
        ax.scatter(grupo['centro_lon'].iloc[0], grupo['centro_lat'].iloc[0], marker='*', s=250, color="black", edgecolors='k', zorder=5)

    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"Mapa Estático - {envio_data}")
    ax.legend(loc="upper right", fontsize=8)

    caminho_png = os.path.join(output_path, "mapa_clusters.png")
    plt.savefig(caminho_png, dpi=300, bbox_inches='tight')
    plt.close(fig)

    print(f"✅ Mapa estático salvo em: {caminho_png}")
    return caminho_png
