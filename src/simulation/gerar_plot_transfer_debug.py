#hub_router_1.0.1/src/simulation/gerar_plot_transfer_debug.py

import os
import json
import math
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import psycopg2
import psycopg2.extras
import pandas as pd
import requests
import folium
from dotenv import load_dotenv


# =========================================================
# CONFIG
# =========================================================

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "simulation_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

OSRM_BASE_URL = f"http://{os.getenv('OSRM_HOST')}:{os.getenv('OSRM_PORT')}"
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "debug_maps")

SIMULATION_ID = "bec3c67e-4a2b-46ab-a82e-0dac73e6368e"

TENANT_ID = os.getenv("TENANT_ID", "59919562-0160-4eee-95ac-8f2df3c6b043")
ENVIO_DATA = os.getenv("ENVIO_DATA", "2026-04-01")
K_CLUSTERS = int(os.getenv("K_CLUSTERS", "5"))
HUB_ID = int(os.getenv("HUB_ID", "5"))

# limite opcional para quebrar em mais de uma rota
TEMPO_MAX_TRANSFERENCIA_MIN = float(os.getenv("TEMPO_MAX_TRANSFERENCIA_MIN", "500"))
VELOCIDADE_MEDIA_KMH = float(os.getenv("VELOCIDADE_MEDIA_KMH", "35"))


# =========================================================
# MODELOS
# =========================================================

@dataclass
class Ponto:
    node_id: str
    nome: str
    lat: float
    lon: float
    cluster: Optional[str] = None
    peso_total_kg: float = 0.0
    volumes_total: float = 0.0
    qde_ctes: int = 0


@dataclass
class Segmento:
    origem_id: str
    destino_id: str
    distancia_km: float
    tempo_min: float
    geometria: List[Tuple[float, float]]


# =========================================================
# DB
# =========================================================

def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def buscar_hub(conn, tenant_id: str, hub_id: int) -> Ponto:
    sql = """
        SELECT hub_id, nome, latitude, longitude, cidade
        FROM hubs
        WHERE tenant_id = %s
          AND hub_id = %s
        LIMIT 1
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (tenant_id, hub_id))
        row = cur.fetchone()

    if not row:
        raise ValueError(f"Hub não encontrado para tenant_id={tenant_id}, hub_id={hub_id}")

    return Ponto(
        node_id=f"hub_{row['hub_id']}",
        nome=row["nome"] or f"Hub {row['hub_id']}",
        lat=float(row["latitude"]),
        lon=float(row["longitude"]),
    )


def buscar_centros_clusters(
    conn,
    tenant_id: str,
    envio_data: str,
    k_clusters: int,
    simulation_id: str
) -> List[Ponto]:

    sql = """
        SELECT
            cluster,
            centro_lat,
            centro_lon,
            peso_total_kg,
            volumes_total,
            qde_ctes,
            cluster_cidade
        FROM resumo_clusters
        WHERE tenant_id = %s
          AND envio_data = %s
          AND k_clusters = %s
          AND simulation_id = %s
        ORDER BY cluster
    """

    df = pd.read_sql(
        sql,
        conn,
        params=(tenant_id, envio_data, k_clusters, simulation_id)
    )

    if df.empty:
        raise ValueError(
            f"Nenhum centro encontrado em resumo_clusters para "
            f"tenant_id={tenant_id}, envio_data={envio_data}, "
            f"k_clusters={k_clusters}, simulation_id={simulation_id}"
        )

    print(f"📊 Total registros carregados: {len(df)}")

    pontos = []

    for _, row in df.iterrows():

        cluster_id = str(row["cluster"])

        # 🔴 IGNORA CLUSTER DO HUB
        if cluster_id.startswith("9999"):
            print(f"⚠️ Ignorando cluster HUB: {cluster_id}")
            continue

        if pd.isna(row["centro_lat"]) or pd.isna(row["centro_lon"]):
            print(f"⚠️ Cluster sem coordenada: {cluster_id}")
            continue

        ponto = Ponto(
            node_id=f"cluster_{cluster_id}",
            nome=f"Cluster {cluster_id}",
            lat=float(row["centro_lat"]),
            lon=float(row["centro_lon"]),
            cluster=cluster_id,
            peso_total_kg=float(row.get("peso_total_kg", 0) or 0),
            volumes_total=float(row.get("volumes_total", 0) or 0),
            qde_ctes=int(row.get("qde_ctes", 0) or 0),
        )

        pontos.append(ponto)

    print(f"📍 Centros válidos (sem hub): {len(pontos)}")

    if not pontos:
        raise ValueError(
            "Centros encontrados, mas nenhum válido após filtros "
            "(possível erro de dados ou filtro por simulation_id)"
        )

    return pontos

# =========================================================
# CACHE + OSRM
# =========================================================

def _coord_key(lat: float, lon: float) -> str:
    return f"{lon:.6f},{lat:.6f}"


def buscar_cache_rota(conn, origem: Tuple[float, float], destino: Tuple[float, float]) -> Optional[Segmento]:
    origem_key = _coord_key(origem[0], origem[1])
    destino_key = _coord_key(destino[0], destino[1])

    sql = """
        SELECT distancia_km, tempo_min, rota_completa_json
        FROM cache_rotas
        WHERE origem = %s
          AND destino = %s
        ORDER BY id DESC
        LIMIT 1
    """

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (origem_key, destino_key))
            row = cur.fetchone()
    except Exception:
        return None

    if not row:
        return None

    try:
        geom_raw = row["rota_completa_json"]
        geom = json.loads(geom_raw) if isinstance(geom_raw, str) else geom_raw
        coords = []
        if isinstance(geom, list):
            for p in geom:
                if isinstance(p, dict) and "lat" in p and "lon" in p:
                    coords.append((float(p["lat"]), float(p["lon"])))
                elif isinstance(p, (list, tuple)) and len(p) == 2:
                    coords.append((float(p[0]), float(p[1])))
    except Exception:
        coords = [origem, destino]

    return Segmento(
        origem_id=origem_key,
        destino_id=destino_key,
        distancia_km=float(row["distancia_km"] or 0),
        tempo_min=float(row["tempo_min"] or 0),
        geometria=coords if coords else [origem, destino],
    )


def consultar_osrm(origem: Tuple[float, float], destino: Tuple[float, float]) -> Segmento:
    # lat/lon -> OSRM usa lon,lat
    url = (
        f"{OSRM_BASE_URL}/route/v1/driving/"
        f"{origem[1]},{origem[0]};{destino[1]},{destino[0]}"
        f"?overview=full&geometries=geojson"
    )

    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    if data.get("code") != "Ok" or not data.get("routes"):
        raise RuntimeError(f"OSRM sem rota válida: {data}")

    route = data["routes"][0]
    distancia_km = float(route["distance"]) / 1000.0
    tempo_min = float(route["duration"]) / 60.0

    coords = []
    for lon, lat in route["geometry"]["coordinates"]:
        coords.append((float(lat), float(lon)))

    return Segmento(
        origem_id=_coord_key(origem[0], origem[1]),
        destino_id=_coord_key(destino[0], destino[1]),
        distancia_km=distancia_km,
        tempo_min=tempo_min,
        geometria=coords,
    )


def obter_segmento(conn, origem: Tuple[float, float], destino: Tuple[float, float]) -> Segmento:
    if origem == destino:
        return Segmento(
            origem_id=_coord_key(origem[0], origem[1]),
            destino_id=_coord_key(destino[0], destino[1]),
            distancia_km=0.03,
            tempo_min=0.2,
            geometria=[origem, destino],
        )

    seg_cache = buscar_cache_rota(conn, origem, destino)
    if seg_cache:
        return seg_cache

    return consultar_osrm(origem, destino)


# =========================================================
# SAVINGS
# =========================================================

def construir_matriz_segmentos(conn, hub: Ponto, pontos: List[Ponto]) -> Dict[Tuple[str, str], Segmento]:
    todos = [hub] + pontos
    segmentos = {}

    for a in todos:
        for b in todos:
            if a.node_id == b.node_id:
                continue
            seg = obter_segmento(conn, (a.lat, a.lon), (b.lat, b.lon))
            segmentos[(a.node_id, b.node_id)] = seg

    return segmentos


def tempo_rota(route: List[str], hub_id: str, segmentos: Dict[Tuple[str, str], Segmento]) -> float:
    total = 0.0
    prev = hub_id
    for node in route:
        total += segmentos[(prev, node)].tempo_min
        prev = node
    total += segmentos[(prev, hub_id)].tempo_min
    return total


def distancia_rota(route: List[str], hub_id: str, segmentos: Dict[Tuple[str, str], Segmento]) -> float:
    total = 0.0
    prev = hub_id
    for node in route:
        total += segmentos[(prev, node)].distancia_km
        prev = node
    total += segmentos[(prev, hub_id)].distancia_km
    return total


def clarke_wright_savings(
    hub: Ponto,
    pontos: List[Ponto],
    segmentos: Dict[Tuple[str, str], Segmento],
    tempo_max_min: float,
) -> List[List[str]]:
    # rota inicial: uma rota por cluster
    routes = {p.node_id: [p.node_id] for p in pontos}
    route_of = {p.node_id: p.node_id for p in pontos}

    savings = []
    for i in pontos:
        for j in pontos:
            if i.node_id >= j.node_id:
                continue

            s = (
                segmentos[(hub.node_id, i.node_id)].distancia_km
                + segmentos[(hub.node_id, j.node_id)].distancia_km
                - segmentos[(i.node_id, j.node_id)].distancia_km
            )
            savings.append((i.node_id, j.node_id, s))

    savings.sort(key=lambda x: x[2], reverse=True)

    for i_id, j_id, _ in savings:
        ri_key = route_of.get(i_id)
        rj_key = route_of.get(j_id)

        # 🔴 valida existência real
        if (
            ri_key is None or
            rj_key is None or
            ri_key == rj_key or
            ri_key not in routes or
            rj_key not in routes
        ):
            continue

        ri = routes[ri_key]
        rj = routes[rj_key]
        # só junta se estiverem nas extremidades
        merged = None
        if ri[-1] == i_id and rj[0] == j_id:
            merged = ri + rj
        elif ri[0] == i_id and rj[-1] == j_id:
            merged = rj + ri
        elif ri[0] == i_id and rj[0] == j_id:
            merged = list(reversed(ri)) + rj
        elif ri[-1] == i_id and rj[-1] == j_id:
            merged = ri + list(reversed(rj))

        if not merged:
            continue

        if tempo_rota(merged, hub.node_id, segmentos) > tempo_max_min:
            continue

        # efetiva merge
        new_key = merged[0]

        # remove antigas primeiro
        del routes[ri_key]
        del routes[rj_key]

        # cria nova
        routes[new_key] = merged

        # 🔥 atualiza TODAS as referências
        for node in merged:
            route_of[node] = new_key

            return list(routes.values())


# =========================================================
# MAPA
# =========================================================

def montar_geometria_rota(
    route: List[str],
    hub: Ponto,
    pontos_by_id: Dict[str, Ponto],
    segmentos: Dict[Tuple[str, str], Segmento],
) -> List[Tuple[float, float]]:
    coords = []
    prev = hub.node_id

    for node in route:
        seg = segmentos[(prev, node)]
        if seg.geometria:
            if coords:
                coords.extend(seg.geometria[1:])
            else:
                coords.extend(seg.geometria)
        prev = node

    seg_back = segmentos[(prev, hub.node_id)]
    if seg_back.geometria:
        if coords:
            coords.extend(seg_back.geometria[1:])
        else:
            coords.extend(seg_back.geometria)

    return coords


def gerar_mapa(
    tenant_id: str,
    envio_data: str,
    k_clusters: int,
    hub: Ponto,
    pontos: List[Ponto],
    routes: List[List[str]],
    segmentos: Dict[Tuple[str, str], Segmento],
) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_lats = [hub.lat] + [p.lat for p in pontos]
    all_lons = [hub.lon] + [p.lon for p in pontos]
    center_lat = sum(all_lats) / len(all_lats)
    center_lon = sum(all_lons) / len(all_lons)

    mapa = folium.Map(location=[center_lat, center_lon], zoom_start=10)

    folium.Marker(
        location=[hub.lat, hub.lon],
        popup=f"Hub: {hub.nome}",
        tooltip="Hub Central",
        icon=folium.Icon(color="red", icon="home"),
    ).add_to(mapa)

    pontos_by_id = {p.node_id: p for p in pontos}

    for p in pontos:
        folium.Marker(
            location=[p.lat, p.lon],
            popup=(
                f"{p.nome}<br>"
                f"Cluster: {p.cluster}<br>"
                f"CTEs: {p.qde_ctes}<br>"
                f"Peso: {p.peso_total_kg:.2f} kg<br>"
                f"Volumes: {p.volumes_total:.0f}"
            ),
            tooltip=f"Cluster {p.cluster}",
            icon=folium.Icon(color="green", icon="map-marker"),
        ).add_to(mapa)

    cores = [
        "blue", "purple", "orange", "darkred", "cadetblue",
        "darkgreen", "black", "pink", "lightblue", "gray"
    ]

    for idx, route in enumerate(routes):
        coords = montar_geometria_rota(route, hub, pontos_by_id, segmentos)
        if len(coords) >= 2:
            folium.PolyLine(
                locations=coords,
                color=cores[idx % len(cores)],
                weight=5,
                opacity=0.85,
                tooltip=f"Rota {idx + 1}",
            ).add_to(mapa)

    html_path = os.path.join(
        OUTPUT_DIR,
        f"{tenant_id}_mapa_transfer_debug_{envio_data}_k{k_clusters}.html"
    )
    mapa.save(html_path)
    return html_path


# =========================================================
# MAIN
# =========================================================

def main():
    print("Conectando ao banco...")
    conn = get_conn()

    try:
        print("Buscando hub...")
        hub = buscar_hub(conn, TENANT_ID, HUB_ID)

        print("Buscando centros dos clusters...")
        pontos = buscar_centros_clusters(
            conn,
            TENANT_ID,
            ENVIO_DATA,
            K_CLUSTERS,
            SIMULATION_ID
        )

        print(f"Hub: {hub.nome} | Centros: {len(pontos)}")

        print("Construindo matriz de segmentos via cache/OSRM...")
        segmentos = construir_matriz_segmentos(conn, hub, pontos)

        print("Rodando Clarke-Wright Savings...")
        routes = clarke_wright_savings(
            hub=hub,
            pontos=pontos,
            segmentos=segmentos,
            tempo_max_min=TEMPO_MAX_TRANSFERENCIA_MIN,
        )

        print(f"Rotas geradas: {len(routes)}")
        for i, r in enumerate(routes, start=1):
            dist = distancia_rota(r, hub.node_id, segmentos)
            tempo = tempo_rota(r, hub.node_id, segmentos)
            clusters = [p for p in r]
            print(f"Rota {i}: stops={len(r)} | dist={dist:.2f} km | tempo={tempo:.2f} min | {clusters}")

        print("Gerando mapa...")
        html_path = gerar_mapa(
            tenant_id=TENANT_ID,
            envio_data=ENVIO_DATA,
            k_clusters=K_CLUSTERS,
            hub=hub,
            pontos=pontos,
            routes=routes,
            segmentos=segmentos,
        )

        print(f"Mapa salvo em: {html_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()