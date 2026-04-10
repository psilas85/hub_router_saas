# simulation/infrastructure/cache_routes.py

import json
import math
from geopy.distance import geodesic
from simulation.utils.google_api import buscar_rota_google
from simulation.utils.osrm_api import buscar_rota_osrm  # 🔹 Import OSRM
from simulation.utils.rate_limiter import RateLimiter

# 🚦 Valores mínimos para evitar rotas "zeradas"
MIN_DIST_KM = 0.03   # 30 metros
MIN_TIME_MIN = 0.2   # 12 segundos
MANUAL_ROUTE_DISTANCE_FACTOR = 1.2
DEFAULT_MANUAL_FALLBACK_SPEED_KMH = 60.0
GOOGLE_RATE_LIMITER = RateLimiter(max_calls_per_sec=10)


def _formatar_coord(coord: tuple) -> str:
    """
    Formata coordenada para string no padrão 'lon,lat' (necessário para OSRM e Google).
    coord: (lat, lon)
    """
    lat, lon = coord
    return f"{round(lon, 6)},{round(lat, 6)}"


def _rota_minima(origem, destino, logger=None):
    """Retorna rota mínima quando origem/destino são iguais ou muito próximos."""
    distancia_metros = geodesic(origem, destino).meters
    if logger:
        logger.info(f"⚡ Rota curta detectada ({distancia_metros:.1f}m). "
                    f"Usando fallback mínimo {MIN_DIST_KM} km | {MIN_TIME_MIN} min")
    return MIN_DIST_KM, MIN_TIME_MIN, [
        {"lat": origem[0], "lon": origem[1]},
        {"lat": destino[0], "lon": destino[1]}
    ]


def _distancia_haversine_km(origem, destino):
    lat1, lon1 = origem
    lat2, lon2 = destino

    raio_terra_km = 6371.0
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return raio_terra_km * c


def _calcular_rota_manual(origem, destino, velocidade_media_kmh=None, logger=None):
    velocidade_kmh = float(velocidade_media_kmh or DEFAULT_MANUAL_FALLBACK_SPEED_KMH)
    if velocidade_kmh <= 0:
        velocidade_kmh = DEFAULT_MANUAL_FALLBACK_SPEED_KMH

    distancia_base_km = _distancia_haversine_km(origem, destino)
    distancia_km = max(distancia_base_km * MANUAL_ROUTE_DISTANCE_FACTOR, MIN_DIST_KM)
    tempo_min = max((distancia_km / velocidade_kmh) * 60, MIN_TIME_MIN)
    coordenadas = [
        {"lat": origem[0], "lon": origem[1]},
        {"lat": destino[0], "lon": destino[1]},
    ]

    if logger:
        logger.warning(
            "⚠️ OSRM e Google indisponíveis. Aplicando fallback manual "
            f"(haversine * {MANUAL_ROUTE_DISTANCE_FACTOR:.1f}) | "
            f"dist={distancia_km:.2f} km | tempo_trânsito={tempo_min:.1f} min"
        )

    return distancia_km, tempo_min, coordenadas


def _montar_rota_json(origem_str, destino_str, distancia_km, tempo_min, rota_completa_dicts):
    return {
        "origem": origem_str,
        "destino": destino_str,
        "distancia_km": float(distancia_km),
        "tempo_minutos": float(tempo_min),
        "coordenadas": rota_completa_dicts,
        "fonte": "osrm",
    }


def _extrair_rota_cache(rota_json):
    if not rota_json:
        return None

    coordenadas = rota_json.get("coordenadas") or rota_json.get("rota_completa") or []
    if not coordenadas:
        return None

    tempo_min = rota_json.get("tempo_minutos")
    if tempo_min is None:
        tempo_min = rota_json.get("tempo_min")

    distancia_km = rota_json.get("distancia_km")
    if distancia_km is None or tempo_min is None:
        return None

    return float(distancia_km), float(tempo_min), coordenadas, rota_json.get("fonte")


def _salvar_cache(db_conn, origem_str, destino_str, tenant_id,
                  distancia_km, tempo_min, rota_completa_dicts, logger=None,
                  fonte="osrm"):
    """
    Salva rota válida no cache. Ignora se rota_completa_dicts estiver vazio.
    """
    if not rota_completa_dicts:
        if logger:
            logger.warning(f"⚠️ Tentativa de salvar rota inválida {origem_str} -> {destino_str}. Ignorada.")
        return

    rota_json = _montar_rota_json(
        origem_str,
        destino_str,
        distancia_km,
        tempo_min,
        rota_completa_dicts,
    )
    rota_json["fonte"] = fonte

    insert = """
        INSERT INTO cache_rotas (
            origem, destino, distancia_km, tempo_minutos, rota_json, tenant_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (origem, destino, tenant_id) DO UPDATE SET
            distancia_km = EXCLUDED.distancia_km,
            tempo_minutos = EXCLUDED.tempo_minutos,
            rota_json = EXCLUDED.rota_json
    """
    cursor = db_conn.cursor()
    cursor.execute(insert, (
        origem_str, destino_str,
        float(distancia_km), float(tempo_min),
        json.dumps(rota_json), tenant_id
    ))
    db_conn.commit()
    cursor.close()

    if logger:
        logger.info(f"✅ Cache salvo para {origem_str} -> {destino_str}")


def _buscar_rota_google_rate_limited(origem, destino, logger=None):
    if logger:
        logger.info("🚦 Aplicando rate limit antes da chamada ao Google.")
    GOOGLE_RATE_LIMITER.wait()
    return buscar_rota_google(origem, destino)


def _obter_rota_detalhada(
    origem: tuple,
    destino: tuple,
    tenant_id: str,
    db_conn,
    logger=None,
    velocidade_media_kmh=None,
):
    origem_str = _formatar_coord(origem)
    destino_str = _formatar_coord(destino)

    if origem_str == destino_str or geodesic(origem, destino).meters < 30:
        distancia_km, tempo_min, coordenadas = _rota_minima(origem, destino, logger)
        return distancia_km, tempo_min, coordenadas, "fallback_minimo"

    query = """
        SELECT rota_json
        FROM cache_rotas
        WHERE origem = %s AND destino = %s AND tenant_id = %s
    """
    cursor = db_conn.cursor()
    cursor.execute(query, (origem_str, destino_str, tenant_id))
    row = cursor.fetchone()
    cursor.close()

    if row:
        rota_json = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        rota_cache = _extrair_rota_cache(rota_json)
        if rota_cache:
            distancia_km, tempo_min, coordenadas, fonte_cache = rota_cache
            if fonte_cache == "osrm":
                if logger:
                    logger.info(f"🚗 Cache HIT OSRM: {origem_str} → {destino_str}")
                return distancia_km, tempo_min, coordenadas, "cache_osrm"
            if logger:
                logger.warning(
                    f"⚠️ Cache sem fonte OSRM para {origem_str} -> {destino_str}. Recalculando rota."
                )
        elif logger:
            logger.warning(f"⚠️ Cache inválido ignorado {origem_str} -> {destino_str}")

    if logger:
        logger.info(f"🔍 Cache MISS (rota): {origem_str} → {destino_str} → tentando OSRM...")

    distancia_km, tempo_min, rota_raw = buscar_rota_osrm(origem, destino)
    if distancia_km and tempo_min and rota_raw:
        rota_completa_dicts = [
            {"lat": float(lat), "lon": float(lon)}
            for lat, lon in rota_raw if lat is not None and lon is not None
        ]
        if rota_completa_dicts:
            _salvar_cache(
                db_conn,
                origem_str,
                destino_str,
                tenant_id,
                distancia_km,
                tempo_min,
                rota_completa_dicts,
                logger,
                fonte="osrm",
            )
            return float(distancia_km), float(tempo_min), rota_completa_dicts, "osrm"

    if logger:
        logger.warning(f"⚠️ OSRM falhou para {origem_str} → {destino_str}, tentando Google...")

    distancia_km, tempo_min, rota_raw = _buscar_rota_google_rate_limited(
        origem,
        destino,
        logger,
    )
    if distancia_km and tempo_min and rota_raw:
        rota_completa_dicts = [
            {"lat": float(lat), "lon": float(lon)}
            for lat, lon in rota_raw if lat is not None and lon is not None
        ]
        if rota_completa_dicts:
            return float(distancia_km), float(tempo_min), rota_completa_dicts, "google"

    distancia_km, tempo_min, coordenadas = _calcular_rota_manual(
        origem,
        destino,
        velocidade_media_kmh=velocidade_media_kmh,
        logger=logger,
    )
    return distancia_km, tempo_min, coordenadas, "manual_haversine"


def obter_rota_real_detalhada(
    origem: tuple,
    destino: tuple,
    tenant_id: str,
    db_conn,
    logger=None,
    velocidade_media_kmh=None,
):
    return _obter_rota_detalhada(
        origem,
        destino,
        tenant_id,
        db_conn,
        logger,
        velocidade_media_kmh,
    )


def obter_rota_last_mile_detalhada(
    origem,
    destino,
    tenant_id,
    simulation_db,
    db_conn,
    logger=None,
    velocidade_media_kmh=None,
):
    return _obter_rota_detalhada(
        origem,
        destino,
        tenant_id,
        db_conn,
        logger,
        velocidade_media_kmh,
    )


def obter_rota_real(
    origem: tuple,
    destino: tuple,
    tenant_id: str,
    db_conn,
    logger=None,
    velocidade_media_kmh=None,
):
    distancia_km, tempo_min, coordenadas, _ = obter_rota_real_detalhada(
        origem,
        destino,
        tenant_id,
        db_conn,
        logger,
        velocidade_media_kmh,
    )
    return distancia_km, tempo_min, coordenadas

def obter_rota_last_mile(
    origem,
    destino,
    tenant_id,
    simulation_db,
    db_conn,
    logger=None,
    velocidade_media_kmh=None,
):
    distancia_km, tempo_min, coordenadas, _ = obter_rota_last_mile_detalhada(
        origem,
        destino,
        tenant_id,
        simulation_db,
        db_conn,
        logger,
        velocidade_media_kmh,
    )
    return distancia_km, tempo_min, coordenadas
