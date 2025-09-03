# simulation/infrastructure/cache_routes.py

import json
from geopy.distance import geodesic
from simulation.utils.google_api import buscar_rota_google
from simulation.utils.osrm_api import buscar_rota_osrm  # 🔹 Import OSRM

# 🚦 Valores mínimos para evitar rotas "zeradas"
MIN_DIST_KM = 0.03   # 30 metros
MIN_TIME_MIN = 0.2   # 12 segundos


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


def obter_rota_real(origem: tuple, destino: tuple, tenant_id: str, db_conn, logger=None):
    origem_str = _formatar_coord(origem)
    destino_str = _formatar_coord(destino)

    # 🚦 Se origem == destino ou <30m → fallback
    if origem_str == destino_str or geodesic(origem, destino).meters < 30:
        return _rota_minima(origem, destino, logger)

    # 1️⃣ Buscar no cache
    query = """
        SELECT rota_json
        FROM cache_rotas
        WHERE origem = %s AND destino = %s AND tenant_id = %s
    """
    cursor = db_conn.cursor()
    cursor.execute(query, (origem_str, destino_str, tenant_id))
    row = cursor.fetchone()

    if row:
        if logger:
            logger.info(f"🚗 Cache HIT (rota): {origem_str} → {destino_str}")
        rota_json = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        cursor.close()
        return (
            float(rota_json.get("distancia_km")),
            float(rota_json.get("tempo_min")),
            rota_json.get("rota_completa", [])
        )

    if logger:
        logger.info(f"🔍 Cache MISS (rota): {origem_str} → {destino_str} → tentando OSRM...")

    # 2️⃣ OSRM primeiro
    distancia_km, tempo_min, rota_raw = buscar_rota_osrm(origem, destino)

    # ⚠️ Se OSRM falhar → Google
    if not distancia_km or not tempo_min or not rota_raw:
        if logger:
            logger.warning(f"⚠️ OSRM falhou para {origem_str} → {destino_str}, tentando Google...")
        distancia_km, tempo_min, rota_raw = buscar_rota_google(origem, destino)

    # 3️⃣ Se ainda falhar → fallback mínimo
    if not distancia_km or not tempo_min or not rota_raw:
        cursor.close()
        return _rota_minima(origem, destino, logger)

    # 4️⃣ Padroniza coordenadas
    rota_completa_dicts = [
        {"lat": float(lat), "lon": float(lon)}
        for lat, lon in rota_raw if lat is not None and lon is not None
    ]

    # 5️⃣ Salva no cache
    rota_json = {
        "origem": origem_str,
        "destino": destino_str,
        "distancia_km": float(distancia_km),
        "tempo_min": float(tempo_min),
        "rota_completa": rota_completa_dicts
    }

    insert = """
        INSERT INTO cache_rotas (
            origem, destino, distancia_km, tempo_minutos, rota_json, tenant_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (origem, destino, tenant_id) DO NOTHING
    """
    cursor.execute(insert, (
        origem_str, destino_str,
        float(distancia_km), float(tempo_min),
        json.dumps(rota_json), tenant_id
    ))
    db_conn.commit()
    cursor.close()

    if logger:
        logger.info("📝 Nova rota salva no cache.")

    return float(distancia_km), float(tempo_min), rota_completa_dicts


def obter_rota_last_mile(origem, destino, tenant_id, simulation_db, db_conn, logger=None):
    origem_str = _formatar_coord(origem)
    destino_str = _formatar_coord(destino)

    # 🚦 Se origem == destino ou <30m → fallback
    if origem_str == destino_str or geodesic(origem, destino).meters < 30:
        return _rota_minima(origem, destino, logger)

    # 1️⃣ Buscar no cache
    query = """
        SELECT rota_json
        FROM cache_rotas
        WHERE origem = %s AND destino = %s AND tenant_id = %s
    """
    cursor = db_conn.cursor()
    cursor.execute(query, (origem_str, destino_str, tenant_id))
    row = cursor.fetchone()

    if row:
        if logger:
            logger.info(f"🚗 Cache HIT (rota): {origem_str} → {destino_str}")
        rota_json = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        cursor.close()
        return (
            float(rota_json.get("distancia_km")),
            float(rota_json.get("tempo_min")),
            rota_json.get("rota_completa", [])
        )

    if logger:
        logger.info(f"🔍 Cache MISS (rota): {origem_str} → {destino_str} → tentando OSRM...")

    # 2️⃣ OSRM primeiro
    distancia_km, tempo_min, rota_raw = buscar_rota_osrm(origem, destino)

    # ⚠️ Se OSRM falhar → Google
    if not distancia_km or not tempo_min or not rota_raw:
        if logger:
            logger.warning(f"⚠️ OSRM falhou para {origem_str} → {destino_str}, tentando Google...")
        distancia_km, tempo_min, rota_raw = buscar_rota_google(origem, destino)

    # 3️⃣ Se ainda falhar → fallback mínimo
    if not distancia_km or not tempo_min or not rota_raw:
        cursor.close()
        return _rota_minima(origem, destino, logger)

    # 4️⃣ Padroniza coordenadas
    rota_completa_dicts = [
        {"lat": float(lat), "lon": float(lon)}
        for lat, lon in rota_raw if lat is not None and lon is not None
    ]

    if not rota_completa_dicts:
        cursor.close()
        return _rota_minima(origem, destino, logger)

    # 5️⃣ Salva no cache
    rota_json = {
        "origem": origem_str,
        "destino": destino_str,
        "distancia_km": float(distancia_km),
        "tempo_min": float(tempo_min),
        "rota_completa": rota_completa_dicts
    }

    insert = """
        INSERT INTO cache_rotas (
            origem, destino, distancia_km, tempo_minutos, rota_json, tenant_id
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (origem, destino, tenant_id) DO NOTHING
    """
    cursor.execute(insert, (
        origem_str, destino_str,
        float(distancia_km), float(tempo_min),
        json.dumps(rota_json), tenant_id
    ))
    db_conn.commit()
    cursor.close()

    return float(distancia_km), float(tempo_min), rota_completa_dicts
