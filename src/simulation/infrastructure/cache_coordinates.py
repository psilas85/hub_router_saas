# infrastructure/cache_coordinates.py
import re

from geopy.geocoders import Nominatim

from simulation.utils.google_api import buscar_coords_google


PADRAO_CENTRO_DESCONHECIDO = re.compile(
    r"^Centro desconhecido "
    r"\((?P<lat>-?\d+(?:\.\d+)?),\s*(?P<lon>-?\d+(?:\.\d+)?)\)$"
)


def _extrair_coordenadas_de_centro_desconhecido(endereco):
    if not isinstance(endereco, str):
        return None

    match = PADRAO_CENTRO_DESCONHECIDO.match(endereco.strip())
    if not match:
        return None

    return float(match.group("lat")), float(match.group("lon"))


def _normalizar_coordenada(valor):
    if valor is None:
        return None

    try:
        return float(valor)
    except (TypeError, ValueError):
        return valor


def buscar_coordenadas(endereco, tenant_id, db_conn, logger):
    coordenadas_diretas = _extrair_coordenadas_de_centro_desconhecido(endereco)
    if coordenadas_diretas is not None:
        if logger:
            logger.info(
                "📍 Endereço sintético detectado; reutilizando coordenadas "
                f"do ponto denso para {endereco}"
            )
        return coordenadas_diretas

    if logger:
        cursor = db_conn.cursor()
        cursor.execute("SELECT current_database(), current_schema;")
        info = cursor.fetchone()
        logger.info(f"🧩 Conectado ao banco: {info[0]}, schema: {info[1]}")

    # 1. Verificar cache
    query = """
        SELECT latitude, longitude
        FROM cache_localizacoes
        WHERE endereco_completo = %s AND tenant_id = %s
    """
    cursor.execute(query, (endereco, tenant_id))
    row = cursor.fetchone()
    cursor.close()

    if row:
        if logger:
            logger.info(f"📍 Cache HIT (localização): {endereco}")
        return _normalizar_coordenada(row[0]), _normalizar_coordenada(row[1])

    if logger:
        logger.info(f"🔍 Cache MISS: {endereco} → tentando Nominatim...")

    # 2. Tentar Nominatim
    try:
        geolocator = Nominatim(user_agent="cluster_router_sim")
        location = geolocator.geocode(endereco, timeout=10)
        if location:
            salvar_localizacao_cache(
                db_conn,
                endereco,
                location.latitude,
                location.longitude,
                "nominatim",
                tenant_id,
            )
            return float(location.latitude), float(location.longitude)
    except Exception as e:
        if logger:
            logger.warning(f"Nominatim falhou: {e}")

    if logger:
        logger.info("⚠️ Nominatim falhou. Tentando Google Maps...")

    # 3. Tentar Google
    lat, lon = buscar_coords_google(endereco)
    if lat is not None and lon is not None:
        salvar_localizacao_cache(
            db_conn, endereco, lat, lon, "google", tenant_id
        )
        return _normalizar_coordenada(lat), _normalizar_coordenada(lon)

    if logger:
        logger.error(f"❌ Falha geral ao buscar coordenadas para: {endereco}")
    return (None, None)


def salvar_localizacao_cache(db_conn, endereco, lat, lon, fonte, tenant_id, cidade=None):
    cursor = db_conn.cursor()
    insert = """
        INSERT INTO cache_localizacoes (endereco_completo, latitude, longitude, fonte, tenant_id, cidade)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (endereco_completo, tenant_id) DO NOTHING
    """
    cursor.execute(insert, (endereco, lat, lon, fonte, tenant_id, cidade))
    db_conn.commit()
    cursor.close()



