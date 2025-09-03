# infrastructure/cache_coordinates.py
from geopy.geocoders import Nominatim
from simulation.utils.google_api import buscar_coords_google
import json

from geopy.geocoders import Nominatim
from simulation.utils.google_api import buscar_coords_google
import json

from geopy.geocoders import Nominatim
from simulation.utils.google_api import buscar_coords_google
import json

def buscar_coordenadas(endereco, tenant_id, db_conn, logger):
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
        if logger: logger.info(f"📍 Cache HIT (localização): {endereco}")
        return row[0], row[1]

    if logger: logger.info(f"🔍 Cache MISS: {endereco} → tentando Nominatim...")

    # 2. Tentar Nominatim
    try:
        geolocator = Nominatim(user_agent="cluster_router_sim")
        location = geolocator.geocode(endereco, timeout=10)
        if location:
            salvar_localizacao_cache(db_conn, endereco, location.latitude, location.longitude, "nominatim", tenant_id)
            return location.latitude, location.longitude
    except Exception as e:
        if logger: logger.warning(f"Nominatim falhou: {e}")

    if logger: logger.info(f"⚠️ Nominatim falhou. Tentando Google Maps...")

    # 3. Tentar Google
    lat, lon = buscar_coords_google(endereco)
    if lat is not None and lon is not None:
        salvar_localizacao_cache(db_conn, endereco, lat, lon, "google", tenant_id)
        return lat, lon

    if logger: logger.error(f"❌ Falha geral ao buscar coordenadas para: {endereco}")
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



