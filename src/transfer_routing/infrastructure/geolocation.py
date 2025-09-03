# transfer_routing/infrastructure/geolocation.py

import requests
import os
from dotenv import load_dotenv

from transfer_routing.infrastructure.cache import (
    obter_rota_do_cache,
    save_route_to_cache
)
from transfer_routing.infrastructure.osrm_service import OSRMRouteService
from transfer_routing.logs.logging_factory import LoggerFactory

load_dotenv()

GOOGLE_MAPS_API_KEY = os.getenv("GMAPS_API_KEY")
OSRM_URL = os.getenv("OSRM_URL", "http://osrm:5000")

logger = LoggerFactory.get_logger(__name__)
osrm_service = OSRMRouteService(OSRM_URL)


def formatar_coord(coord: tuple) -> str:
    return f"{round(coord[0], 6)},{round(coord[1], 6)}"


def get_route(origem: tuple, destino: tuple, tenant_id: str, conn, logger=None):
    origem_str = formatar_coord(origem)
    destino_str = formatar_coord(destino)

    if origem_str == destino_str:
        if logger:
            logger.warning(f"Origem e destino são iguais: {origem_str}. Ignorando rota.")
        return None, None, []

    # 1️⃣ Cache no banco
    distancia_km, tempo_minutos, rota_json = obter_rota_do_cache(
        origem_str, destino_str, conn, logger=logger
    )
    if distancia_km is not None and tempo_minutos is not None:
        return distancia_km, tempo_minutos, rota_json

    # 2️⃣ OSRM
    if logger:
        logger.info(f"Tentando OSRM para {origem_str} -> {destino_str}")
    rota, distancia_km, tempo_minutos = osrm_service.consultar_rota(origem, destino)
    if distancia_km > 0:
        rota_json = {"coordenadas": rota}
        save_route_to_cache(
            origem_str, destino_str, tenant_id,
            distancia_km, tempo_minutos, rota_json, conn, logger=logger
        )
        return distancia_km, tempo_minutos, rota_json

    # 3️⃣ Google Maps
    if logger:
        logger.info(f"Tentando Google Maps para {origem_str} -> {destino_str}")
    try:
        url = "https://maps.googleapis.com/maps/api/directions/json"
        params = {
            "origin": origem_str,
            "destination": destino_str,
            "key": GOOGLE_MAPS_API_KEY,
            "mode": "driving"
        }

        response = requests.get(url, params=params)
        data = response.json()

        if data["status"] != "OK":
            if logger:
                logger.warning(f"Rota inválida ou erro na API: {data.get('error_message', 'Sem mensagem')}")
            return None, None, []

        route = data["routes"][0]
        legs = route["legs"][0]

        distancia_km = legs["distance"]["value"] / 1000
        tempo_minutos = legs["duration"]["value"] / 60

        coordenadas = [
            (step["end_location"]["lat"], step["end_location"]["lng"])
            for step in legs["steps"]
        ]
        overview_polyline = route.get("overview_polyline", {}).get("points", "")

        rota_json = {
            "coordenadas": coordenadas,
            "overview_polyline": overview_polyline
        }

        save_route_to_cache(
            origem_str, destino_str, tenant_id,
            distancia_km, tempo_minutos, rota_json, conn, logger=logger
        )
        return distancia_km, tempo_minutos, rota_json

    except Exception as e:
        if logger:
            logger.error(f"Erro ao obter rota da API Google Maps: {e}")
        return None, None, []

