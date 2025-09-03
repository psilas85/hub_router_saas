# simulation/utils/osrm_api.py

import os
import requests

# Lê host e porta do .env, com valores padrão
OSRM_HOST = os.getenv("OSRM_HOST", "osrm_service")
OSRM_PORT = os.getenv("OSRM_PORT", "5000")

import os
import requests

# Lê host e porta do .env, com valores padrão
OSRM_HOST = os.getenv("OSRM_HOST", "osrm_service")
OSRM_PORT = os.getenv("OSRM_PORT", "5000")

def buscar_rota_osrm(origem: tuple, destino: tuple):
    """
    Busca rota no OSRM.

    Args:
        origem (tuple): (lat, lon)
        destino (tuple): (lat, lon)

    Returns:
        tuple: (distancia_km, tempo_min, rota_completa)
    """
    try:
        lat1, lon1 = origem
        lat2, lon2 = destino

        url = (
            f"http://{OSRM_HOST}:{OSRM_PORT}/route/v1/driving/"
            f"{lon1},{lat1};{lon2},{lat2}?overview=full&geometries=geojson"
        )

        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None, None, []

        data = response.json()
        if "routes" not in data or not data["routes"]:
            return None, None, []

        route = data["routes"][0]
        distancia_km = round(route["distance"] / 1000, 2)
        tempo_min = round(route["duration"] / 60, 2)

        # Usa geometry diretamente, que sempre vem preenchido
        rota_completa = [(lat, lon) for lon, lat in route["geometry"]["coordinates"]]

        return distancia_km, tempo_min, rota_completa

    except Exception as e:
        print(f"❌ Erro no OSRM: {e}")
        return None, None, []

