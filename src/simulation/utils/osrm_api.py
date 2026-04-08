# simulation/utils/osrm_api.py

import os
import requests

# Lê host e porta do .env, com valores padrão
OSRM_HOST = os.getenv("OSRM_HOST", "osrm_service")
OSRM_PORT = os.getenv("OSRM_PORT", "5000")
OSRM_MAX_SNAP_DISTANCE_METERS = float(os.getenv("OSRM_MAX_SNAP_DISTANCE_METERS", "5000"))


def _rota_osrm_invalida(data, origem, destino):
    waypoints = data.get("waypoints") or []
    if len(waypoints) >= 2:
        distancia_snap_origem = float(waypoints[0].get("distance") or 0.0)
        distancia_snap_destino = float(waypoints[1].get("distance") or 0.0)
        if (
            distancia_snap_origem > OSRM_MAX_SNAP_DISTANCE_METERS
            or distancia_snap_destino > OSRM_MAX_SNAP_DISTANCE_METERS
        ):
            print(
                "⚠️ OSRM descartado por snap distante demais | "
                f"origem={distancia_snap_origem:.1f}m | destino={distancia_snap_destino:.1f}m"
            )
            return True

    rota = (data.get("routes") or [None])[0]
    if not rota:
        return True

    lat1, lon1 = origem
    lat2, lon2 = destino
    mesma_origem_destino = lat1 == lat2 and lon1 == lon2
    distancia = float(rota.get("distance") or 0.0)
    duracao = float(rota.get("duration") or 0.0)

    if not mesma_origem_destino and (distancia <= 0.0 or duracao <= 0.0):
        print(
            "⚠️ OSRM descartado por rota zerada com origem/destino distintos | "
            f"origem={origem} | destino={destino}"
        )
        return True

    return False

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
        if _rota_osrm_invalida(data, origem, destino):
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

