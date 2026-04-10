#simulation/utils/google_api.py

import googlemaps
import os
import requests

GOOGLE_MAPS_API_KEY = (
    os.getenv("GOOGLE_API_KEY")
    or os.getenv("GOOGLE_MAPS_API_KEY")
    or os.getenv("GMAPS_API_KEY")
)


def _get_gmaps_client():
    if not GOOGLE_MAPS_API_KEY:
        return None

    try:
        return googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    except Exception:
        return None

def buscar_coords_google(endereco: str):
    gmaps = _get_gmaps_client()
    if gmaps is None:
        return None, None

    try:
        resultado = gmaps.geocode(endereco)
        if resultado:
            location = resultado[0]['geometry']['location']
            componentes = resultado[0].get('address_components', [])
            pais = next((c['short_name'] for c in componentes if 'country' in c['types']), None)
            if pais == 'BR':
                return location['lat'], location['lng']
    except Exception as e:
        print(f"⚠️ Erro ao buscar coordenadas no Google: {e}")
    return None, None


def buscar_endereco_google(lat: float, lon: float):
    if not GOOGLE_MAPS_API_KEY:
        return None, None

    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {
        "latlng": f"{lat},{lon}",
        "language": "pt-BR",
        "key": GOOGLE_MAPS_API_KEY,
    }

    try:
        response = requests.get(base_url, params=params, timeout=10)
        if response.status_code != 200:
            return None, None

        data = response.json()
        if data.get("status") != "OK" or not data.get("results"):
            return None, None

        resultado = data["results"][0]
        cidade = "Desconhecido"
        for componente in resultado.get("address_components", []):
            tipos = set(componente.get("types", []))
            if {
                "administrative_area_level_2",
                "political",
            }.issubset(tipos) or "locality" in tipos:
                cidade = componente.get("long_name") or cidade
                break

        return resultado.get("formatted_address"), cidade
    except Exception:
        return None, None


from googlemaps.convert import decode_polyline

def buscar_rota_google(origem: tuple, destino: tuple):
    """
    Consulta a API do Google Maps Directions para obter:
    - distância em km
    - tempo em minutos
    - rota completa como lista de [lat, lon]
    """
    base_url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": f"{origem[0]},{origem[1]}",
        "destination": f"{destino[0]},{destino[1]}",
        "mode": "driving",
        "key": GOOGLE_MAPS_API_KEY
    }

    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        return None, None, []

    data = response.json()
    if not data["routes"]:
        return None, None, []

    route = data["routes"][0]
    leg = route["legs"][0]

    distancia_km = leg["distance"]["value"] / 1000
    tempo_min = leg["duration"]["value"] / 60

    # 🚀 Novo: rota detalhada com overview_polyline
    polyline_str = route.get("overview_polyline", {}).get("points", "")
    rota_completa = [[p["lat"], p["lng"]] for p in decode_polyline(polyline_str)] if polyline_str else []

    return distancia_km, tempo_min, rota_completa
