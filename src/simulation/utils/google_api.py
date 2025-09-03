#simulation/utils/google_api.py

import googlemaps
import os
import requests

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_API_KEY")
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)

def buscar_coords_google(endereco: str):
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
