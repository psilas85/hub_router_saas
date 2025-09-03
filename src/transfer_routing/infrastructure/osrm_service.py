#transfer_routing/infrastructure/osrm_service.py

import requests

class OSRMRouteService:
    def __init__(self, osrm_url="http://osrm:5000"):
        self.osrm_url = osrm_url.rstrip("/")

    def consultar_rota(self, origem, destino):
        try:
            coords = f"{origem[1]},{origem[0]};{destino[1]},{destino[0]}"
            url = f"{self.osrm_url}/route/v1/driving/{coords}"
            params = {"overview": "full", "geometries": "geojson"}
            r = requests.get(url, params=params, timeout=5)
            r.raise_for_status()
            data = r.json()

            if not data.get("routes"):
                return [], 0, 0

            route = data["routes"][0]
            distancia_km = route["distance"] / 1000
            tempo_min = route["duration"] / 60
            coordenadas = [{"lat": lat, "lon": lon} for lon, lat in route["geometry"]["coordinates"]]
            return coordenadas, distancia_km, tempo_min

        except Exception as e:
            print(f"❌ Erro no OSRM: {e}")
            return [], 0, 0
