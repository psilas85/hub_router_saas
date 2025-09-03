#simulation/utils/nominatim_api.py

import requests

def buscar_rota_nominatim(origem: tuple, destino: tuple):
    try:
        url = f"http://router.project-osrm.org/route/v1/driving/{origem[1]},{origem[0]};{destino[1]},{destino[0]}?overview=full&geometries=geojson"
        resposta = requests.get(url)
        dados = resposta.json()

        if dados and dados["code"] == "Ok":
            rota = dados["routes"][0]
            distancia_km = rota["distance"] / 1000
            duracao_min = rota["duration"] / 60
            rota_completa = rota["geometry"]["coordinates"]
            # OSRM retorna (lon, lat) — inverta para (lat, lon)
            rota_latlon = [(lat, lon) for lon, lat in rota_completa]
            return distancia_km, duracao_min, rota_latlon
    except:
        pass

    return None, None, []
