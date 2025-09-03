#last_mile_routing/infrastructure/geolocation_service.py

from geopy.distance import geodesic
import googlemaps
import os
import time


def calcular_distancia_km(ponto_a, ponto_b):
    return geodesic(ponto_a, ponto_b).km


import googlemaps
import time


class GoogleDirectionsService:
    def __init__(self, api_key):
        self.api_key = api_key
        self.gmaps = googlemaps.Client(key=api_key)

    def consultar_rota(self, origem, destino):
        """
        Consulta uma rota no Google Directions.

        origem e destino no formato (lat, lon)

        Retorna:
            - lista de coordenadas [{"lat": ..., "lon": ...}, ...]
            - distancia em km
            - tempo em minutos
        """
        try:
            origem_str = f"{origem[0]},{origem[1]}"
            destino_str = f"{destino[0]},{destino[1]}"

            response = self.gmaps.directions(
                origin=origem_str,
                destination=destino_str,
                mode="driving",
                language="pt-BR",
                region="br"
            )

            if not response:
                print(f"❌ Nenhuma rota encontrada para {origem} → {destino}")
                return [], 0, 0

            route = response[0]
            leg = route["legs"][0]
            distancia_km = leg["distance"]["value"] / 1000
            tempo_min = leg["duration"]["value"] / 60

            polyline = route["overview_polyline"]["points"]
            coordenadas = self._decode_polyline(polyline)

            return coordenadas, distancia_km, tempo_min

        except Exception as e:
            print(f"❌ Erro ao consultar rota Google Directions: {e}")
            time.sleep(2)  # backoff simples
            return [], 0, 0

    @staticmethod
    def _decode_polyline(polyline_str):
        """
        Decodifica a polyline do Google em lista de (lat, lon)
        """
        index = 0
        lat = 0
        lng = 0
        coordinates = []

        while index < len(polyline_str):
            result = 1
            shift = 0
            b = 0
            while True:
                b = ord(polyline_str[index]) - 63 - 1
                index += 1
                result += b << shift
                shift += 5
                if b < 0x1f:
                    break
            lat += ~(result >> 1) if result & 1 else (result >> 1)

            result = 1
            shift = 0
            while True:
                b = ord(polyline_str[index]) - 63 - 1
                index += 1
                result += b << shift
                shift += 5
                if b < 0x1f:
                    break
            lng += ~(result >> 1) if result & 1 else (result >> 1)

            coordinates.append((lat * 1e-5, lng * 1e-5))

        return [{"lat": c[0], "lon": c[1]} for c in coordinates]
