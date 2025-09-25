#hub_router_1.0.1/src/clusterization/domain/geolocalizacao_service.py

import os
import requests
import time
import logging
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

from clusterization.domain.endereco_helper import EnderecoHelper
from clusterization.infrastructure.database_reader import DatabaseReader
from clusterization.infrastructure.database_writer import DatabaseWriter
from clusterization.config import UF_BOUNDS


class GeolocalizacaoService:
    def __init__(self, reader: DatabaseReader, writer: DatabaseWriter):
        self.reader = reader
        self.writer = writer
        self.geolocator = Nominatim(user_agent="cluster_router")
        self.GOOGLE_MAPS_API_KEY = os.getenv("GMAPS_API_KEY")
        self.cache_temporario = {}

    def coordenada_valida(self, lat, lon, uf):
        bounds = UF_BOUNDS.get(uf)
        if not bounds:
            logging.warning(f"⚠️ UF {uf} não possui limites definidos em UF_BOUNDS.")
            return False
        return (
            bounds["lat_min"] <= lat <= bounds["lat_max"]
            and bounds["lon_min"] <= lon <= bounds["lon_max"]
        )

    def buscar_coordenadas(self, endereco_original, uf_esperada=None):
        endereco_padronizado = EnderecoHelper.preprocessar(endereco_original)
        logging.debug(f"🔍 Endereço padronizado para busca: {endereco_padronizado}")

        if endereco_padronizado in self.cache_temporario:
            logging.debug(f"🧠 Recuperado do cache em memória: {endereco_padronizado}")
            lat_lon = self.cache_temporario[endereco_padronizado]
        else:
            lat_lon = self.reader.buscar_localizacao(endereco_padronizado)

            if lat_lon:
                logging.info(f"📦 Coordenadas recuperadas do banco: {endereco_padronizado} → {lat_lon}")
            else:
                logging.debug(f"🚫 Endereço não encontrado no banco: {endereco_padronizado}")
                lat_lon = self._buscar_lat_lon_nominatim(endereco_padronizado)
                if lat_lon:
                    logging.info(f"🌐 Coordenadas via Nominatim: {endereco_padronizado} → {lat_lon}")
                    self.writer.inserir_localizacao(endereco_padronizado, *lat_lon)
                else:
                    lat_lon = self._buscar_lat_lon_google(endereco_padronizado)
                    if lat_lon:
                        logging.info(f"🛰️ Coordenadas via Google Maps: {endereco_padronizado} → {lat_lon}")
                        self.writer.inserir_localizacao(endereco_padronizado, *lat_lon)

            if lat_lon:
                self.cache_temporario[endereco_padronizado] = lat_lon

        # ⚠️ Validação geográfica com base na UF informada
        if lat_lon and uf_esperada:
            lat, lon = lat_lon
            if not self.coordenada_valida(lat, lon, uf_esperada):
                logging.warning(f"❌ Coordenada fora da UF esperada ({uf_esperada}): {lat_lon} → ignorada.")
                return None

        time.sleep(1)
        return lat_lon

    def _buscar_lat_lon_nominatim(self, endereco, tentativas=3):
        for tentativa in range(tentativas):
            try:
                params = {
                    "q": endereco,
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1,  # 🔎 força trazer UF, cidade etc
                }
                response = requests.get(
                    "https://nominatim.openstreetmap.org/search",
                    params=params,
                    headers={"User-Agent": "cluster_router"},
                    timeout=30,  # ⏱ aumenta timeout
                )
                response.raise_for_status()
                results = response.json()

                if results:
                    r = results[0]
                    lat = float(r["lat"])
                    lon = float(r["lon"])
                    # ✅ validação adicional: checar se veio "CE"
                    uf = r.get("address", {}).get("state")
                    logging.debug(f"Nominatim raw → {r.get('display_name')} / UF detectada: {uf}")
                    return lat, lon
            except requests.exceptions.Timeout:
                logging.warning(f"⏱ Timeout Nominatim (tentativa {tentativa+1}/{tentativas})")
                time.sleep(2 * (tentativa + 1))  # ⏳ backoff progressivo
            except Exception as e:
                logging.error(f"Erro Nominatim: {e}")
                return None
        return None


    def _buscar_lat_lon_google(self, endereco):
        if not self.GOOGLE_MAPS_API_KEY:
            return None

        from urllib.parse import quote
        endereco_url = quote(endereco)

        url = f"https://maps.googleapis.com/maps/api/geocode/json?address={endereco_url}&key={self.GOOGLE_MAPS_API_KEY}"
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            if data["status"] == "OK":
                location = data["results"][0]["geometry"]["location"]
                return location["lat"], location["lng"]
        except Exception as e:
            logging.error(f"Erro Google Maps: {e}")
        return None
