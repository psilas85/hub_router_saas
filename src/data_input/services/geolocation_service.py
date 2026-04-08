# src/data_input/services/geolocation_service.py

import os
import re
import time
import requests
import random
import pandas as pd
import re

from data_input.utils.address_normalizer import normalize_address
from data_input.application.geo_validator import GeoValidator
from concurrent.futures import ThreadPoolExecutor, as_completed

import logging
logger = logging.getLogger(__name__)

class GeolocationService:

    def __init__(self, reader):
        self.reader = reader
        self.validator = GeoValidator()

        self.nominatim_url = os.getenv("NOMINATIM_LOCAL_URL")
        self.google_key = os.getenv("GMAPS_API_KEY")


        self.timeout = (
            float(os.getenv("GEOCODE_TIMEOUT", "5")),
            float(os.getenv("GEOCODE_READ_TIMEOUT", "10"))
        )
        self.max_retries = 2

    # ---------------------------------------------------------
    # VALIDAÇÃO NUMÉRICA
    # ---------------------------------------------------------
    def _is_valid(self, lat, lon):
        import math
        return (
            isinstance(lat, (int, float)) and
            isinstance(lon, (int, float)) and
            not math.isnan(lat) and
            not math.isnan(lon)
        )


    def _sanitize_string(self, s, field_name=""):
        """
        Sanitiza string para uso em geocoding.
        Remove lixo, normaliza espaços, retorna None se inválido.
        """

        if pd.isna(s) or s is None:
            return None

        s = str(s).strip().upper()

        # Remove espaços duplicados
        s = re.sub(r"\s+", " ", s)

        # Remove caracteres inválidos (mantém acentos)
        s = re.sub(r"[^\w\s\-ááéíóúàâêôãõçÁÉÍÓÚÀÂÊÔÃÕÇ]", " ", s)

        # Re-cleanup de espaços após remoção
        s = re.sub(r"\s+", " ", s).strip()

        if not s or len(s) < 2:
            logger.debug(f"[SANITIZE][EMPTY] field={field_name}")
            return None

        return s

    # ---------------------------------------------------------
    # NOMINATIM
    # ---------------------------------------------------------
    def _geocode_nominatim_structured(self, row):

        # 🔥 NOVO: Sanitiza strings

        # Normalização inspirada no SalesRouter
        street = self._sanitize_string(row.get("cte_rua"), "street")
        city = self._sanitize_string(row.get("cte_cidade"), "city")
        state = self._sanitize_string(row.get("cte_uf"), "state")

        # Remove duplicidade tipo "RUA RUA ..."
        if street:
            street = re.sub(r"^(RUA|AVENIDA|RODOVIA) \\1 ", r"\\1 ", street)
            street = re.sub(r"^(RUA|AVENIDA|RODOVIA) +", lambda m: m.group(1) + " ", street)
            street = re.sub(r"\b(RUA|AVENIDA|RODOVIA) +\\1 ", r"\\1 ", street)
            street = re.sub(r",", " ", street)
            street = re.sub(r"\s+", " ", street).strip()

        # Remove postalcode do request (padrão que funcionou melhor)
        params = {
            "street": street,
            "city": city,
            "state": state,
            "country": "Brazil",
            "format": "json",
            "limit": 1
        }

        headers = {
            "User-Agent": "HubRouter-Geocoder"
        }

        logger.info(f"[NOMINATIM][QUERY] params={params}")
        for retry_num in range(self.max_retries + 1):
            try:
                r = requests.get(
                    f"{self.nominatim_url}/search",
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )

                if r.status_code == 429:
                    logger.warning(f"[NOMINATIM][RATE_LIMIT] retry_num={retry_num} aguardando 2s")
                    time.sleep(2)
                    continue

                if r.status_code == 500:
                    logger.warning(f"[NOMINATIM][SERVER_ERROR] retry_num={retry_num} aguardando 1s")
                    time.sleep(1)
                    continue

                if r.status_code != 200:
                    logger.warning(f"[NOMINATIM][HTTP_{r.status_code}] params={params}")
                    return None

                try:
                    data = r.json()
                except Exception as e:
                    logger.warning(f"[NOMINATIM][INVALID_JSON] {e}")
                    return None


                if not data:
                    logger.info(f"[NOMINATIM][MISS] params={params}")
                    return None

                item = data[0]

                try:
                    lat = float(item["lat"])
                    lon = float(item["lon"])
                except (ValueError, KeyError) as e:
                    logger.warning(f"[NOMINATIM][INVALID_COORDS] erro={e} item={item}")
                    return None


                logger.info(f"[NOMINATIM][HIT] lat={lat} lon={lon} params={params}")

                return lat, lon, "nominatim_structured"

            except requests.exceptions.Timeout:
                if retry_num < self.max_retries:
                    # Backoff exponencial: retry0=1s, retry1=2s, retry2=4s + jitter
                    wait = (2 ** retry_num) + random.uniform(0, 1)
                    wait = min(wait, 10)  # Cap em 10s máximo
                    logger.warning(f"[NOMINATIM][TIMEOUT] retry_num={retry_num} espera {wait:.2f}s")
                    time.sleep(wait)
                    continue
                else:
                    logger.warning(f"[NOMINATIM][TIMEOUT] esgotadas retries")
                    return None

            except requests.exceptions.ConnectionError:
                if retry_num < self.max_retries:
                    wait = (2 ** retry_num) + random.uniform(0, 1)
                    wait = min(wait, 10)
                    logger.warning(f"[NOMINATIM][CONNECTION] retry_num={retry_num} espera {wait:.2f}s")
                    time.sleep(wait)
                    continue
                else:
                    logger.warning(f"[NOMINATIM][CONNECTION] esgotadas retries")
                    return None

            except Exception as e:
                logger.error(f"[NOMINATIM][UNKNOWN_ERROR] {e}")
                return None

        logger.warning(f"[NOMINATIM][FAILED_ALL_RETRIES]")
        return None

    # ---------------------------------------------------------
    # GOOGLE
    # ---------------------------------------------------------
    def _geocode_google(self, address):

        if not self.google_key:
            return None

        url = "https://maps.googleapis.com/maps/api/geocode/json"

        params = {
            "address": address,
            "key": self.google_key
        }

        for retry_num in range(self.max_retries + 1):
            try:
                r = requests.get(url, params=params, timeout=self.timeout)

                if r.status_code != 200:
                    logger.warning(f"[GOOGLE][HTTP_{r.status_code}] retry_num={retry_num}")
                    if retry_num < self.max_retries:
                        time.sleep(0.5)
                        continue
                    return None

                data = r.json()

                if data.get("status") != "OK":
                    logger.warning(f"[GOOGLE][API_ERROR] status={data.get('status')} retry_num={retry_num}")
                    if retry_num < self.max_retries:
                        time.sleep(0.5)
                        continue
                    return None

                loc = data["results"][0]["geometry"]["location"]

                logger.info(f"[GOOGLE][HIT] lat={loc['lat']} lon={loc['lng']}")
                return loc["lat"], loc["lng"], "google"

            except requests.exceptions.Timeout:
                if retry_num < self.max_retries:
                    wait = (2 ** retry_num) + random.uniform(0, 1)
                    wait = min(wait, 10)
                    logger.warning(f"[GOOGLE][TIMEOUT] retry_num={retry_num} espera {wait:.2f}s")
                    time.sleep(wait)
                    continue
                else:
                    logger.warning(f"[GOOGLE][TIMEOUT] esgotadas retries")
                    return None

            except requests.exceptions.ConnectionError:
                if retry_num < self.max_retries:
                    wait = (2 ** retry_num) + random.uniform(0, 1)
                    wait = min(wait, 10)
                    logger.warning(f"[GOOGLE][CONNECTION] retry_num={retry_num} espera {wait:.2f}s")
                    time.sleep(wait)
                    continue
                else:
                    logger.warning(f"[GOOGLE][CONNECTION] esgotadas retries")
                    return None

            except Exception as e:
                logger.error(f"[GOOGLE][ERROR] {e}")
                return None

        logger.warning(f"[GOOGLE][FAILED_ALL_RETRIES]")
        return None

    # ---------------------------------------------------------
    # BATCH (SEM PERSISTÊNCIA)
    # ---------------------------------------------------------

    def geocode_batch(self, df):

        logger.info(f"[GEOCODE_BATCH_START] total={len(df)}")

        df = df.copy()

        df["addr_norm"] = df["endereco_completo"].apply(normalize_address)

        df_unique = df.drop_duplicates("addr_norm")

        results = {}

        cache_batch = {}
        for addr_norm in df_unique["addr_norm"]:
            cached = self.reader.buscar_localizacao(addr_norm)
            if cached:
                cache_batch[addr_norm] = cached

        logger.info(f"[CACHE][PRELOAD] {len(cache_batch)}/{len(df_unique['addr_norm'])} hits em batch")

        # métricas
        cache_hit = 0
        cache_miss = 0
        nominatim_hit = 0
        google_hit = 0
        falha = 0

        # ---------------------------------------------------------
        # 🔥 FUNÇÃO THREAD SAFE (SEM ALTERAR DF)
        # ---------------------------------------------------------
        def processar_endereco(row):

            addr_norm = row["addr_norm"]

            # -----------------------------------------
            # CACHE
            # -----------------------------------------

            cached = cache_batch.get(addr_norm)

            if cached:
                lat = cached["latitude"]
                lon = cached["longitude"]
                src = "cache"
                logger.debug(f"[CACHE][HIT] addr={addr_norm[:40]}...")

                uf = row.get("cte_uf")
                status = self.validator.validar_ponto(lat, lon, uf)

                if status == "ok":
                    return addr_norm, lat, lon, src, "cache_hit"
            else:
                logger.debug(f"[CACHE][MISS] addr={addr_norm[:40]}...")

            # -----------------------------------------
            # NOMINATIM
            # -----------------------------------------
            res = self._geocode_nominatim_structured(row)

            if res:
                lat, lon, src = res
                origem = "nominatim"
                logger.info(f"[NOMINATIM][HIT] lat={lat} lon={lon}")
            else:
                logger.debug(f"[NOMINATIM][MISS] addr={addr_norm[:40]}...")

                # -----------------------------------------
                # GOOGLE
                # -----------------------------------------
                res = self._geocode_google(row["endereco_completo"])

                if res:
                    lat, lon, src = res
                    origem = "google"
                    logger.info(f"[GOOGLE][HIT] lat={lat} lon={lon}")
                else:
                    logger.warning(f"[GOOGLE][MISS] addr={addr_norm[:40]}...")
                    return addr_norm, None, None, "falha", "falha"

            # valida número
            if not self._is_valid(lat, lon):
                return addr_norm, None, None, "falha", "falha"

            # valida UF
            uf = row.get("cte_uf")
            status = self.validator.validar_ponto(
                lat,
                lon,
                uf
            )

            if status != "ok":
                return addr_norm, None, None, status, "falha"

            return addr_norm, lat, lon, src, origem

        # ---------------------------------------------------------
        # 🔥 PARALELISMO CONTROLADO
        # ---------------------------------------------------------
        max_workers = int(os.getenv("GEOCODE_THREADS", "8"))

        futures = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            for _, row in df_unique.iterrows():
                futures.append(executor.submit(processar_endereco, row))

            for future in as_completed(futures):

                addr_norm, lat, lon, src, origem = future.result()

                results[addr_norm] = (lat, lon, src)

                # métricas
                if origem == "cache_hit":
                    cache_hit += 1
                elif origem == "nominatim":
                    nominatim_hit += 1
                elif origem == "google":
                    google_hit += 1
                else:
                    falha += 1

        # ---------------------------------------------------------
        # 🔥 APLICA RESULTADO NO DF (FORA DAS THREADS)
        # ---------------------------------------------------------
        df["destino_latitude"] = df["addr_norm"].map(
            lambda x: results.get(x, (None, None, None))[0]
        )

        df["destino_longitude"] = df["addr_norm"].map(
            lambda x: results.get(x, (None, None, None))[1]
        )

        df["geocode_source"] = df["addr_norm"].map(
            lambda x: results.get(x, (None, None, None))[2]
        )

        df.drop(columns=["addr_norm"], inplace=True)

        logger.info(
            f"[STATS] cache={cache_hit} | nominatim={nominatim_hit} | google={google_hit} | falha={falha}"
        )

        total = len(df_unique)
        logger.info(
            f"[GEOCODE_BATCH_END] "
            f"total={total} | "
            f"cache={cache_hit} | "
            f"nominatim={nominatim_hit} | "
            f"google={google_hit} | "
            f"falha={falha}"
        )

        return df