#hub_router_1.0.1/src/data_input/application/reprocess_invalids_service.py

import pandas as pd
import logging
import requests
import os

from data_input.application.geo_validator import GeoValidator
from data_input.domain.municipio_polygon_validator import ponto_dentro_municipio
from data_input.utils.address_normalizer import normalize_address

logger = logging.getLogger(__name__)


# =========================================================
# 🔥 GOOGLE DIRETO
# =========================================================
def geocode_google_direto(endereco: str):

    api_key = os.getenv("GMAPS_API_KEY")

    if not api_key or not endereco:
        return None, None

    url = "https://maps.googleapis.com/maps/api/geocode/json"

    params = {
        "address": endereco,
        "key": api_key,
        "region": "br"
    }

    try:
        resp = requests.get(url, params=params, timeout=5)

        if resp.status_code != 200:
            return None, None

        data = resp.json()

        if data.get("status") == "OK":
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]

    except Exception as e:
        logger.warning(f"[GOOGLE_FAIL] {e}")

    return None, None


# =========================================================
# 🔥 SERVICE
# =========================================================
class ReprocessInvalidsService:

    def __init__(self, geolocation_service, database_writer):
        self.geo = geolocation_service
        self.writer = database_writer

    def _is_hard_fail(self, motivo: str) -> bool:
        """
        NUNCA reprocessar regras de negócio
        """
        return (
            "peso" in motivo or
            "dado_faltante" in motivo or
            "duplicado" in motivo or
            "cidade_uf_divergencia" in motivo
        )

    def execute(self, df_invalid: pd.DataFrame):
        """
        Reprocessa com 2 estratégias em cascata:
        1. Google (full address)
        2. City-level (só cidade+UF)
        """
        logger.info(f"[REPROCESS_START] total={len(df_invalid) if df_invalid is not None else 0}")

        if df_invalid is None or df_invalid.empty:
            logger.warning("[REPROCESS] DataFrame vazio")
            return pd.DataFrame(), df_invalid

        df_invalid = df_invalid.copy()

        reprocessados_validos = []
        reprocessados_invalidos = []

        for idx, row in df_invalid.iterrows():

            motivo = str(
                row.get("motivo_invalidade")
                or row.get("motivo")
                or ""
            ).lower()

            logger.debug(f"[REPROCESS][ROW_{idx}] motivo={motivo}")

            if self._is_hard_fail(motivo):
                logger.warning(f"[REPROCESS][HARD_FAIL] motivo={motivo}")
                reprocessados_invalidos.append(row)
                continue

            source = str(row.get("geocode_source") or "").strip().lower()
            if source not in ["cache", "nominatim", "nominatim_structured"]:
                logger.warning(f"[REPROCESS][SKIP_SOURCE] idx={idx} source={source}")
                reprocessados_invalidos.append(row)
                continue

            endereco = row.get("endereco_completo")
            cidade = row.get("cte_cidade")
            uf = row.get("cte_uf")

            if not endereco or len(str(endereco)) < 10:
                logger.warning(f"[REPROCESS][INVALID_ADDRESS] idx={idx}")
                reprocessados_invalidos.append(row)
                continue

            # ⭐ ESTRATÉGIA 1: Google full address
            logger.info(f"[FALLBACK][ESTRATEGIA_1] google_full idx={idx}")
            lat_g, lon_g = geocode_google_direto(endereco)

            if lat_g is not None and lon_g is not None:
                status = GeoValidator.validar_ponto(lat_g, lon_g, uf)
                if status == "ok":
                    row["destino_latitude"] = lat_g
                    row["destino_longitude"] = lon_g
                    row["geocode_source"] = "google_override"
                    row["motivo_invalidade"] = None
                    row["valido_municipio"] = True
                    logger.info(f"[FALLBACK][SUCCESS_1] idx={idx}")
                    reprocessados_validos.append(row)
                    continue
                logger.warning(f"[FALLBACK][POLY_FAIL_1] idx={idx} status={status}")

            # ⭐ ESTRATÉGIA 2: City-level (só cidade+UF)
            logger.info(f"[FALLBACK][ESTRATEGIA_2] city_uf idx={idx}")
            lat_c, lon_c = geocode_google_direto(f"{cidade}, {uf}, Brasil")

            if lat_c is not None and lon_c is not None:
                status = GeoValidator.validar_ponto(lat_c, lon_c, uf)
                if status == "ok":
                    row["destino_latitude"] = lat_c
                    row["destino_longitude"] = lon_c
                    row["geocode_source"] = "fallback_cidade"
                    row["motivo_invalidade"] = None
                    row["valido_municipio"] = True
                    row["status_validacao"] = "warning"
                    logger.info(f"[FALLBACK][SUCCESS_2] idx={idx}")
                    reprocessados_validos.append(row)
                    continue
                logger.warning(f"[FALLBACK][POLY_FAIL_2] idx={idx} status={status}")

            logger.error(f"[FALLBACK][FAILED_ALL] idx={idx} cidade={cidade} uf={uf}")
            reprocessados_invalidos.append(row)

        df_recuperados = pd.DataFrame(reprocessados_validos)
        df_invalid_final = pd.DataFrame(reprocessados_invalidos)

        logger.info(
            f"[REPROCESS_END] "
            f"recuperados={len(df_recuperados)} ({len(df_recuperados)/max(len(df_invalid),1)*100:.1f}%) | "
            f"mantidos_invalidos={len(df_invalid_final)}"
        )

        return df_recuperados, df_invalid_final