#hub_router_1.0.1/src/data_input/application/reprocess_invalids_service.py

import pandas as pd
import logging
import requests
import os

from data_input.domain.municipio_polygon_validator import (
    get_municipio_centroid,
    ponto_dentro_municipio,
)

logger = logging.getLogger(__name__)


REPROCESS_GOOGLE_SOURCES = {"cache", "nominatim", "nominatim_structured"}
DIRECT_FALLBACK_SOURCES = {"google", "google_override"}


def _validar_por_poligono(lat, lon, cidade, uf):
    inside = ponto_dentro_municipio(lat, lon, cidade, uf)
    return "ok" if inside is True else "falha"


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
        Reprocessa em cascata:
        1. cache/nominatim -> Google full address -> polígono
        2. google ou falha do passo 1 -> centroide do município -> polígono
        """
        total_invalid = len(df_invalid) if df_invalid is not None else 0
        logger.info(f"[REPROCESS_START] total={total_invalid}")

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
            run_google_full = source in REPROCESS_GOOGLE_SOURCES
            fallback_sources = REPROCESS_GOOGLE_SOURCES.union(
                DIRECT_FALLBACK_SOURCES
            )
            run_centroid_fallback = source in fallback_sources

            if not run_centroid_fallback:
                logger.warning(
                    f"[REPROCESS][SKIP_SOURCE] idx={idx} source={source}"
                )
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
            if run_google_full:
                logger.info(
                    f"[FALLBACK][ESTRATEGIA_1] google_full idx={idx} "
                    f"endereco='{endereco}'"
                )
                lat_g, lon_g = geocode_google_direto(endereco)
                logger.info(
                    f"[FALLBACK][GOOGLE_RESPONSE] idx={idx} "
                    f"lat={lat_g} lon={lon_g}"
                )

                if lat_g is not None and lon_g is not None:
                    status = _validar_por_poligono(lat_g, lon_g, cidade, uf)
                    logger.info(
                        f"[FALLBACK][GOOGLE_VALIDATION] idx={idx} "
                        f"status={status}"
                    )
                    if status == "ok":
                        row["destino_latitude"] = lat_g
                        row["destino_longitude"] = lon_g
                        row["geocode_source"] = "google_override"
                        row["motivo_invalidade"] = None
                        row["valido_municipio"] = True
                        logger.info(f"[FALLBACK][SUCCESS_1] idx={idx}")
                        reprocessados_validos.append(row)
                        continue
                    logger.warning(
                        f"[FALLBACK][POLY_FAIL_1] idx={idx} status={status}"
                    )

            # ⭐ ESTRATÉGIA 2: centroide do município
            logger.info(
                f"[FALLBACK][ESTRATEGIA_2] municipio_centroid idx={idx} "
                f"cidade='{cidade}' uf='{uf}'"
            )
            lat_c, lon_c = get_municipio_centroid(cidade, uf)
            logger.info(
                f"[FALLBACK][CITY_UF_RESPONSE] idx={idx} "
                f"lat={lat_c} lon={lon_c}"
            )

            if lat_c is not None and lon_c is not None:
                status = _validar_por_poligono(lat_c, lon_c, cidade, uf)
                logger.info(
                    f"[FALLBACK][CITY_UF_VALIDATION] idx={idx} "
                    f"status={status}"
                )
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
                logger.warning(
                    f"[FALLBACK][POLY_FAIL_2] idx={idx} status={status}"
                )

            logger.error(
                f"[FALLBACK][FAILED_ALL] idx={idx} cidade={cidade} uf={uf}"
            )
            reprocessados_invalidos.append(row)

        df_recuperados = pd.DataFrame(reprocessados_validos)
        df_invalid_final = pd.DataFrame(reprocessados_invalidos)

        recovered_pct = len(df_recuperados) / max(len(df_invalid), 1) * 100
        logger.info(
            f"[REPROCESS_END] "
            f"recuperados={len(df_recuperados)} ({recovered_pct:.1f}%) | "
            f"mantidos_invalidos={len(df_invalid_final)}"
        )

        return df_recuperados, df_invalid_final
