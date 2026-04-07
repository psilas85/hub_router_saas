#hub_router_1.0.1/src/data_input/domain/municipio_polygon_validator.py

import json
import os
import re
import unicodedata
import logging
from pathlib import Path
from functools import lru_cache

from shapely.geometry import shape, Point

logger = logging.getLogger(__name__)

BUFFER_GRAUS = float(os.getenv("MUNICIPIO_BUFFER_GRAUS", "0.005"))

BASE_PATH = Path(
    os.getenv(
        "IBGE_MUNICIPIOS_GEOJSON",
        "/app/data/ibge/municipios.geojson"
    )
)

def _strip_accents(txt):
    txt = unicodedata.normalize("NFKD", txt)
    return "".join(c for c in txt if not unicodedata.combining(c))

def _norm(txt):
    if not txt:
        return None
    txt = str(txt).strip()
    txt = _strip_accents(txt)
    txt = re.sub(r"\s+", " ", txt)
    return txt.upper()

@lru_cache(maxsize=1)
def _load_polygons():

    if not BASE_PATH.exists():
        logger.warning(f"[POLYGON][FILE_NOT_FOUND] {BASE_PATH}")
        return {}

    try:
        with open(BASE_PATH, "r", encoding="utf-8") as f:
            geo = json.load(f)
    except Exception as e:
        logger.error(f"[POLYGON][LOAD_ERROR] {e}")
        return {}

    polygons = {}

    for feat in geo.get("features", []):
        props = feat.get("properties", {})

        cidade = _norm(props.get("NM_MUN"))
        uf = _norm(props.get("SIGLA_UF"))

        if not cidade or not uf:
            continue

        try:
            polygons[(cidade, uf)] = shape(feat["geometry"])
        except Exception:
            continue

    logger.info(f"[POLYGON][LOADED] total={len(polygons)}")

    return polygons


def ponto_dentro_municipio(lat, lon, cidade, uf):

    if lat is None or lon is None:
        return False

    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        return False

    cidade = _norm(cidade)
    uf = _norm(uf)

    if not cidade or not uf:
        return None

    polygons = _load_polygons()
    poly = polygons.get((cidade, uf))

    if poly is None:
        logger.warning(f"[POLYGON][NOT_FOUND] {cidade}-{uf}")
        return None

    ponto = Point(lon, lat)

    inside_strict = poly.contains(ponto)
    inside_buffer = poly.buffer(BUFFER_GRAUS).contains(ponto)

    if inside_strict:
        return True

    if inside_buffer:
        logger.warning(f"[POLYGON][BUFFER_HIT] {cidade}-{uf}")
        return True

    logger.warning(f"[POLYGON][OUTSIDE] {cidade}-{uf}")

    return False