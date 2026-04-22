#hub_router_1.0.1/src/data_input/domain/municipio_polygon_validator.py

import json
import os
import re
import unicodedata
import logging
from pathlib import Path
from functools import lru_cache

from shapely.geometry import shape, Point
from shapely.prepared import prep

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BUFFER_GRAUS = float(os.getenv("MUNICIPIO_BUFFER_GRAUS", "0.02"))

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


@lru_cache(maxsize=None)
def _get_polygon_context(cidade, uf):

    cidade = _norm(cidade)
    uf = _norm(uf)

    if not cidade or not uf:
        return None

    poly = _load_polygons().get((cidade, uf))

    if poly is None:
        return None

    try:
        buffered_poly = poly.buffer(BUFFER_GRAUS)
        return {
            "poly": poly,
            "buffered_poly": buffered_poly,
            "prepared_poly": prep(poly),
            "prepared_buffered_poly": prep(buffered_poly),
        }
    except Exception:
        return None


def ponto_dentro_municipio(lat, lon, cidade, uf):
    print(f"[DEBUG][POLYGON] lat={lat} lon={lon} cidade={cidade} uf={uf}")

    if lat is None or lon is None:
        logger.warning(f"[POLYGON][INVALID_COORDS] lat={lat} lon={lon}")
        return False

    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        logger.warning(f"[POLYGON][COORD_CAST_FAIL] lat={lat} lon={lon}")
        return False

    cidade_orig = cidade
    uf_orig = uf
    cidade = _norm(cidade)
    uf = _norm(uf)

    if not cidade or not uf:
        logger.warning(f"[POLYGON][NORM_FAIL] cidade='{cidade_orig}' uf='{uf_orig}'")
        print(f"[DEBUG][POLYGON] NORM_FAIL cidade_orig='{cidade_orig}' uf_orig='{uf_orig}' cidade='{cidade}' uf='{uf}'")
        return None

    polygon_context = _get_polygon_context(cidade, uf)

    if polygon_context is None:
        logger.warning(f"[POLYGON][NOT_FOUND] cidade='{cidade}' uf='{uf}' (orig: '{cidade_orig}'/'{uf_orig}')")
        print(f"[DEBUG][POLYGON] NOT_FOUND cidade='{cidade}' uf='{uf}' (orig: '{cidade_orig}'/'{uf_orig}')")
        return None

    ponto = Point(lon, lat)

    inside_strict = polygon_context["prepared_poly"].contains(ponto)
    inside_buffer = polygon_context["prepared_buffered_poly"].contains(ponto)

    logger.info(f"[POLYGON][CHECK] cidade='{cidade}' uf='{uf}' lat={lat} lon={lon} inside_strict={inside_strict} inside_buffer={inside_buffer} buffer={BUFFER_GRAUS}")
    print(f"[DEBUG][POLYGON] CHECK cidade='{cidade}' uf='{uf}' lat={lat} lon={lon} inside_strict={inside_strict} inside_buffer={inside_buffer} buffer={BUFFER_GRAUS}")

    if inside_strict:
        logger.info(f"[POLYGON][INSIDE] cidade='{cidade}' uf='{uf}' lat={lat} lon={lon}")
        print(f"[DEBUG][POLYGON] INSIDE cidade='{cidade}' uf='{uf}' lat={lat} lon={lon}")
        return True

    if inside_buffer:
        logger.warning(f"[POLYGON][BUFFER_HIT] cidade='{cidade}' uf='{uf}' lat={lat} lon={lon}")
        print(f"[DEBUG][POLYGON] BUFFER_HIT cidade='{cidade}' uf='{uf}' lat={lat} lon={lon}")
        return True

    logger.warning(f"[POLYGON][OUTSIDE] cidade='{cidade}' uf='{uf}' lat={lat} lon={lon}")
    print(f"[DEBUG][POLYGON] OUTSIDE cidade='{cidade}' uf='{uf}' lat={lat} lon={lon}")
    return False