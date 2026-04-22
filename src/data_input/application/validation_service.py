# src/data_input/application/validation_service.py

import pandas as pd
import json
import os
from pathlib import Path
from functools import lru_cache
import unicodedata
import re
import logging
import time

from shapely.geometry import Point

from data_input.config.config import UF_BOUNDS
from data_input.domain.municipio_polygon_validator import (
    ponto_dentro_municipio,
    _load_polygons,
    _get_polygon_context,
    _norm
)

logger = logging.getLogger(__name__)


def _strip_accents_val(txt):
    txt = unicodedata.normalize("NFKD", txt)
    return "".join(c for c in txt if not unicodedata.combining(c))


def _norm_val(txt):
    """Alias para compatibilidade - usa _norm importada do municipio_polygon_validator."""
    return _norm(txt)


@lru_cache(maxsize=1)
def _load_cidade_uf_map():
    """Carrega mapa de (cidade, uf) válidos do geojson."""
    polygons = _load_polygons()
    # Extrai um set de (cidade, uf) válidos dos polígonos carregados
    return set(polygons.keys())


def _valida_cidade_uf(cidade, uf):
    """Verifica se a combinação cidade-uf é válida."""
    if not cidade or not uf:
        return None

    cidade = _norm(cidade)
    uf = _norm(uf)

    if not cidade or not uf:
        return None

    valid_pairs = _load_cidade_uf_map()
    return (cidade, uf) in valid_pairs


class ValidationService:

    REQUIRED = [
        "cte_numero",
        "cte_cidade",
        "cte_uf",
        "cte_peso",
        "envio_data",
        # "cte_cep",  # Agora opcional
        # "cte_volumes",  # Agora opcional
    ]

    def execute(self, df: pd.DataFrame):

        df = df.copy()

        # ---------------------------
        # NORMALIZA
        # ---------------------------
        for col in self.REQUIRED:
            if col not in df.columns:
                df[col] = None

        df["destino_latitude"] = df.get("destino_latitude")
        df["destino_longitude"] = df.get("destino_longitude")

        df = df.replace({"": None, "nan": None, "None": None})

        df["motivo_invalidade"] = None

        # ---------------------------
        # 1. CAMPOS OBRIGATÓRIOS
        # ---------------------------
        mask_required = df[self.REQUIRED].isnull().any(axis=1)
        df.loc[mask_required, "motivo_invalidade"] = "dado_faltante"

        # ---------------------------
        # 1.5 DIVERGÊNCIA CIDADE x UF (HARD FAIL) - LOTE VETORIZADO
        # ---------------------------
        mask_already_invalid = df["motivo_invalidade"].notna()
        mask_need_check = ~mask_already_invalid

        if mask_need_check.any():
            # ⚡ PRÉ-CARREGAR MAPA UMA ÚNICA VEZ
            valid_pairs = _load_cidade_uf_map()
            valid_pairs_set = set(valid_pairs)

            df_check = df[mask_need_check].copy()

            # Normalizar cidade+uf em lote (usando _norm importada)
            df_check["cidade_norm"] = df_check["cte_cidade"].apply(_norm)
            df_check["uf_norm"] = df_check["cte_uf"].apply(_norm)

            # Criar tuple (cidade, uf) e verificar em lote contra set
            df_check["cidade_uf_tuple"] = list(zip(df_check["cidade_norm"], df_check["uf_norm"]))
            df_check["cidade_uf_valido"] = df_check["cidade_uf_tuple"].isin(valid_pairs_set)

            # Aplicar resultado ao dataframe original
            mask_cidade_uf_invalido = ~df_check["cidade_uf_valido"]
            df.loc[df[mask_need_check].index[mask_cidade_uf_invalido], "motivo_invalidade"] = "cidade_uf_divergencia"

        # ---------------------------
        # 2. PESO / VOLUME

        mask_peso = df["cte_peso"].fillna(0) <= 0
        # Volume agora realmente opcional: não invalida se vazio ou zero
        df.loc[mask_peso, "motivo_invalidade"] = "peso_invalido"

        # ---------------------------
        # 3. DUPLICIDADE CTE
        # ---------------------------
        duplicados = df.duplicated(subset=["cte_numero"], keep=False)
        df.loc[duplicados, "motivo_invalidade"] = "cte_duplicado"
        # Log detalhado dos duplicados
        if duplicados.any():
            cte_duplicados = df.loc[duplicados, "cte_numero"]
            contagem = cte_duplicados.value_counts()
            logger.warning("[VALIDACAO][DUPLICADOS] cte_numero duplicados e suas contagens:")
            for cte, count in contagem.items():
                logger.warning(f"[VALIDACAO][DUPLICADO] cte_numero={cte} count={count}")

        # ---------------------------
        # 4. GEOCODE
        # ---------------------------
        mask_geo = (
            df["destino_latitude"].isnull() |
            df["destino_longitude"].isnull()
        )
        df.loc[mask_geo, "motivo_invalidade"] = "geocode_falha"

        if "cte_numero_endereco" not in df.columns:
            df["cte_numero_endereco"] = None

        numero_text = (
            df["cte_numero_endereco"]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )
        rua_text = df["cte_rua"].fillna("").astype(str).str.strip()
        cidade_text = df["cte_cidade"].fillna("").astype(str).str.strip()
        uf_text = df["cte_uf"].fillna("").astype(str).str.strip()

        mask_numero_desmembrado = df["cte_numero_endereco"].notna()
        mask_numero_invalido = (
            mask_numero_desmembrado &
            (
                numero_text.isin(["", "S/N", "SN", "N/A", "NONE", "-"])
                | rua_text.eq("")
                | cidade_text.eq("")
                | uf_text.eq("")
            )
        )
        df.loc[mask_numero_invalido, "motivo_invalidade"] = "dado_faltante_endereco"

        # ---------------------------
        # 5. UF BOUNDING BOX
        # ---------------------------
        def validar_uf(row):
            uf = str(row["cte_uf"]).upper()
            lat = row["destino_latitude"]
            lon = row["destino_longitude"]

            if pd.isna(lat) or pd.isna(lon):
                return False

            bounds = UF_BOUNDS.get(uf)

            if not bounds:
                return False

            return (
                bounds["lat_min"] <= lat <= bounds["lat_max"]
                and bounds["lon_min"] <= lon <= bounds["lon_max"]
            )

        mask_uf = ~df.apply(validar_uf, axis=1)
        df.loc[mask_uf, "motivo_invalidade"] = "fora_uf"

        # =========================================================
        # 🔥 6. MUNICÍPIO (VETORIZADO) - PRÉ-CARREGAR POLÍGONOS
        # =========================================================

        df_valid = df[df["motivo_invalidade"].isna()].copy()
        df_invalid = df[df["motivo_invalidade"].notna()].copy()

        if not df_valid.empty:
            municipio_validation_start = time.perf_counter()

            df_valid["cidade_norm"] = df_valid["cte_cidade"].apply(_norm)
            df_valid["uf_norm"] = df_valid["cte_uf"].apply(_norm)
            df_valid["valido_municipio"] = False

            grupos_municipio = df_valid.groupby(["cidade_norm", "uf_norm"], sort=False)

            for (cidade_norm, uf_norm), group in grupos_municipio:
                if not cidade_norm or not uf_norm:
                    continue

                polygon_context = _get_polygon_context(cidade_norm, uf_norm)

                if polygon_context is None:
                    logger.debug(f"[POLYGON][NOT_FOUND] {cidade_norm}-{uf_norm}")
                    continue

                prepared_poly = polygon_context["prepared_poly"]
                prepared_buffered_poly = polygon_context["prepared_buffered_poly"]

                group_valid_indexes = []

                for row in group.itertuples():
                    lat = row.destino_latitude
                    lon = row.destino_longitude

                    if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
                        continue

                    point_lon = float(lon)
                    point_lat = float(lat)
                    ponto = Point(point_lon, point_lat)
                    inside_strict = prepared_poly.contains(ponto)
                    inside_buffer = prepared_buffered_poly.contains(ponto)

                    if inside_strict:
                        group_valid_indexes.append(row.Index)
                        continue

                    if inside_buffer:
                        logger.info(
                            f"[POLYGON][BUFFER] {cidade_norm}-{uf_norm} "
                            f"lat={lat} lon={lon}"
                        )
                        group_valid_indexes.append(row.Index)

                if group_valid_indexes:
                    df_valid.loc[group_valid_indexes, "valido_municipio"] = True

            df_invalid_municipio = df_valid[~df_valid["valido_municipio"]].copy()
            df_valid = df_valid[df_valid["valido_municipio"]].copy()

            logger.info(
                "[POLYGON][VALIDATION_DONE] "
                f"validos={len(df_valid)} invalidos={len(df_invalid_municipio)} "
                f"tempo={time.perf_counter() - municipio_validation_start:.2f}s"
            )

            df_invalid_municipio["motivo_invalidade"] = "fora_municipio"

            df_invalid = pd.concat(
                [df_invalid, df_invalid_municipio],
                ignore_index=True
            )

        return df_valid, df_invalid