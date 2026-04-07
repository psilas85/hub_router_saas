# src/data_input/application/validation_service.py

import pandas as pd
import json
import os
from pathlib import Path
from functools import lru_cache
import unicodedata
import re
import logging

from data_input.config.config import UF_BOUNDS
from data_input.domain.municipio_polygon_validator import (
    ponto_dentro_municipio,
    _load_polygons,
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
        "cte_cep",
        "cte_peso",
        "cte_volumes",
        "envio_data",
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
        # ---------------------------
        mask_peso = df["cte_peso"].fillna(0) <= 0
        mask_vol = df["cte_volumes"].fillna(0) <= 0

        df.loc[mask_peso, "motivo_invalidade"] = "peso_invalido"
        df.loc[mask_vol, "motivo_invalidade"] = "volume_invalido"

        # ---------------------------
        # 3. DUPLICIDADE CTE
        # ---------------------------
        duplicados = df.duplicated(subset=["cte_numero"], keep=False)
        df.loc[duplicados, "motivo_invalidade"] = "cte_duplicado"

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

            # ⚡ PRÉ-CARREGAR POLÍGONOS UMA ÚNICA VEZ
            polygons = _load_polygons()

            # Criar função que usa polygons pré-carregados
            def validar_municipio_otimizado(row):
                lat = row.get("destino_latitude")
                lon = row.get("destino_longitude")
                cidade = row.get("cte_cidade")
                uf = row.get("cte_uf")

                if lat is None or lon is None:
                    return None

                cidade = _norm(cidade)
                uf = _norm(uf)

                if not cidade or not uf:
                    return None

                poly = polygons.get((cidade, uf))

                if poly is None:
                    logger.debug(f"[POLYGON][NOT_FOUND] {cidade}-{uf}")
                    return None

                from shapely.geometry import Point
                BUFFER_GRAUS = float(os.getenv("MUNICIPIO_BUFFER_GRAUS", "0.005"))

                ponto = Point(lon, lat)
                inside_strict = poly.contains(ponto)
                inside_buffer = poly.buffer(BUFFER_GRAUS).contains(ponto)

                if inside_strict:
                    return True
                elif inside_buffer:
                    logger.info(f"[POLYGON][BUFFER] {cidade}-{uf} lat={lat} lon={lon}")
                    return True
                else:
                    return False

            # Aplicar validação em lote, mas com polygons pré-carregados
            mask_municipio = df_valid.apply(validar_municipio_otimizado, axis=1)

            df_valid["valido_municipio"] = mask_municipio

            df_invalid_municipio = df_valid[~df_valid["valido_municipio"]].copy()
            df_valid = df_valid[df_valid["valido_municipio"]].copy()

            df_invalid_municipio["motivo_invalidade"] = "fora_municipio"

            df_invalid = pd.concat(
                [df_invalid, df_invalid_municipio],
                ignore_index=True
            )

        return df_valid, df_invalid