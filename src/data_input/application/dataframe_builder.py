# hub_router/src/data_input/application/dataframe_builder.py

import pandas as pd
import numpy as np
import re
import logging
logger = logging.getLogger(__name__)

class DataFrameBuilder:

    MAP = {
        "Filial": "transportadora",
        "Remetente": "remetente_nome",
        "CNPJ/CPF": "remetente_cnpj",
        "Origem": "remetente_cidade",
        "UF Origem": "remetente_uf",
        "Destinatário": "destinatario_nome",
        "CNPJ/CPF.1": "destinatario_cnpj",
        "Logradouro": "cte_rua",
        "Numero": "cte_numero_endereco",
        "Numero Cte": "cte_numero",
        "Número Cte": "cte_numero",
        "Nº Cte": "cte_numero",
        "Bairro": "cte_bairro",
        "Complemento": "cte_complemento",
        "Cidade": "cte_cidade",
        "UF Entrega": "cte_uf",
        "CEP": "cte_cep",
        "NF": "cte_nf",
        "Volumes": "cte_volumes",
        "Kg Taxado": "cte_peso",
        "Valor NF": "cte_valor_nf",
        "Valor Frete": "cte_valor_frete",
        "Doc/Min": "doc_min",
        "Data Frete": "envio_data",
        "Prazo Entrega": "cte_prazo_min",
        "Tempo Atendimento": "cte_tempo_atendimento_min",
    }

    # ---------------------------------------------------------
    # 🔧 MONTAGEM DE ENDEREÇO (PADRÃO LIMPO)
    # ---------------------------------------------------------
    def _montar_endereco(self, rua, numero, bairro, cidade, uf):
        if not rua:
            return None

        rua = str(rua).strip()
        numero_raw = numero

        # remove espaços duplicados
        rua = re.sub(r"\s+", " ", rua)

        # 🔥 COMPATIBILIDADE CACHE: só adiciona número se válido
        if numero_raw is not None:
            numero = str(numero_raw).strip()
            if numero.upper() not in ["S/N", "SN", "N/A", "NONE", "-", ""]:
                # Evita duplicação se número já estiver na rua
                numero_pattern = re.escape(numero)
                if not re.search(r'\b' + numero_pattern + r'\b', rua, re.IGNORECASE):
                    rua = f"{rua} {numero}"

        # Limpeza final
        rua = rua.replace("S/N", "").replace("SN", "").strip()

        if not rua:
            return None

        bairro = str(bairro).strip() if bairro else ""
        cidade = str(cidade).strip() if cidade else ""
        uf = str(uf).strip() if uf else ""

        if not cidade or not uf:
            return None

        # Monta endereço sem vírgula extra se bairro estiver vazio
        if bairro:
            endereco = f"{rua}, {bairro}, {cidade} - {uf}"
        else:
            endereco = f"{rua}, {cidade} - {uf}"

        # Remove vírgulas duplicadas e espaços
        endereco = re.sub(r",\s*,", ",", endereco)
        endereco = re.sub(r",\s*", ", ", endereco)
        endereco = re.sub(r"\s+", " ", endereco).strip()

        return endereco

    # ---------------------------------------------------------
    # 🚀 BUILD
    # ---------------------------------------------------------
    def build(self, df: pd.DataFrame):

        logger.info(f"[DATAFRAME_BUILD_START] linhas={len(df)}")

        df.columns = df.columns.str.strip()
        df.rename(columns=self.MAP, inplace=True)


        # ---------------------------------------------
        # NUMÉRICOS (VERSÃO FINAL ROBUSTA)
        # ---------------------------------------------
        def parse_numero(valor):

            if pd.isna(valor):
                return None

            # se já é número, não mexe
            if isinstance(valor, (int, float)):
                return float(valor)

            valor = str(valor).strip()

            # remove separador de milhar (.)
            valor = re.sub(r"\.(?=\d{3}(\D|$))", "", valor)

            # troca decimal
            valor = valor.replace(",", ".")

            return pd.to_numeric(valor, errors="coerce")


        for col in ["cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete", "cte_prazo_min"]:
            if col in df.columns:
                df[col] = df[col].apply(parse_numero)

        # Tempo Atendimento: converter para minutos inteiros
        def parse_tempo_minutos(valor):
            if pd.isna(valor) or valor is None:
                return None
            if isinstance(valor, (int, float)):
                return int(valor)
            valor = str(valor).strip()
            match = re.match(r"^(\d{1,2}):(\d{2})$", valor)
            if match:
                horas = int(match.group(1))
                minutos = int(match.group(2))
                return horas * 60 + minutos
            # Se vier só número, tenta converter
            try:
                return int(valor)
            except Exception:
                return None

        if "cte_tempo_atendimento_min" in df.columns:
            df["cte_tempo_atendimento_min"] = df["cte_tempo_atendimento_min"].apply(parse_tempo_minutos)

        # ---------------------------------------------
        # DATA
        # ---------------------------------------------
        if "envio_data" in df.columns:
            df["envio_data"] = pd.to_datetime(
                df["envio_data"], errors="coerce", dayfirst=True
            ).dt.date

        # ---------------------------------------------
        # CTE NUMERO (FLEXÍVEL E OBRIGATÓRIO)
        # ---------------------------------------------
        if "cte_numero" not in df.columns:
            if "doc_min" in df.columns:
                df["cte_numero"] = df["doc_min"].astype(str).apply(
                    lambda x: re.split(r"/", x)[0] if "/" in x else x
                )
            else:
                logger.error("[DATAFRAME][ERRO] Nenhuma coluna de número de CTe encontrada (cte_numero ou doc_min)")
                raise ValueError("Arquivo de entrada deve conter a coluna 'cte_numero' ou 'doc_min'")

        # ---------------------------------------------
        # ENDEREÇO COMPLETO (CORRIGIDO)
        # ---------------------------------------------
        if "cte_numero_endereco" not in df.columns:
            df["cte_numero_endereco"] = None

        df["endereco_completo"] = df.apply(
            lambda x: self._montar_endereco(
                x.get("cte_rua"),
                x.get("cte_numero_endereco"),
                x.get("cte_bairro"),
                x.get("cte_cidade"),
                x.get("cte_uf"),
            ),
            axis=1
        )

        # ---------------------------------------------
        # COLUNAS DE GEO
        # ---------------------------------------------
        for col in ["destino_latitude", "destino_longitude"]:
            if col not in df.columns:
                df[col] = None

        logger.info(f"[DATAFRAME_BUILD_END] enderecos_completos={df['endereco_completo'].notna().sum()}")

        return df