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
        "Endereço entrega": "cte_rua",
        "Rua": "cte_rua",
        "Logradouro": "cte_rua",
        "Número": "cte_numero_endereco",
        "Numero": "cte_numero_endereco",
        "Número Endereço": "cte_numero_endereco",
        "Numero Endereco": "cte_numero_endereco",
        "Número do Endereço": "cte_numero_endereco",
        "Numero do Endereco": "cte_numero_endereco",
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

        endereco = f"{rua}, {bairro}, {cidade} - {uf}"
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


        for col in ["cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete"]:
            if col in df.columns:
                df[col] = df[col].apply(parse_numero)

        # ---------------------------------------------
        # DATA
        # ---------------------------------------------
        if "envio_data" in df.columns:
            df["envio_data"] = pd.to_datetime(
                df["envio_data"], errors="coerce", dayfirst=True
            ).dt.date

        # ---------------------------------------------
        # CTE NUMERO
        # ---------------------------------------------
        if "doc_min" in df.columns:
            df["cte_numero"] = df["doc_min"].astype(str).apply(
                lambda x: re.split(r"/", x)[0] if "/" in x else x
            )

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