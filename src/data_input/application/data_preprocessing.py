#hub_router_1.0.1/src/data_input/application/data_preprocessing.py
import locale

# ✅ Ajuste robusto para suportar diferentes nomes de locale
for loc in ("pt_BR.UTF-8", "pt_BR.utf8", "C.UTF-8", "C"):
    try:
        locale.setlocale(locale.LC_ALL, loc)
        break
    except locale.Error:
        continue

import pandas as pd
import numpy as np
import logging
import os
import re
from typing import List
from data_input.config.config import UF_BOUNDS, CLUSTERING_CONFIG
from clusterization.domain.clustering_service import ClusteringService
from clusterization.domain.geolocalizacao_service import GeolocalizacaoService
from clusterization.domain.endereco_helper import EnderecoHelper
from clusterization.domain.entities import Entrega

logging.basicConfig(level=logging.INFO)

class DataPreprocessing:


    def __init__(self, writer, reader, tenant_id: str, path_saida_invalidos: str = None):
        self.writer = writer
        self.reader = reader
        self.tenant_id = tenant_id
        self.path_saida_invalidos = path_saida_invalidos or f"./tenants/{tenant_id}/output/registros_invalidos.csv"
        self.clustering_service = ClusteringService(UF_BOUNDS, CLUSTERING_CONFIG, reader, writer)
        self.geo_service = GeolocalizacaoService(reader, writer)

        self.mapeamento_colunas = {
            "Filial": "transportadora",
            "Remetente": "remetente_nome",
            "CNPJ/CPF": "remetente_cnpj",
            "Origem": "remetente_cidade",
            "UF Origem": "remetente_uf",
            "Destinatário": "destinatario_nome",
            "CNPJ/CPF.1": "destinatario_cnpj",
            "Endereço entrega": "cte_rua",
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

    def execute(self, filepath, sep=","):
        try:
            df = self.carregar_dados(filepath, sep=sep)
            df = self.criar_cte_numero(df)
            df = self.criar_endereco_completo(df)
            df = self.formatar_dados(df)
            df = self.buscar_coordenadas(df)

            # 🔍 Salva registros com coordenadas fora da UF esperada
            if "coordenada_invalida" in df.columns:
                df_coord_invalidas = df[df["coordenada_invalida"] == True].copy()
                if not df_coord_invalidas.empty:
                    self.path_saida_coord_invalidas = f"./tenants/{self.tenant_id}/output/coordenadas_invalidas.csv"
                    os.makedirs(os.path.dirname(self.path_saida_coord_invalidas), exist_ok=True)
                    df_coord_invalidas.to_csv(self.path_saida_coord_invalidas, index=False, sep=";", encoding="utf-8-sig")
                    logging.warning(f"📍 {len(df_coord_invalidas)} registro(s) com coordenadas fora da UF esperada salvos em: {self.path_saida_coord_invalidas}")

            df = self.validar_dados(df)

            if df.empty:
                logging.error("Nenhum dado válido restante após validação.")
                return None

            # ✅ Prepara dados para inserção no banco
            campos_entrega = Entrega.__init__.__code__.co_varnames[1:]
            if "tenant_id" not in campos_entrega:
                campos_entrega = list(campos_entrega) + ["tenant_id"]

            df["tenant_id"] = self.tenant_id
            df = df[[col for col in df.columns if col in campos_entrega]]

            entregas = [Entrega(**row) for row in df.to_dict(orient="records")]

            # 💾 Salva no banco
            self.writer.inserir_dados_entregas(entregas)
            self.writer.atualizar_data_processamento(entregas)

            logging.info(f"✅ Processamento finalizado com {len(entregas)} registros válidos.")
            return df, getattr(self, "qtde_invalidos", 0)

        except Exception as e:
            logging.error(f"❌ Erro ao executar pré-processamento: {e}", exc_info=True)
            return None


    def carregar_dados(self, filepath, sep=","):
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arquivo não encontrado: {filepath}")

        df = pd.read_csv(filepath, sep=sep, encoding="utf-8-sig", dtype=str)


        # 🔧 Padroniza nomes das colunas para evitar problemas com espaços e símbolos
        df.columns = df.columns.str.strip()

        # Aplica mapeamento de colunas
        df.rename(columns=self.mapeamento_colunas, inplace=True)

        # ✅ Conversão segura de campos numéricos
        def converter_coluna_valores(df, col):
            """
            Converte valores numéricos de diferentes formatos:
            - Formato brasileiro: 1.234,56
            - Formato americano: 1,234.56
            - Valor simples: 1234.56 ou 1234,56
            """
            serie = df[col].astype(str).str.strip()

            # Detecção de padrão americano: tem ',' antes de '.' → troca ',' por nada e mantém '.'
            padrao_americano = serie.str.contains(r"\d+,\d{3}\.\d{2}$")

            serie_formatada = serie.copy()
            serie_formatada[padrao_americano] = (
                serie_formatada[padrao_americano]
                .str.replace(",", "", regex=False)
            )

            # Demais formatos → remove '.' de milhar e troca ',' decimal por '.'
            serie_formatada[~padrao_americano] = (
                serie_formatada[~padrao_americano]
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )

            return pd.to_numeric(serie_formatada, errors="coerce")



        colunas_milhar = ["cte_valor_nf", "cte_valor_frete"]
        colunas_decimais = ["cte_peso", "cte_volumes"]

        for col in colunas_milhar + colunas_decimais:
            if col in df.columns:
                df[col] = converter_coluna_valores(df, col)

        # Limpa CNPJs
        for col in ["remetente_cnpj", "destinatario_cnpj"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(r'[^0-9]', '', regex=True)

        # Conversão de data
        if "envio_data" in df.columns:
            df["envio_data"] = pd.to_datetime(df["envio_data"], dayfirst=True, errors="coerce").dt.date

        # Colunas geográficas e extras obrigatórias
        for col in [
            "cluster", "cluster_cidade", "cluster_endereco",
            "centro_lat", "centro_lon", "destino_latitude", "destino_longitude"
        ]:
            if col not in df.columns:
                df[col] = np.nan

        return df


    def criar_cte_numero(self, df):
        if "doc_min" in df.columns:
            df["cte_numero"] = df["doc_min"].astype(str).apply(lambda x: re.split(r"/", x)[0] if "/" in x else x)
        return df

    def criar_endereco_completo(self, df):
        df[["cte_rua", "cte_numero", "cte_cidade", "cte_uf"]] = df[["cte_rua", "cte_numero", "cte_cidade", "cte_uf"]].fillna("").astype(str)
        df["endereco_completo"] = df.apply(lambda x: EnderecoHelper.montar_endereco_completo(x["cte_rua"], x["cte_numero"], x["cte_cidade"], x["cte_uf"]), axis=1)
        return df

    def buscar_coordenadas(self, df):
        ignorados_por_uf = 0
        for idx, row in df.iterrows():
            if pd.isna(row["destino_latitude"]) or pd.isna(row["destino_longitude"]):
                endereco = row["endereco_completo"]
                uf_esperada = row["cte_uf"]
                coordenadas = self.geo_service.buscar_coordenadas(endereco, uf_esperada)
                if coordenadas:
                    df.at[idx, "destino_latitude"], df.at[idx, "destino_longitude"] = coordenadas
                else:
                    ignorados_por_uf += 1
                    df.at[idx, "coordenada_invalida"] = True  # marca para exclusão posterior

        self.registros_ignorados_uf = ignorados_por_uf
        return df


    def formatar_dados(self, df):
        for col in ["cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete"]:
            df[col] = df[col].astype(str).str.replace(",", ".").str.strip()
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["cte_cep"] = df["cte_cep"].astype(str).str.zfill(8)

        if "envio_data" in df.columns:
            df["envio_data"] = pd.to_datetime(df["envio_data"], errors="coerce", dayfirst=True).dt.date

        return df

    def validar_dados(self, df):
        colunas_obrigatorias = [
            "cte_numero", "cte_cidade", "cte_uf", "cte_cep",
            "cte_volumes", "cte_peso", "cte_valor_nf", "cte_valor_frete",
            "destino_latitude", "destino_longitude", "envio_data"
        ]

        # Substitui valores vazios por NaN
        for col in colunas_obrigatorias:
            if col in df.columns:
                df[col] = df[col].replace({"": np.nan, "nan": np.nan, "None": np.nan})

        # Registros com campos obrigatórios ausentes
        registros_invalidos = df[df[colunas_obrigatorias].isna().any(axis=1)].copy()

        # Adiciona motivo da invalidação
        def identificar_colunas_invalidas(row):
            faltantes = [col for col in colunas_obrigatorias if pd.isna(row[col])]
            return ", ".join(faltantes)

        if not registros_invalidos.empty:
            registros_invalidos["motivo_invalidade"] = registros_invalidos.apply(identificar_colunas_invalidas, axis=1)
            registros_invalidos.to_csv(self.path_saida_invalidos, index=False, encoding="utf-8-sig", sep=";")
            logging.warning(f"⚠️ {len(registros_invalidos)} registro(s) inválido(s) salvos em: {self.path_saida_invalidos}")

        # Remove inválidos por campos obrigatórios
        df_validos = df.dropna(subset=colunas_obrigatorias)

        # Remove registros com coordenadas inválidas, se houver a coluna
        if "coordenada_invalida" in df_validos.columns:
            removidos_coord = df_validos[df_validos["coordenada_invalida"] == True]
            if not removidos_coord.empty:
                logging.info(f"🗑️ {len(removidos_coord)} registro(s) removido(s) por coordenadas fora da UF esperada.")
            df_validos = df_validos[df_validos["coordenada_invalida"] != True]

        # Garante retorno de DataFrame (mesmo que vazio)
        self.qtde_invalidos = len(registros_invalidos)
        if df_validos is None:
            return pd.DataFrame()

        return df_validos





