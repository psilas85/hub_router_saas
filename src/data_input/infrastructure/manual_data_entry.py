import logging
import sys

import pandas as pd

from data_input.application.dataframe_builder import DataFrameBuilder
from data_input.domain.entities import Entrega
from data_input.infrastructure.database_connection import conectar_banco
from data_input.infrastructure.database_writer import DatabaseWriter

logger = logging.getLogger(__name__)

LAT_MIN, LAT_MAX = -34.0, 5.5
LON_MIN, LON_MAX = -74.0, -34.0

REQUIRED = [
    "cte_numero",
    "cte_cidade",
    "cte_uf",
    "cte_peso",
    "envio_data",
    "transportadora",
    "cte_rua",
    "cte_numero_endereco",
    "cte_bairro",
    "destino_latitude",
    "destino_longitude",
]


def formatar_linhas(df: pd.DataFrame) -> str:
    registros = df.to_dict(orient="records")
    return "\n".join(str(registro) for registro in registros)


def validar_limites_lat_lon(df: pd.DataFrame) -> pd.Series:
    lat_ok = df["destino_latitude"].between(LAT_MIN, LAT_MAX)
    lon_ok = df["destino_longitude"].between(LON_MIN, LON_MAX)
    return lat_ok & lon_ok


def validar_obrigatorios(df: pd.DataFrame) -> None:
    missing_columns = [col for col in REQUIRED if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing_columns}")

    mask = df[REQUIRED].isnull().to_numpy().any(axis=1)
    if mask.any():
        linhas_com_falta = pd.DataFrame(df.loc[mask, REQUIRED]).copy()
        linhas_invalidas = formatar_linhas(linhas_com_falta)
        raise ValueError(
            "Existem linhas com campos obrigatórios vazios:\n"
            f"{linhas_invalidas}"
        )


def validar_regras_iniciais(df: pd.DataFrame) -> None:
    mask_peso = df["cte_peso"].fillna(0) <= 0
    if mask_peso.any():
        linhas_peso_invalido = pd.DataFrame(
            df.loc[mask_peso, ["cte_numero", "cte_peso"]]
        ).copy()
        raise ValueError(
            "Existem linhas com peso inválido:\n"
            f"{formatar_linhas(linhas_peso_invalido)}"
        )

    duplicados = df.duplicated(subset=["cte_numero"], keep=False)
    if duplicados.any():
        linhas_duplicadas = pd.DataFrame(
            df.loc[duplicados, ["cte_numero", "transportadora"]]
        ).copy()
        raise ValueError(
            "Existem CTe's duplicados no arquivo:\n"
            f"{formatar_linhas(linhas_duplicadas)}"
        )


def montar_entregas(df: pd.DataFrame, tenant_id: str):
    entregas = []
    for row in df.to_dict(orient="records"):
        entregas.append(
            Entrega(
                cte_numero=row.get("cte_numero"),
                remetente_cnpj=row.get("remetente_cnpj"),
                cte_rua=row.get("cte_rua"),
                cte_bairro=row.get("cte_bairro"),
                cte_complemento=row.get("cte_complemento"),
                cte_numero_endereco=row.get("cte_numero_endereco"),
                cte_cidade=row.get("cte_cidade"),
                cte_uf=row.get("cte_uf"),
                cte_cep=row.get("cte_cep"),
                cte_nf=row.get("cte_nf"),
                cte_volumes=row.get("cte_volumes"),
                cte_peso=row.get("cte_peso"),
                cte_tempo_atendimento_min=row.get("cte_tempo_atendimento_min"),
                cte_prazo_min=row.get("cte_prazo_min"),
                cte_valor_nf=row.get("cte_valor_nf"),
                cte_valor_frete=row.get("cte_valor_frete"),
                envio_data=row.get("envio_data"),
                endereco_completo=row.get("endereco_completo"),
                transportadora=row.get("transportadora"),
                remetente_nome=row.get("remetente_nome"),
                destinatario_nome=row.get("destinatario_nome"),
                destinatario_cnpj=row.get("destinatario_cnpj"),
                destino_latitude=row.get("destino_latitude"),
                destino_longitude=row.get("destino_longitude"),
                remetente_cidade=row.get("remetente_cidade"),
                remetente_uf=row.get("remetente_uf"),
                doc_min=row.get("doc_min"),
                tenant_id=tenant_id,
                geocode_source=row.get("geocode_source"),
            )
        )
    return entregas


def salvar_localizacoes(writer: DatabaseWriter, entregas) -> None:
    for entrega in entregas:
        writer.inserir_localizacao(
            entrega.endereco_completo,
            entrega.destino_latitude,
            entrega.destino_longitude,
            origem="manual",
        )


def process_manual_data_input(input_path: str, tenant_id: str) -> dict:
    raw_df = pd.read_excel(input_path)
    builder = DataFrameBuilder()
    df = builder.build(raw_df)
    df["geocode_source"] = "manual"

    validar_obrigatorios(df)
    validar_regras_iniciais(df)

    mask_latlon = validar_limites_lat_lon(df)
    if not mask_latlon.all():
        linhas_coordenadas_invalidas = pd.DataFrame(
            df.loc[
                ~mask_latlon,
                ["cte_numero", "destino_latitude", "destino_longitude"],
            ]
        ).copy()
        coordenadas_invalidas = formatar_linhas(linhas_coordenadas_invalidas)
        raise ValueError(
            "Existem coordenadas fora dos limites globais:\n"
            f"{coordenadas_invalidas}"
        )

    entregas = montar_entregas(df, tenant_id)

    conexao = conectar_banco()
    if conexao is None:
        raise RuntimeError("Não foi possível conectar ao banco de dados")

    writer = DatabaseWriter(conexao)

    try:
        salvar_localizacoes(writer, entregas)
        writer.inserir_dados_entregas(entregas)
        conexao.commit()
        logger.info("Entrada manual concluída com sucesso")
        return {
            "status": "done",
            "tenant_id": tenant_id,
            "total_processados": len(df),
            "validos": len(df),
            "invalidos": 0,
            "mensagem": "Entrada manual concluída com sucesso",
        }
    except Exception:
        conexao.rollback()
        raise
    finally:
        conexao.close()


def main(input_path: str, tenant_id: str) -> None:
    process_manual_data_input(input_path, tenant_id)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: manual_data_entry.py <input.xlsx> <tenant_id>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
