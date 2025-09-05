#hub_router_1.0.1/src/costs_transfer/visualization/main_visualization.py

import argparse
import os
import logging
import pandas as pd

from costs_transfer.visualization.gerar_relatorio_transferencias import gerar_relatorio_transferencias
from costs_transfer.infrastructure.transfer_cost_db import conectar_banco

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def carregar_detalhes_transferencias(tenant_id: str, envio_data: str) -> pd.DataFrame:
    try:
        conn = conectar_banco()

        query = """
            SELECT
                rota_transf,
                hub_central_nome,
                tipo_veiculo,
                cte_peso,
                cte_valor_frete,
                distancia_total,
                custo_transferencia_total,
                percentual_custo
            FROM transfer_costs_details
            WHERE tenant_id = %s AND envio_data = %s AND tipo_veiculo IS NOT NULL
            ORDER BY rota_transf
        """
        df = pd.read_sql(query, conn, params=(tenant_id, envio_data))
        conn.close()
        return df
    except Exception as e:
        logger.error(f"❌ Erro ao carregar dados detalhados: {e}")
        return pd.DataFrame()


def salvar_csv(df: pd.DataFrame, tenant_id: str, envio_data: str) -> str:
    output_csv_path = f"exports/costs_transfer/csv/{tenant_id}"
    os.makedirs(output_csv_path, exist_ok=True)
    caminho_csv = f"{output_csv_path}/transfer_costs_detalhado_{envio_data}.csv"
    df.to_csv(caminho_csv, index=False, sep=";", decimal=",")
    logger.info(f"📁 CSV salvo em: {caminho_csv}")
    return caminho_csv


def salvar_json(df: pd.DataFrame, tenant_id: str, envio_data: str) -> str:
    output_json_path = f"exports/costs_transfer/json/{tenant_id}"
    os.makedirs(output_json_path, exist_ok=True)
    caminho_json = f"{output_json_path}/transfer_costs_resumo_{envio_data}.json"
    df.to_json(caminho_json, orient="records", force_ascii=False, indent=2)
    logger.info(f"📁 JSON salvo em: {caminho_json}")
    return caminho_json


def main():
    parser = argparse.ArgumentParser(description="Gerar relatório, CSV e JSON de custos de transferência por rota")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data", required=True, help="Data do envio no formato AAAA-MM-DD")
    args = parser.parse_args()

    tenant_id = args.tenant
    envio_data = args.data
    modo_forcar = args.modo_forcar

    logger.info(f"📦 Processando relatórios para tenant '{tenant_id}' e data '{envio_data}'")

    pdf_dir = f"exports/costs_transfer/pdf/{tenant_id}"
    os.makedirs(pdf_dir, exist_ok=True)
    caminho_pdf = f"{pdf_dir}/relatorio_transfer_costs_detalhado_{envio_data}.pdf"

    if os.path.exists(caminho_pdf) and not modo_forcar:
        confirmacao = input("⚠️ Já existe um relatório para essa data. Deseja sobrescrever? (s/n): ").strip().lower()
        if confirmacao != "s":
            logger.info("❌ Operação cancelada.")
            return

    df = carregar_detalhes_transferencias(tenant_id, envio_data)
    if df.empty:
        logger.warning("⚠️ Nenhum dado encontrado. Encerrando.")
        return

    # Geração de relatórios
    salvar_csv(df, tenant_id, envio_data)
    salvar_json(df, tenant_id, envio_data)
    gerar_relatorio_transferencias(envio_data, tenant_id, df)

    logger.info(f"📄 {len(df)} registros incluídos no relatório.")

    total_peso = df["cte_peso"].sum()
    total_frete = df["cte_valor_frete"].sum()
    total_custo = df["custo_transferencia_total"].sum()

    logger.info(f"📦 Total transferido: {total_peso:,.2f} kg | R$ {total_frete:,.2f} em frete | R$ {total_custo:,.2f} em custo")
    logger.info("✅ Relatórios gerados com sucesso.")


if __name__ == "__main__":
    main()
