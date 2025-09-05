# transfer_routing/visualization/main_visualization.py

import os
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv

from transfer_routing.visualization.route_plotter import gerar_mapa
from transfer_routing.visualization.mapa_estatico import gerar_mapa_estatico_transferencias
from transfer_routing.visualization.gerador_relatorio_transferencias import gerar_relatorio_transferencias
from transfer_routing.infrastructure.database_connection import conectar_banco_routing, fechar_conexao
from transfer_routing.logs.logging_factory import LoggerFactory


def parse_data(data_str: str):
    try:
        return datetime.strptime(data_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"❌ Data inválida: {data_str}. Use AAAA-MM-DD.")


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="🗺️ Visualização de rotas - Transfer Routing")
    parser.add_argument("--data", type=str, required=True, help="Data inicial (AAAA-MM-DD)")
    parser.add_argument("--data_final", type=str, required=False, help="Data final (AAAA-MM-DD, opcional)")
    parser.add_argument("--tenant", type=str, required=True, help="Tenant ID")

    args = parser.parse_args()

    tenant_id = args.tenant
    data_inicial = parse_data(args.data)
    data_final = parse_data(args.data_final) if args.data_final else data_inicial

    logger = LoggerFactory.get_logger("visualization")

    datas = [
        data_inicial + timedelta(days=i)
        for i in range((data_final - data_inicial).days + 1)
    ]

    for data in datas:
        envio_data = data.strftime("%Y-%m-%d")
        logger.info(f"🗺️ Gerando mapa de {envio_data} para tenant '{tenant_id}'...")

        # HTML
        caminho_mapa_html = gerar_mapa(tenant_id, data, data, output_path=caminho_output(tenant_id, "maps"))

        # PNG
        logger.info("📸 Gerando mapa estático (PNG)...")
        caminho_mapa_png = gerar_mapa_estatico_transferencias(tenant_id, data, data, output_path=caminho_output(tenant_id, "maps"))

        # PDF
        conn = conectar_banco_routing()
        logger.info("📄 Gerando relatório PDF...")
        gerar_relatorio_transferencias(
            tenant_id=tenant_id,
            envio_data=envio_data,
            data_final=envio_data,
            output_path=caminho_output(tenant_id, "relatorios"),
            caminho_mapa_html=caminho_mapa_html,
            caminho_mapa_png=caminho_mapa_png,
            conn=conn,
            logger=logger
        )
        fechar_conexao(conn)

        logger.info(f"✅ Processamento de {envio_data} finalizado.")


if __name__ == "__main__":
    main()
