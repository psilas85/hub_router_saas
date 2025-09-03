#last_mile_routing/visualization/main_visualization.py

import os
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv

from last_mile_routing.visualization.route_plotter import plotar_rotas
from last_mile_routing.visualization.generate_pdf_report import generate_pdf_report


def parse_data(data_str, padrao):
    if not data_str:
        return padrao
    try:
        return datetime.strptime(data_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"❌ Data inválida: {data_str}. Use AAAA-MM-DD."
        )


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="🗺️ Visualização de rotas - Last Mile")

    parser.add_argument("--tenant", type=str, required=True, help="Tenant ID")
    parser.add_argument("--data_inicial", type=str, required=True, help="Data inicial (AAAA-MM-DD)")
    parser.add_argument("--data_final", type=str, required=False, help="Data final (AAAA-MM-DD)")

    args = parser.parse_args()

    data_inicial = parse_data(args.data_inicial, None)
    data_final = parse_data(args.data_final, data_inicial)

    datas = [
        data_inicial + timedelta(days=i)
        for i in range((data_final - data_inicial).days + 1)
    ]

    for data in datas:
        print(f"🗺️ Gerando mapa de {data} para tenant '{args.tenant}'...")
        plotar_rotas(args.tenant, data)

        print(f"📄 Gerando relatório PDF de {data} para tenant '{args.tenant}'...")
        generate_pdf_report(args.tenant, data)

        print(f"✅ Processamento de {data} finalizado.")


if __name__ == "__main__":
    main()
