#hub_router_1.0.1/src/last_mile_routing/main_routing.py

import argparse
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from last_mile_routing.application.routing_use_case import RoutingUseCase
from last_mile_routing.logs.logging_factory import LoggerFactory


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
    logger = LoggerFactory.get_logger("routing")

    # 🔥 Carrega variáveis do .env
    load_dotenv()

    parser = argparse.ArgumentParser(description="🚚 Roteirização de entregas")

    parser.add_argument("--tenant", type=str, required=True, help="Tenant ID")
    parser.add_argument("--data_inicial", type=str, required=True, help="Data inicial (AAAA-MM-DD)")
    parser.add_argument("--data_final", type=str, required=False, help="Data final (AAAA-MM-DD)")


    parser.add_argument("--entregas_por_subcluster", type=int, default=25, help="Número de entregas por subcluster na clusterização inicial")
    parser.add_argument("--tempo_maximo_rota", type=float, default=1200)
    parser.add_argument("--tempo_parada_leve", type=float, default=10.0)
    parser.add_argument("--tempo_parada_pesada", type=float, default=20.0)
    parser.add_argument("--tempo_descarga_por_volume", type=float, default=0.4)
    parser.add_argument("--peso_leve_max", type=float, default=50.0)
    parser.add_argument("--restricao_veiculo_leve_municipio", action="store_true", help="Se definido, restringe veículos leves (Moto, Fiorino) em rotas que atendem mais de uma cidade")

    parser.add_argument("--modo_forcar", action="store_true", help="Se definido, sobrescreve qualquer roteirização existente para a data")

    args = parser.parse_args()

    data_inicial = parse_data(args.data_inicial, None)
    data_final = parse_data(args.data_final, data_inicial)  # se não informado, usa inicial


    parametros = {
        "entregas_por_subcluster": args.entregas_por_subcluster,
        "tempo_maximo_rota": args.tempo_maximo_rota,
        "tempo_parada_leve": args.tempo_parada_leve,
        "tempo_parada_pesada": args.tempo_parada_pesada,
        "tempo_descarga_por_volume": args.tempo_descarga_por_volume,
        "peso_leve_max": args.peso_leve_max,
        "restricao_veiculo_leve_municipio": args.restricao_veiculo_leve_municipio,
        "modo_forcar": args.modo_forcar,
    }

    # 🔑 API Key do Google
    api_key = os.getenv("GMAPS_API_KEY")
    if not api_key:
        raise ValueError("❌ API Key do Google Maps não encontrada. Verifique o arquivo .env ou variável de ambiente.")

    datas = [
        data_inicial + timedelta(days=i)
        for i in range((data_final - data_inicial).days + 1)
    ]

    for data in datas:
        logger.info(f"📅 Processando {data}...")
        roteirizador = RoutingUseCase(args.tenant, parametros, api_key)
        roteirizador.executar(data)


if __name__ == "__main__":
    main()
