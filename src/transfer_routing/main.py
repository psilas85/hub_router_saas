# transfer_routing/main.py

import argparse
from datetime import datetime

from transfer_routing.application.transfer_routing_use_case import TransferRoutingUseCase


def main():
    parser = argparse.ArgumentParser(description="Roteirização de Transferências")

    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data", required=True, help="Data do envio (AAAA-MM-DD)")
    parser.add_argument("--modo-forcar", action="store_true", help="Forçar sobrescrita de dados existentes")

    parser.add_argument("--tempo-maximo", type=float, default=1200,
                        help="Tempo máximo da rota (minutos)")
    parser.add_argument("--tempo-parada-leve", type=float, default=10.0,
                        help="Tempo de parada leve (peso <= peso-leve-max)")
    parser.add_argument("--peso-leve-max", type=float, default=50.0,
                        help="Limite de peso (kg) para considerar parada leve")
    parser.add_argument("--tempo-parada-pesada", type=float, default=20.0,
                        help="Tempo de parada pesada (peso > peso-leve-max)")
    parser.add_argument("--tempo-por-volume", type=float, default=0.05,
                        help="Tempo de descarregamento por volume (min por volume)")

    args = parser.parse_args()

    envio_data = datetime.strptime(args.data, "%Y-%m-%d").date()

    use_case = TransferRoutingUseCase(
        tenant_id=args.tenant,
        modo_forcar=args.modo_forcar,
        tempo_maximo=args.tempo_maximo,
        tempo_parada_leve=args.tempo_parada_leve,
        peso_leve_max=args.peso_leve_max,
        tempo_parada_pesada=args.tempo_parada_pesada,
        tempo_por_volume=args.tempo_por_volume
    )

    use_case.run(data_inicial=envio_data, data_final=envio_data)


if __name__ == "__main__":
    main()
