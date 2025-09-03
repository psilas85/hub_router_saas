# costs_last_mile/main_costs.py
import logging
import argparse
from costs_last_mile.domain.cost_service_last_mile import CostService
from datetime import date

def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="📊 Cálculo de Custos Last-Mile")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data_inicial", required=True, help="Data inicial no formato AAAA-MM-DD")
    parser.add_argument("--data_final", required=False, help="Data final no formato AAAA-MM-DD (opcional)")
    parser.add_argument("--modo_forcar", action="store_true", help="Força sobrescrita dos dados existentes")

    args = parser.parse_args()

    data_final = args.data_final or args.data_inicial  # 🔑 default

    service = CostService(tenant_id=args.tenant)
    service.processar_custos(data_inicial=args.data_inicial, data_final=data_final, modo_forcar=args.modo_forcar)

if __name__ == "__main__":
    main()
