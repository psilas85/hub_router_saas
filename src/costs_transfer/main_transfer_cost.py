#costs_transfer/main_transfer_cost.py

import logging
import argparse
from costs_transfer.domain.transfer_service import TransferCostService

def main():
    # === Configuração de logging ===
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # === Parser de argumentos CLI ===
    parser = argparse.ArgumentParser(description="📊 Calcular custos de transferência por tenant e período.")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data_inicial", required=True, help="Data inicial no formato YYYY-MM-DD")
    parser.add_argument("--data_final", required=False)
    parser.add_argument("--modo_forcar", action="store_true", help="Forçar sobrescrita de dados existentes")

    args = parser.parse_args()

    try:
        service = TransferCostService(tenant_id=args.tenant)
        service.processar_custos(
            data_inicial=args.data_inicial,
            data_final = args.data_final or args.data_inicial,
            modo_forcar=args.modo_forcar
        )
        logging.info("✅ Execução finalizada com sucesso.")
    except Exception as e:
        logging.error(f"❌ Erro inesperado durante o processamento: {e}", exc_info=True)

if __name__ == "__main__":
    main()
