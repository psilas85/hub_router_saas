import logging

from costs_transfer.application.transfer_cost_use_case import TransferCostUseCase
from costs_transfer.infrastructure.transfer_cost_repository import TransferCostRepository

class TransferCostService:
    def __init__(self, tenant_id: str):
        repository = TransferCostRepository()
        self.use_case = TransferCostUseCase(repository, tenant_id)

    def processar_custos(self, data_inicial: str, data_final: str, modo_forcar: bool = False):
        logging.info("🚀 Iniciando processamento dos custos de transferência...")
        self.use_case.calcular_custos(data_inicial, data_final, modo_forcar)
        logging.info("✅ Processamento concluído.")
