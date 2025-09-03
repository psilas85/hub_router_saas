#costs_last_mile/domain/cost_service_last_mile.py

import logging

from costs_last_mile.application.cost_use_case_last_mile import CostUseCase
from costs_last_mile.infrastructure.cost_repository_last_mile import CostRepository

class CostService:
    def __init__(self, tenant_id: str):
        repository = CostRepository()
        self.use_case = CostUseCase(repository, tenant_id)

    def processar_custos(self, data_inicial, data_final, modo_forcar: bool = False):
        logging.info("🚀 Iniciando processamento dos custos...")
        self.use_case.calcular_custos(data_inicial, data_final, modo_forcar=modo_forcar)
        logging.info("✅ Processamento concluído.")

