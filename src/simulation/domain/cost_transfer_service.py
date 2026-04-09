#domain/cost_transfer_service.py

from simulation.domain.entities import TransferenciaResumo
from simulation.infrastructure.simulation_database_reader import obter_tarifa_km_veiculo_transferencia

class CostTransferService:
    def __init__(self, db_conn, logger, tenant_id: str):
        self.db_conn = db_conn
        self.logger = logger
        self.tenant_id = tenant_id

    def calcular_custo(self, lista_resumo: list[TransferenciaResumo]) -> float:
        self.logger.info("💰 Calculando custo de transferência...")

        custo_total = 0.0

        for resumo in lista_resumo:
            tipo_veiculo = resumo.tipo_veiculo
            distancia_km = resumo.distancia_total_km

            custo_por_km = obter_tarifa_km_veiculo_transferencia(
                tipo_veiculo,
                self.db_conn,
                self.tenant_id,
            )
            custo = distancia_km * custo_por_km
            custo_total += custo

            self.logger.info(
                f"🚛 Rota {resumo.rota_id} - Veículo: {tipo_veiculo}, "
                f"Distância: {distancia_km:.2f} km, Custo: R${custo:,.2f}"
            )


        self.logger.info(f"💰 Custo total de transferência: R${custo_total:,.2f}")
        return round(custo_total, 2)
