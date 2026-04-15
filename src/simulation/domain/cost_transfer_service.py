# hub_router_1.0.1/src/simulation/domain/cost_transfer_service.py

from simulation.domain.entities import TransferenciaResumo
from simulation.infrastructure.simulation_database_reader import (
    obter_tarifas_veiculo_transferencia,
)


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
            distancia_km = float(resumo.distancia_total_km or 0.0)
            qde_clusters_rota = int(resumo.qde_clusters_rota or 0)

            tarifa_km, tarifa_fixa = obter_tarifas_veiculo_transferencia(
                tipo_veiculo,
                self.db_conn,
                self.tenant_id,
            )

            custo_distancia = distancia_km * tarifa_km
            custo_paradas = qde_clusters_rota * tarifa_fixa
            custo_rota = custo_distancia + custo_paradas

            custo_total += custo_rota

            self.logger.info(
                f"🚛 Rota {resumo.rota_id} - Veículo: {tipo_veiculo}, "
                f"Distância: {distancia_km:.2f} km, "
                f"Clusters: {qde_clusters_rota}, "
                f"Custo distância: R${custo_distancia:,.2f}, "
                f"Custo fixo: R${custo_paradas:,.2f}, "
                f"Custo total rota: R${custo_rota:,.2f}"
            )

        self.logger.info(f"💰 Custo total de transferência: R${custo_total:,.2f}")
        return round(custo_total, 2)