# domain/cost_last_mile_service.py
import pandas as pd
from simulation.infrastructure.simulation_database_reader import carregar_tarifas_last_mile, definir_tipo_veiculo_last_mile

class CostLastMileService:
    def __init__(self, db_conn, logger):
        self.db_conn = db_conn
        self.logger = logger

    def calcular_custo(self, df_rotas_last_mile: pd.DataFrame) -> float:
        self.logger.info("💰 Calculando custo de last-mile...")

        # Corrige possíveis valores nulos e tipagem
        df_rotas_last_mile['distancia_total_km'] = pd.to_numeric(df_rotas_last_mile['distancia_total_km'], errors='coerce').fillna(0.0)
        df_rotas_last_mile['qtde_entregas'] = pd.to_numeric(df_rotas_last_mile['qtde_entregas'], errors='coerce').fillna(0).astype(int)

        # Agrupa por rota única (importante para evitar duplicações por CTE)
        df_rotas_agrupado = df_rotas_last_mile.drop_duplicates(subset=["rota_id"])

        custo_total = 0.0

        for _, row in df_rotas_agrupado.iterrows():
            distancia_km = float(row['distancia_total_km'])
            qtde_entregas = int(row['qtde_entregas'])
            tipo_veiculo = row.get('tipo_veiculo', 'HR')

            custo_por_km = self._obter_custo_por_km(tipo_veiculo)
            custo_por_entrega = self._obter_custo_por_entrega(tipo_veiculo)

            custo = (distancia_km * custo_por_km) + (qtde_entregas * custo_por_entrega)
            custo_total += custo

            self.logger.info(
                f"🚐 Rota - Veículo: {tipo_veiculo}, "
                f"Distância: {distancia_km:.2f} km, Entregas: {qtde_entregas}, "
                f"Custo: R${custo:,.2f}"
            )

        self.logger.info(f"💰 Custo total de last-mile: R${custo_total:,.2f}")
        return round(custo_total, 2)


    def _obter_custo_por_km(self, tipo_veiculo: str) -> float:
        cursor = self.db_conn.cursor()
        query = "SELECT tarifa_km FROM veiculos_last_mile WHERE tipo_veiculo = %s"
        cursor.execute(query, (tipo_veiculo,))
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] is not None:
            return float(result[0])
        else:
            self.logger.warning(f"❗ Tarifa por km não encontrada para '{tipo_veiculo}', retornando 0.0")
            return 0.0

    def _obter_custo_por_entrega(self, tipo_veiculo: str) -> float:
        cursor = self.db_conn.cursor()
        query = "SELECT tarifa_entrega FROM veiculos_last_mile WHERE tipo_veiculo = %s"
        cursor.execute(query, (tipo_veiculo,))
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] is not None:
            return float(result[0])
        else:
            self.logger.warning(f"❗ Tarifa por entrega não encontrada para '{tipo_veiculo}', retornando 0.0")
            return 0.0

