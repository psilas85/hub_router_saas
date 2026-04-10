# domain/cost_last_mile_service.py
import pandas as pd


class CostLastMileService:
    def __init__(self, db_conn, logger, tenant_id: str):
        self.db_conn = db_conn
        self.logger = logger
        self.tenant_id = tenant_id

    def calcular_custo(self, df_rotas_last_mile: pd.DataFrame) -> float:
        self.logger.info("💰 Calculando custo de last-mile...")

        df_rotas = df_rotas_last_mile.copy()

        # Corrige possíveis valores nulos e tipagem
        df_rotas['distancia_total_km'] = pd.to_numeric(
            df_rotas['distancia_total_km'], errors='coerce'
        )
        df_rotas['qtde_entregas'] = pd.to_numeric(
            df_rotas['qtde_entregas'], errors='coerce'
        ).fillna(0).astype(int)
        df_rotas['ordem_entrega'] = pd.to_numeric(
            df_rotas.get('ordem_entrega'), errors='coerce'
        ).fillna(0).astype(int)

        # Usa a linha-resumo da rota, onde distancia_total_km foi populada explicitamente.
        df_rotas_agrupado = (
            df_rotas[df_rotas['distancia_total_km'].notnull()]
            .sort_values(["rota_id", "ordem_entrega"])
            .drop_duplicates(subset=["rota_id"], keep="first")
        )

        self.logger.info(
            f"📊 Base de custo last-mile: rotas_unicas={df_rotas['rota_id'].nunique()} | rotas_com_resumo={len(df_rotas_agrupado)}"
        )

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
        query = """
            SELECT tarifa_km
            FROM veiculos_last_mile
            WHERE tenant_id = %s AND tipo_veiculo = %s
        """
        cursor.execute(query, (self.tenant_id, tipo_veiculo))
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] is not None:
            return float(result[0])
        else:
            self.logger.warning(f"❗ Tarifa por km não encontrada para '{tipo_veiculo}', retornando 0.0")
            return 0.0

    def _obter_custo_por_entrega(self, tipo_veiculo: str) -> float:
        cursor = self.db_conn.cursor()
        query = """
            SELECT tarifa_entrega
            FROM veiculos_last_mile
            WHERE tenant_id = %s AND tipo_veiculo = %s
        """
        cursor.execute(query, (self.tenant_id, tipo_veiculo))
        result = cursor.fetchone()
        cursor.close()

        if result and result[0] is not None:
            return float(result[0])
        else:
            self.logger.warning(f"❗ Tarifa por entrega não encontrada para '{tipo_veiculo}', retornando 0.0")
            return 0.0

