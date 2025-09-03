import logging
from costs_transfer.infrastructure.transfer_cost_repository import TransferCostRepository

class TransferCostUseCase:
    def __init__(self, repository, tenant_id: str):
        self.repository = repository
        self.tenant_id = tenant_id

    def calcular_custos(self, data_inicial, data_final, modo_forcar: bool = False):
        logging.info(f"📊 Iniciando cálculo de custos de transferência de {data_inicial} a {data_final} para tenant '{self.tenant_id}'...")

        df_transf = self.repository.buscar_transferencias_resumo(data_inicial, data_final, self.tenant_id)
        df_custos = self.repository.buscar_custos_transferencia(self.tenant_id)

        if df_transf.empty or df_custos.empty:
            logging.warning("⚠️ Nenhum dado encontrado para cálculo de custos de transferência.")
            return

        custos_calculados = []

        for _, row in df_transf.iterrows():
            veiculo = row["tipo_veiculo"]
            custo_por_km = df_custos.loc[df_custos["tipo_veiculo"] == veiculo, "custo_por_km"]

            if custo_por_km.empty:
                logging.warning(f"⚠️ Nenhuma tarifa encontrada para veículo: {veiculo}. Pulando...")
                continue

            custo_por_km = float(custo_por_km.iloc[0])
            custo_total = row["distancia_total"] * custo_por_km
            percentual_custo = (custo_total / float(row["cte_valor_frete"])) if float(row["cte_valor_frete"]) > 0 else 0.0

            custos_calculados.append({
                "tenant_id": self.tenant_id,
                "envio_data": row["envio_data"],
                "rota_transf": row["rota_transf"],
                "cte_peso": row["cte_peso"],
                "cte_valor_frete": row["cte_valor_frete"],
                "clusters_qde": row["clusters_qde"],
                "hub_central_nome": row["hub_central_nome"],
                "distancia_total": row["distancia_total"],
                "tipo_veiculo": veiculo,
                "custo_transferencia_total": custo_total,
                "percentual_custo": percentual_custo
            })

        if custos_calculados:
            logging.info(f"💾 {'Sobrescrevendo dados existentes' if modo_forcar else 'Mantendo dados existentes se já existirem'}")
            self.repository.persistir_custos_transferencia(custos_calculados, modo_forcar)
            logging.info(f"✅ {len(custos_calculados)} registros de custos de transferência salvos com sucesso!")
        else:
            logging.warning("⚠️ Nenhum custo calculado para salvar.")
