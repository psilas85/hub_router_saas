#costs_last_mile/application/cost_use_case_last_mile.py

import logging
from costs_last_mile.infrastructure.cost_repository_last_mile import CostRepository

class CostUseCase:
    def __init__(self, repository: CostRepository, tenant_id: str):
        self.repository = repository
        self.tenant_id = tenant_id

    def calcular_custos(self, data_inicial, data_final, modo_forcar: bool = False):
        logging.info(f"📊 Iniciando cálculo de custos de {data_inicial} a {data_final} para tenant '{self.tenant_id}'...")

        rotas_df = self.repository.buscar_resumo_rotas(data_inicial, data_final, self.tenant_id)
        custos_df = self.repository.buscar_custos_veiculo(self.tenant_id)

        if rotas_df.empty or custos_df.empty:
            logging.warning("⚠️ Nenhum dado encontrado para cálculo de custos.")
            return
        
        if modo_forcar:
            datas = rotas_df["data_envio"].unique()
            for data in datas:
                self.repository.deletar_dados_existentes(data, self.tenant_id)

        # 🔹 Normalizar nomes dos veículos nas tarifas
        custos_df["veiculo_normalizado"] = custos_df["veiculo"].str.strip().str.lower()

        custos_calculados = []

        for _, rota in rotas_df.iterrows():
            veiculo_rota = str(rota["veiculo"]).strip().lower()

            # 🔹 Buscar tarifas com base no nome normalizado
            custo_por_km = custos_df.loc[custos_df["veiculo_normalizado"] == veiculo_rota, "custo_por_km"]
            custo_por_entrega = custos_df.loc[custos_df["veiculo_normalizado"] == veiculo_rota, "custo_por_entrega"]

            if custo_por_km.empty or custo_por_entrega.empty:
                logging.warning(f"⚠️ Nenhuma tarifa encontrada para veículo: {rota['veiculo']}. Pulando...")
                continue

            custo_por_km = float(custo_por_km.iloc[0])
            custo_por_entrega = float(custo_por_entrega.iloc[0])

            custo_distancia = rota["distancia_total_km"] * custo_por_km
            custo_entrega = rota["quantidade_entregas"] * custo_por_entrega
            custo_total = custo_distancia + custo_entrega

            frete_total = rota.get("cte_frete_total")

            try:
                frete_total = float(frete_total)
            except (TypeError, ValueError):
                frete_total = 0.0

            percentual_custo = (custo_total / frete_total * 100) if frete_total > 0 else 0.0


            custos_calculados.append({
                "tenant_id": self.tenant_id,
                "data_envio": rota["data_envio"],
                "cluster": rota["cluster"],
                "sub_cluster": rota["sub_cluster"],
                "quantidade_entregas": rota["quantidade_entregas"],
                "peso_total_kg": rota["peso_total_kg"],
                "distancia_total_km": rota["distancia_total_km"],
                "cte_frete_total": frete_total,  # <-- Corrigido aqui
                "veiculo": rota["veiculo"],
                "custo_entrega_total": custo_total,
                "percentual_custo": percentual_custo
            })


        if custos_calculados:
            self.repository.persistir_custos_rota_detalhes(custos_calculados, modo_forcar=modo_forcar)
            logging.info(f"✅ {len(custos_calculados)} registros de custos salvos com sucesso!")
        else:
            logging.warning("⚠️ Nenhum custo calculado para salvar.")
