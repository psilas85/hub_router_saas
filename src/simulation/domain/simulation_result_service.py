# simulation/domain/simulation_result_service.py

from simulation.infrastructure.simulation_database_writer import persistir_resultado_simulacao

class SimulationResultService:

    def __init__(self, db, logger):
        self.db = db
        self.logger = logger

    def salvar_resultado(self, dados: dict, modo_forcar: bool = False, auto_commit: bool = True):
        self.logger.info("💾 Salvando resultado da simulação...")

        if modo_forcar:
            self.logger.info(f"♻️ Limpando resultado existente para tenant_id={dados['tenant_id']}, envio_data={dados['envio_data']}, k={dados['k_clusters']}")
            try:
                cursor = self.db.cursor()
                cursor.execute("""
                    DELETE FROM resultados_simulacao
                    WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
                """, (dados["tenant_id"], dados["envio_data"], dados["k_clusters"]))
                if auto_commit:
                    self.db.commit()
                cursor.close()
            except Exception as e:
                self.logger.warning(f"⚠️ Falha ao limpar resultados existentes: {e}")
                self.db.rollback()

        persistir_resultado_simulacao(
            db=self.db,
            simulation_id=dados["simulation_id"],
            tenant_id=dados["tenant_id"],
            envio_data=dados["envio_data"],
            k_clusters=dados["k_clusters"],
            custo_total=dados["custo_total"],
            quantidade_entregas=dados["quantidade_entregas"],
            custo_transferencia=dados["custo_transferencia"],
            custo_last_mile=dados["custo_last_mile"],
            custo_cluster=dados.get("custo_cluster", 0.0),
            is_ponto_otimo=dados.get("is_ponto_otimo", False),
            auto_commit=auto_commit,
        )

        self.logger.info("✅ Resultado da simulação salvo com sucesso.")

