from simulation.infrastructure.simulation_database_writer import inserir_hub, remover_hub

class HubUseCase:
    def __init__(self, simulation_db, logger):
        self.simulation_db = simulation_db
        self.logger = logger

    def adicionar_hub(self, tenant_id, nome, endereco, lat, lon):
        self.logger.info(f"➕ Adicionando hub '{nome}'...")
        inserir_hub(self.simulation_db, tenant_id, nome, endereco, lat, lon)
        self.logger.info("✅ Hub adicionado ou atualizado com sucesso.")

    def excluir_hub(self, tenant_id, nome):
        self.logger.info(f"🗑️ Removendo hub '{nome}'...")
        remover_hub(self.simulation_db, tenant_id, nome)
        self.logger.info("✅ Hub removido com sucesso.")
