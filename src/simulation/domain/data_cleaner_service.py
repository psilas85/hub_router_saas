#hub_router_1.0.1/src/simulation/domain/data_cleaner_service.py

import shutil
from pathlib import Path

class DataCleanerService:
    def __init__(
        self,
        db_conn,
        tenant_id: str,
        envio_data: str,
        logger,
        output_dir="exports/simulation",
        simulation_id: str = None
    ):
        self.db_conn = db_conn
        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.simulation_id = simulation_id
        self.logger = logger
        self.output_dir = Path(output_dir).resolve()

    def limpar_tabelas(self):
        cursor = self.db_conn.cursor()

        try:
            sql = """
            DELETE FROM detalhes_rotas WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM detalhes_transferencias WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM entregas_clusterizadas WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM resultados_simulacao WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM resumo_clusters WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM resumo_rotas_last_mile WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM resumo_transferencias WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM rotas_last_mile WHERE tenant_id=%s AND envio_data=%s;
            DELETE FROM rotas_transferencias WHERE tenant_id=%s AND envio_data=%s;
            """

            params = [self.tenant_id, self.envio_data] * 9

            cursor.execute(sql, params)
            self.db_conn.commit()

            self.logger.info(
                f"🗑️ Tabelas limpas: tenant={self.tenant_id} | data={self.envio_data}"
            )

        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"❌ Erro ao limpar banco: {e}")

        finally:
            cursor.close()

    def limpar_artefatos(self):
        base_path = self.output_dir
        target_path = base_path / str(self.tenant_id) / str(self.envio_data)

        if target_path.exists():
            try:
                shutil.rmtree(target_path)
                self.logger.info(f"🗑️ Artefatos removidos: {target_path}")
            except Exception as e:
                self.logger.error(f"❌ Erro ao remover artefatos: {e}")
        else:
            self.logger.info(f"ℹ️ Nenhum artefato encontrado: {target_path}")

    def limpar_completo(self):
        self.logger.info(f"♻️ Limpando tudo | tenant={self.tenant_id} | data={self.envio_data}")
        self.limpar_tabelas()
        self.limpar_artefatos()
        self.logger.info("✅ Limpeza concluída")