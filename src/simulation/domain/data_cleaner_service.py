#hub_router_1.0.1/src/simulation/domain/data_cleaner_service.py

import shutil
import os
from pathlib import Path

class DataCleanerService:
    def __init__(self, db_conn, tenant_id: str, envio_data: str, logger,
                 output_dir="exports/simulation/maps", simulation_id: str = None):
        self.db_conn = db_conn
        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.simulation_id = simulation_id
        self.logger = logger
        # garante caminho absoluto
        self.output_dir = Path(output_dir).resolve()

    def limpar_tabelas(self):
        """
        Executa limpeza em lote de todas as tabelas relacionadas à simulação.
        """
        sql = """
        WITH del1 AS (
            DELETE FROM detalhes_rotas WHERE tenant_id = %s AND envio_data = %s
        ), del2 AS (
            DELETE FROM detalhes_transferencias WHERE tenant_id = %s AND envio_data = %s
        ), del3 AS (
            DELETE FROM entregas_clusterizadas WHERE tenant_id = %s AND envio_data = %s
        ), del4 AS (
            DELETE FROM resultados_simulacao WHERE tenant_id = %s AND envio_data = %s
        ), del5 AS (
            DELETE FROM resumo_clusters WHERE tenant_id = %s AND envio_data = %s
        ), del6 AS (
            DELETE FROM resumo_rotas_last_mile WHERE tenant_id = %s AND envio_data = %s
        ), del7 AS (
            DELETE FROM resumo_transferencias WHERE tenant_id = %s AND envio_data = %s
        ), del8 AS (
            DELETE FROM rotas_last_mile WHERE tenant_id = %s AND envio_data = %s
        )
        DELETE FROM rotas_transferencias WHERE tenant_id = %s AND envio_data = %s;
        """
        cursor = self.db_conn.cursor()
        try:
            cursor.execute(sql, [self.tenant_id, self.envio_data] * 9)
            self.db_conn.commit()
            self.logger.info(f"🗑️ Todas as tabelas limpas para envio_data={self.envio_data}, tenant={self.tenant_id}")
        except Exception as e:
            self.db_conn.rollback()
            self.logger.error(f"❌ Erro ao limpar tabelas: {e}")
        finally:
            cursor.close()

    def limpar_artefatos(self):
        """
        Remove todos os arquivos exportados (maps, graphs, relatorios, csv, json)
        relacionados ao tenant e envio_data.
        """
        subdirs = ["maps", "graphs", "relatorios", "csv", "json"]

        for sub in subdirs:
            # Caminho do diretório: exports/simulation/<sub>/<tenant_id>
            dir_path = self.output_dir.parent / sub / str(self.tenant_id)

            if dir_path.exists():
                arquivos = list(dir_path.glob(f"*{self.envio_data}*"))
                if arquivos:
                    for f in arquivos:
                        try:
                            if f.is_file():
                                f.unlink()  # remove arquivo
                                self.logger.info(f"🗑️ Artefato removido: {f}")
                            elif f.is_dir():
                                shutil.rmtree(f, ignore_errors=True)
                                self.logger.info(f"🗑️ Diretório removido: {f}")
                        except Exception as e:
                            self.logger.error(f"❌ Erro ao remover {f}: {e}")
                else:
                    self.logger.info(
                        f"ℹ️ Nenhum artefato encontrado em {dir_path} para envio_data={self.envio_data}"
                    )

    def limpar_completo(self):
        """
        Executa a limpeza total: tabelas + artefatos exportados.
        """
        self.logger.info(f"♻️ Limpando dados e artefatos de {self.envio_data} (tenant={self.tenant_id})...")
        self.limpar_tabelas()
        self.limpar_artefatos()
        self.logger.info("✅ Limpeza concluída.")
