# transfer_routing/application/transfer_routing_use_case.py

from datetime import date

from transfer_routing.domain.transfer_planner import TransferPlanner
from transfer_routing.infrastructure.database_connection import (
    conectar_banco_cluster,
    conectar_banco_routing,
    fechar_conexao
)
from transfer_routing.infrastructure.database_writer import (
    contar_roteirizacao_transferencias,
    existe_roteirizacao_transferencias,
    excluir_roteirizacao_transferencias
)
from transfer_routing.logs.logging_factory import LoggerFactory


class TransferRoutingUseCase:
    def __init__(self, tenant_id: str, modo_forcar: bool, tempo_maximo: float, tempo_parada_leve: float,
                 peso_leve_max: float, tempo_parada_pesada: float, tempo_por_volume: float,
                 progress_callback=None):
        self.tenant_id = tenant_id
        self.modo_forcar = modo_forcar
        self.tempo_maximo = tempo_maximo
        self.tempo_parada_leve = tempo_parada_leve
        self.peso_leve_max = peso_leve_max
        self.tempo_parada_pesada = tempo_parada_pesada
        self.tempo_por_volume = tempo_por_volume
        self.progress_callback = progress_callback
        self.logger = LoggerFactory.get_logger("transfer_routing")

    def run(self, data_inicial: date, data_final: date):
        def _progress(p: int, s: str):
            if self.progress_callback:
                self.progress_callback(p, s)

        conn_cluster = conectar_banco_cluster()
        conn_routing = conectar_banco_routing()

        try:
            _progress(5, "Verificando dados existentes")
            resumo_count, detalhes_count = contar_roteirizacao_transferencias(
                self.tenant_id,
                data_inicial,
                conn_routing,
            )
            if resumo_count > 0 and detalhes_count == 0:
                self.logger.warning(
                    f"Roteirização incompleta para {data_inicial}: "
                    f"{resumo_count} resumos e {detalhes_count} detalhes. Limpando parcial..."
                )
                excluir_roteirizacao_transferencias(
                    self.tenant_id,
                    data_inicial,
                    conn_routing,
                    self.logger,
                )

            if existe_roteirizacao_transferencias(self.tenant_id, data_inicial, conn_routing):
                if self.modo_forcar:
                    excluir_roteirizacao_transferencias(self.tenant_id, data_inicial, conn_routing, self.logger)
                    self.logger.info(f"Processando novamente para {data_inicial} (modo forçar ativo)...")
                else:
                    mensagem = (
                        f"Já existe roteirização para {data_inicial}. "
                        "Nenhum dado foi reprocessado. Ative forçar sobrescrita para substituir o resultado salvo."
                    )
                    self.logger.info(f"{mensagem} tenant={self.tenant_id}")
                    return {
                        "status": "skipped_existing",
                        "processed": False,
                        "envio_data": str(data_inicial),
                        "mensagem": mensagem,
                        "resumos_existentes": resumo_count,
                        "detalhes_existentes": detalhes_count,
                    }

            _progress(10, "Iniciando roteirização")
            planner = TransferPlanner(
                tenant_id=self.tenant_id,
                tempo_maximo=self.tempo_maximo,
                tempo_parada_leve=self.tempo_parada_leve,
                peso_leve_max=self.peso_leve_max,
                tempo_parada_pesada=self.tempo_parada_pesada,
                tempo_por_volume=self.tempo_por_volume,
                progress_callback=_progress,
            )

            planner.executar(
                envio_data=data_inicial,
                conn_cluster=conn_cluster,
                conn_routing=conn_routing,
                logger=self.logger
            )

            self.logger.info("Processamento de roteirização de transferências concluído com sucesso.")
            return {
                "status": "processed",
                "processed": True,
                "envio_data": str(data_inicial),
                "mensagem": f"Transferências processadas com sucesso para {data_inicial}.",
            }

        except Exception as e:
            self.logger.error(f"Erro na roteirização de transferências: {e}")
            raise

        finally:
            fechar_conexao(conn_cluster)
            fechar_conexao(conn_routing)
