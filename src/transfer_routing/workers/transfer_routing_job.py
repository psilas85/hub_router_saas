# src/transfer_routing/workers/transfer_routing_job.py

from datetime import date
from typing import Optional

from transfer_routing.application.transfer_routing_use_case import TransferRoutingUseCase


TRANSFER_ROUTING_JOBS_QUEUE = "transfer_routing_jobs"


def processar_transfer_routing_job(
    tenant_id: str,
    envio_data: str,
    modo_forcar: bool,
    tempo_maximo: float,
    tempo_parada_leve: float,
    peso_leve_max: float,
    tempo_parada_pesada: float,
    tempo_por_volume: float,
):
    from rq import get_current_job
    job = get_current_job()

    def update(progress: int, step: str, extra: Optional[dict] = None):
        if not job:
            return
        job.meta["progress"] = progress
        job.meta["step"] = step
        if extra:
            job.meta.update(extra)
        job.save_meta()

    try:
        update(0, "Iniciando")
        roteirizador = TransferRoutingUseCase(
            tenant_id=tenant_id,
            modo_forcar=modo_forcar,
            tempo_maximo=tempo_maximo,
            tempo_parada_leve=tempo_parada_leve,
            peso_leve_max=peso_leve_max,
            tempo_parada_pesada=tempo_parada_pesada,
            tempo_por_volume=tempo_por_volume,
            progress_callback=update,
        )
        resultado = roteirizador.run(
            data_inicial=date.fromisoformat(envio_data),
            data_final=date.fromisoformat(envio_data),
        )
        update(100, "Concluído", {"result": resultado})
        return resultado
    except Exception as exc:
        update(100, f"Erro: {exc}", {"error": str(exc)})
        raise
