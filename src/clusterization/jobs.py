from datetime import date
from typing import Optional

from rq import get_current_job

from clusterization.application.clusterization_runner import executar_clusterizacao_pipeline


CLUSTERIZATION_JOBS_QUEUE = "clusterization_jobs"


def processar_clusterizacao_job(
    tenant_id: str,
    data: str,
    data_final: Optional[str],
    hub_central_id: int,
    min_entregas_por_cluster_alvo: int,
    max_entregas_por_cluster_alvo: int,
    raio_cluster_hub_central: float,
):
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
        update(0, "Job iniciado")
        resultado = executar_clusterizacao_pipeline(
            tenant_id=tenant_id,
            data=date.fromisoformat(data),
            data_final=date.fromisoformat(data_final) if data_final else None,
            hub_central_id=hub_central_id,
            min_entregas_por_cluster_alvo=min_entregas_por_cluster_alvo,
            max_entregas_por_cluster_alvo=max_entregas_por_cluster_alvo,
            raio_cluster_hub_central=raio_cluster_hub_central,
            progress=update,
        )
        update(100, "Concluído", {"result": resultado})
        return resultado
    except Exception as exc:
        update(100, f"Erro: {exc}", {"error": str(exc)})
        raise
