#hub_router_1.0.1/src/data_input/api/routes.py

from fastapi import APIRouter, Query, Depends, HTTPException, UploadFile, File, Request
from typing import Optional
import os
import logging
import json
import uuid
from datetime import date, timedelta
from redis import Redis
from rq import Queue
from rq.job import Job

from data_input.workers.data_input_job import processar_data_input
from data_input.infrastructure.manual_data_entry import process_manual_data_input
from data_input.infrastructure.db_connection import get_connection_context
from data_input.infrastructure.database_writer import DatabaseWriter
from data_input.api.dependencies import verify_token

from fastapi.responses import FileResponse

try:
    from rq.exceptions import NoSuchJobError
except ImportError:
    class NoSuchJobError(Exception):
        pass


router = APIRouter(tags=["Dashboard", "Data Input"])
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_conn = Redis(host="redis", port=6379)
queue = Queue("data_input_jobs", connection=redis_conn)


# ============================================================
# 🔹 UTILS
# ============================================================

def get_tenant_id(request: Request) -> str:
    user = getattr(request.state, "user", {})
    tenant_id = user.get("tenant_id") or user.get("tenant")

    if not tenant_id:
        raise HTTPException(status_code=401, detail="tenant_id não encontrado no token")

    return str(tenant_id)


def normalizar_tenant(tenant_id: str) -> str:
    try:
        if tenant_id and tenant_id.startswith("{"):
            tenant_payload = json.loads(tenant_id)
            tenant_id = tenant_payload.get("tenant_id") or tenant_payload.get("tenant")
        return tenant_id
    except Exception:
        return tenant_id


def salvar_historico_manual(
    tenant_id: str,
    request_id: str,
    arquivo: str,
    status: str,
    total_processados: int,
    validos: int,
    invalidos: int,
    mensagem: str,
) -> None:
    try:
        with get_connection_context() as conn:
            writer = DatabaseWriter(conn)
            writer.salvar_historico_data_input(
                tenant_id=tenant_id,
                job_id=request_id,
                arquivo=arquivo,
                status=status,
                total_processados=total_processados,
                validos=validos,
                invalidos=invalidos,
                mensagem=mensagem,
                tipo_processamento="manual",
            )
    except Exception:
        logger.exception("Erro ao salvar histórico manual")


# ============================================================
# 🔹 HEALTH
# ============================================================

@router.get("/health")
def healthcheck():
    return {"status": "ok", "servico": "Data Input"}


# ============================================================
# 🔹 UPLOAD
# ============================================================

@router.post("/upload", dependencies=[Depends(verify_token)])
async def upload_data_input(
    request: Request,
    file: UploadFile = File(...),
    modo_forcar: bool = Query(False),
    limite_peso_kg: Optional[float] = Query(None),
):
    try:
        tenant_id = get_tenant_id(request)

        logger.info(f"[UPLOAD] tenant={tenant_id} arquivo={file.filename}")

        if not file.filename.endswith(".xlsx"):
            raise HTTPException(400, "Arquivo deve ser .xlsx")

        job_id = str(uuid.uuid4())

        BASE_PATH = os.getenv("DATA_INPUT_PATH", "/app/src/data_input")

        tenant_dir = os.path.join(BASE_PATH, "tenants", tenant_id, "input")
        os.makedirs(tenant_dir, exist_ok=True)

        file_path = os.path.join(tenant_dir, f"dados_input_{job_id}.xlsx")

        content = await file.read()

        if not content:
            raise HTTPException(400, "Arquivo vazio")

        with open(file_path, "wb") as f:
            f.write(content)

        job = queue.enqueue(
            processar_data_input,
            tenant_id,
            file_path,
            modo_forcar,
            limite_peso_kg,
            job_timeout=3600  # 🔥 AQUI
        )
        return {
            "status": "processing",
            "job_id": job.id,
            "tenant_id": tenant_id
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Erro upload")
        raise HTTPException(500, str(e))


@router.post("/upload/manual", dependencies=[Depends(verify_token)])
async def upload_data_input_manual(
    request: Request,
    file: UploadFile = File(...),
):
    request_id = str(uuid.uuid4())
    tenant_id = None
    try:
        tenant_id = get_tenant_id(request)

        logger.info(
            f"[UPLOAD_MANUAL] tenant={tenant_id} arquivo={file.filename}"
        )

        if not file.filename.endswith(".xlsx"):
            raise HTTPException(400, "Arquivo deve ser .xlsx")

        BASE_PATH = os.getenv("DATA_INPUT_PATH", "/app/src/data_input")
        tenant_dir = os.path.join(BASE_PATH, "tenants", tenant_id, "input")
        os.makedirs(tenant_dir, exist_ok=True)

        file_path = os.path.join(
            tenant_dir,
            f"dados_input_manual_{request_id}.xlsx"
        )

        content = await file.read()
        if not content:
            raise HTTPException(400, "Arquivo vazio")

        with open(file_path, "wb") as f:
            f.write(content)

        result = process_manual_data_input(file_path, tenant_id)
        result["request_id"] = request_id
        result["arquivo"] = file.filename
        salvar_historico_manual(
            tenant_id=tenant_id,
            request_id=request_id,
            arquivo=file.filename,
            status="done",
            total_processados=result.get("total_processados", 0),
            validos=result.get("validos", 0),
            invalidos=result.get("invalidos", 0),
            mensagem=result.get("mensagem", "Entrada manual concluída"),
        )
        return result

    except ValueError as e:
        if tenant_id:
            salvar_historico_manual(
                tenant_id=tenant_id,
                request_id=request_id,
                arquivo=file.filename,
                status="error",
                total_processados=0,
                validos=0,
                invalidos=0,
                mensagem=str(e),
            )
        raise HTTPException(400, str(e))
    except HTTPException:
        raise
    except Exception as e:
        if tenant_id:
            salvar_historico_manual(
                tenant_id=tenant_id,
                request_id=request_id,
                arquivo=file.filename,
                status="error",
                total_processados=0,
                validos=0,
                invalidos=0,
                mensagem=str(e),
            )
        logger.exception("Erro upload manual")
        raise HTTPException(500, str(e))


# ============================================================
# 🔹 JOB STATUS
# ============================================================

@router.get("/status/{job_id}")
def job_status(job_id: str):
    try:
        job = Job.fetch(job_id, connection=redis_conn)

        if job.is_finished:
            result = job.meta.get("result") or job.result or {}

            return {
                "status": "done",
                "job_id": job.id,
                "tenant_id": result.get("tenant_id"),
                "mensagem": result.get("mensagem"),
                "total_processados": result.get("total_processados", 0),
                "validos": result.get("validos", 0),
                "invalidos": result.get("invalidos", 0),
                "result": result,
            }

        if job.is_failed:
            return {
                "status": "error",
                "job_id": job.id,
                "error": str(job.exc_info),
            }

        return {
            "status": "processing",
            "job_id": job.id,
            "progress": job.meta.get("progress", 0),
            "step": job.meta.get("step", "Em andamento"),
            "total_processados": job.meta.get("total_processados", 0),
            "validos": job.meta.get("validos", 0),
            "invalidos": job.meta.get("invalidos", 0),
        }

    except NoSuchJobError:
        return {"status": "not_found"}

    except Exception as e:
        logger.exception("Erro job_status")
        raise HTTPException(500, str(e))


# ============================================================
# 🔹 DASHBOARD - 30 DIAS
# ============================================================

@router.get("/dashboard/ultimos-30-dias", dependencies=[Depends(verify_token)])
def entregas_ultimos_30_dias(request: Request):

    tenant_id = normalizar_tenant(get_tenant_id(request))

    hoje = date.today()
    inicio = hoje - timedelta(days=30)

    query = """
        SELECT envio_data::date AS dia,
               COUNT(*) AS entregas,
               SUM(cte_valor_nf) AS valor_total,
               AVG(cte_peso) AS peso_medio
        FROM entregas
        WHERE tenant_id = %s
          AND envio_data BETWEEN %s AND %s
        GROUP BY envio_data::date
        ORDER BY envio_data::date
    """

    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, inicio, hoje))
            rows = cur.fetchall()

    cols = ["dia", "entregas", "valor_total", "peso_medio"]
    return [dict(zip(cols, r)) for r in rows]


# ============================================================
# 🔹 DASHBOARD MENSAL
# ============================================================

@router.get("/dashboard/mensal", dependencies=[Depends(verify_token)])
def entregas_mensais(request: Request):

    tenant_id = normalizar_tenant(get_tenant_id(request))

    hoje = date.today()
    inicio = hoje.replace(day=1) - timedelta(days=365)

    query = """
        SELECT to_char(envio_data, 'YYYY-MM') AS mes,
               COUNT(*) AS entregas,
               SUM(cte_valor_nf) AS valor_total
        FROM entregas
        WHERE tenant_id = %s
          AND envio_data >= %s
        GROUP BY 1
        ORDER BY 1
    """

    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, inicio))
            rows = cur.fetchall()

    cols = ["mes", "entregas", "valor_total"]
    return [dict(zip(cols, r)) for r in rows]


# ============================================================
# 🔹 DASHBOARD MAPA
# ============================================================

@router.get("/dashboard/mapa", dependencies=[Depends(verify_token)])
def mapa_entregas(request: Request):

    tenant_id = normalizar_tenant(get_tenant_id(request))

    hoje = date.today()
    inicio = hoje - timedelta(days=30)

    query = """
        SELECT envio_data,
               destino_latitude AS lat,
               destino_longitude AS lon,
               cte_valor_nf
        FROM entregas
        WHERE tenant_id = %s
          AND envio_data BETWEEN %s AND %s
          AND destino_latitude IS NOT NULL
          AND destino_longitude IS NOT NULL
    """

    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, inicio, hoje))
            rows = cur.fetchall()

    cols = ["envio_data", "lat", "lon", "cte_valor_nf"]
    return [dict(zip(cols, r)) for r in rows]


# ============================================================
# 🔹 HISTÓRICO
# ============================================================

@router.get("/historico", dependencies=[Depends(verify_token)])
def listar_historico(
    request: Request,
    limit: int = Query(5, ge=1, le=50)
):

    tenant_id = normalizar_tenant(get_tenant_id(request))

    query = """
        SELECT job_id, arquivo, status, total_processados,
             validos, invalidos, mensagem, tipo_processamento, criado_em
        FROM historico_data_input
        WHERE tenant_id = %s
        ORDER BY criado_em DESC
        LIMIT %s
    """

    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, limit))
            rows = cur.fetchall()

    cols = [
        "job_id", "arquivo", "status", "total_processados",
        "validos", "invalidos", "mensagem", "tipo_processamento",
        "criado_em"
    ]

    return [dict(zip(cols, r)) for r in rows]


# ============================================================
# 🔹 DOWNLOAD
# ============================================================

@router.get("/download/{job_id}", dependencies=[Depends(verify_token)])
def download_resultado(request: Request, job_id: str):

    tenant_id = normalizar_tenant(get_tenant_id(request))

    BASE_PATH = os.getenv("DATA_INPUT_PATH", "/app/src/data_input")

    file_path = os.path.join(
        BASE_PATH,
        "tenants",
        tenant_id,
        "output",
        f"{job_id}.xlsx"
    )

    if not os.path.exists(file_path):
        raise HTTPException(404, "Arquivo não encontrado")

    return FileResponse(
        file_path,
        filename=f"resultado_{job_id}.xlsx"
    )


@router.get("/download_invalidos/{job_id}", dependencies=[Depends(verify_token)])
def download_invalidos(request: Request, job_id: str):

    tenant_id = normalizar_tenant(get_tenant_id(request))

    BASE_PATH = os.getenv("DATA_INPUT_PATH", "/app/src/data_input")

    file_path = os.path.join(
        BASE_PATH,
        "tenants",
        tenant_id,
        "output",
        f"{job_id}.xlsx"
    )

    if not os.path.exists(file_path):
        raise HTTPException(404, "Arquivo não encontrado")

    # retorna o mesmo arquivo (com aba invalidos)
    return FileResponse(
        file_path,
        filename=f"invalidos_{job_id}.xlsx"
    )