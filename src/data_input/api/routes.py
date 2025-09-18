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
import subprocess
import re
from data_input.jobs import processar_csv

from authentication.utils.dependencies import obter_tenant_id_do_token
from data_input.infrastructure.database_connection import conectar_banco, fechar_conexao

router = APIRouter(tags=["Dashboard", "Data Input"])
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 🔹 Conexão Redis
redis_conn = Redis(host="redis", port=6379)
queue = Queue("data_input", connection=redis_conn)


# 🔹 Função auxiliar para normalizar tenant_id
def normalizar_tenant(tenant_id: str) -> str:
    try:
        if tenant_id and tenant_id.startswith("{"):
            tenant_payload = json.loads(tenant_id)
            tenant_id = tenant_payload.get("tenant_id") or tenant_payload.get("tenant")
            logger.info(f"📌 Tenant normalizado do payload: {tenant_id}")
        return tenant_id
    except Exception as e:
        logger.warning(f"⚠️ Não foi possível normalizar tenant_id={tenant_id}: {e}")
        return tenant_id


# 🔹 Healthcheck
@router.get("/health", summary="Healthcheck Data Input")
def healthcheck(request: Request):
    return {"status": "ok", "servico": "Data Input via Gateway"}


# 🔹 Executa pipeline já existente (modo síncrono via subprocess)
@router.post("/processar", summary="Executar pipeline de Data Input (síncrono)")
def processar_data_input(
    modo_forcar: bool = Query(False, description="Forçar reprocessamento"),
    limite_peso_kg: Optional[float] = Query(None, description="Peso máximo permitido por CTE"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    try:
        tenant_id = normalizar_tenant(tenant_id)
        logger.info(f"🔑 Tenant usado: {tenant_id}")

        comando = ["python", "-m", "data_input.main_preprocessing", "--tenant", tenant_id]
        if modo_forcar:
            comando.append("--modo_forcar")
        if limite_peso_kg is not None:
            comando.extend(["--limite-peso-kg", str(limite_peso_kg)])

        result = subprocess.run(
            comando, capture_output=True, text=True, encoding="utf-8", errors="ignore"
        )
        if result.returncode != 0:
            raise Exception(f"Subprocesso falhou:\n{result.stderr or result.stdout}")

        # Parse da saída para extrair resumo
        stdout_lines = result.stdout.strip().splitlines()
        clean_lines = [re.sub(r"^INFO:.*?:", "", l).strip() for l in stdout_lines]

        validos, invalidos, total_processados = 0, 0, 0
        for l in clean_lines:
            match = re.search(
                r".*Resumo:\s*(\d+)\s+válid(?:o|os)[,]?\s*(\d+)\s+inválid",
                l,
                re.IGNORECASE,
            )
            if match:
                validos, invalidos = int(match.group(1)), int(match.group(2))
                total_processados = validos + invalidos
                break

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "mensagem": f"✅ Data Input finalizado para tenant {tenant_id}",
            "total_processados": total_processados,
            "validos": validos,
            "invalidos": invalidos,
        }

    except Exception as e:
        logger.error("🚨 Erro no Data Input", exc_info=True)
        raise HTTPException(500, detail=f"Erro no Data Input: {str(e)}")


# 🔹 Upload assíncrono → salva arquivo e enfileira job
@router.post("/upload", summary="Upload de CSV e processar Data Input (assíncrono)")
async def upload_data_input(
    file: UploadFile = File(...),
    modo_forcar: bool = Query(False, description="Forçar reprocessamento"),
    limite_peso_kg: Optional[float] = Query(None, description="Peso máximo permitido por CTE"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    try:
        logger.info("📩 [UPLOAD] Requisição recebida no Data Input")
        logger.info(f"🔑 Tenant recebido no upload: {tenant_id}, Arquivo: {file.filename}")

        job_id = str(uuid.uuid4())

        tenant_dir = f"/app/src/data_input/tenants/{tenant_id}/input"
        os.makedirs(tenant_dir, exist_ok=True)

        # 🔑 Sempre sobrescreve como 'dados_input.csv'
        file_path = os.path.join(tenant_dir, "dados_input.csv")

        with open(file_path, "wb") as f:
            f.write(await file.read())

        job = queue.enqueue(
            processar_csv,
            job_id, tenant_id, file_path, modo_forcar, limite_peso_kg,
            job_timeout=1800,
            result_ttl=86400
        )

        return {"status": "processing", "job_id": job.id, "tenant_id": tenant_id}
    except Exception as e:
        logger.error("❌ Erro em upload", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao enfileirar job: {str(e)}")


# 🔹 Status do job
@router.get("/status/{job_id}", summary="Consultar status de processamento")
def job_status(job_id: str):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
        if job.is_finished:
            return {"status": "done", "result": job.result}
        elif job.is_failed:
            return {"status": "error", "error": str(job.exc_info)}
        else:
            return {
                "status": "processing",
                "progress": job.meta.get("progress", 0),
                "step": job.meta.get("step", "Em andamento")
            }
    except Exception as e:
        raise HTTPException(404, f"Job {job_id} não encontrado: {e}")

# 🔹 Dashboard: entregas últimos 30 dias
@router.get("/dashboard/ultimos-30-dias", summary="Entregas diárias dos últimos 30 dias")
def entregas_ultimos_30_dias(tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        tenant_id = normalizar_tenant(tenant_id)
        logger.debug(f"🔑 [ultimos-30-dias] tenant_id normalizado: {tenant_id}")
        conn = conectar_banco()
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
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, inicio, hoje))
            rows = cur.fetchall()
        cols = ["dia", "entregas", "valor_total", "peso_medio"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.exception(f"❌ Erro ao buscar entregas últimos 30 dias (tenant={tenant_id})")
        raise HTTPException(500, detail=f"Erro interno ao buscar entregas últimos 30 dias: {e}")
    finally:
        try:
            fechar_conexao(conn)
        except Exception:
            pass

# 🔹 Dashboard: entregas mensais
@router.get("/dashboard/mensal", summary="Entregas mensais (últimos 12 meses)")
def entregas_mensais(tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        tenant_id = normalizar_tenant(tenant_id)
        logger.debug(f"🔑 [mensal] tenant_id normalizado: {tenant_id}")
        conn = conectar_banco()
        hoje = date.today()
        inicio = (hoje.replace(day=1) - timedelta(days=365))

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
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, inicio))
            rows = cur.fetchall()
        cols = ["mes", "entregas", "valor_total"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.exception(f"❌ Erro ao buscar entregas mensais (tenant={tenant_id})")
        raise HTTPException(500, detail=f"Erro interno ao buscar entregas mensais: {e}")
    finally:
        try:
            fechar_conexao(conn)
        except Exception:
            pass

# 🔹 Dashboard: mapa
@router.get("/dashboard/mapa", summary="Mapa de entregas últimos 30 dias")
def mapa_entregas(tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        tenant_id = normalizar_tenant(tenant_id)
        logger.debug(f"🔑 [mapa] tenant_id normalizado: {tenant_id}")
        conn = conectar_banco()
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
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, inicio, hoje))
            rows = cur.fetchall()
        cols = ["envio_data", "lat", "lon", "cte_valor_nf"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        logger.exception(f"❌ Erro ao buscar mapa de entregas (tenant={tenant_id})")
        raise HTTPException(500, detail=f"Erro interno ao buscar mapa de entregas: {e}")
    finally:
        try:
            fechar_conexao(conn)
        except Exception:
            pass

