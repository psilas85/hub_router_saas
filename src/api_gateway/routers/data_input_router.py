# hub_router_1.0.1/src/api_gateway/routers/data_input_router.py

from fastapi import (
    APIRouter,
    Request,
    UploadFile,
    File,
    Query,
    Depends,
    HTTPException
)
from typing import Optional
import logging
import httpx

from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings
from authentication.utils.dependencies import obter_tenant_id_do_token

router = APIRouter(prefix="/data_input", tags=["Data Input"])
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DATA_INPUT_URL = settings.DATA_INPUT_URL


# 🔹 Healthcheck (proxy)
@router.get("/health", summary="Healthcheck Data Input")
async def healthcheck(request: Request):
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    return await forward_request("GET", f"{DATA_INPUT_URL}/data_input/health", headers=headers)


# 🔹 Executa pipeline (proxy)
@router.post("/processar", summary="Executar pipeline de Data Input")
async def processar_data_input(request: Request):
    body = await request.body()
    headers = {k: v for k, v in request.headers.items() if k.lower() != "host"}
    return await forward_request(
        "POST",
        f"{DATA_INPUT_URL}/data_input/processar",
        headers=headers,
        data=body
    )


# 🔹 Upload de CSV + pipeline (proxy para o serviço data_input)
@router.post("/upload", summary="Upload de CSV e processar Data Input")
async def upload_data_input(
    request: Request,
    file: UploadFile = File(...),
    modo_forcar: bool = Query(False, description="Forçar reprocessamento"),
    limite_peso_kg: Optional[float] = Query(None, description="Peso máximo por CTE"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    try:
        logger.info("📩 [UPLOAD] Proxy recebido no API Gateway")
        logger.info(f"🔑 Tenant: {tenant_id}, Arquivo: {file.filename}")

        # monta a URL de destino
        url = f"{DATA_INPUT_URL}/data_input/upload?modo_forcar={str(modo_forcar).lower()}"
        if limite_peso_kg is not None:
            url += f"&limite_peso_kg={limite_peso_kg}"

        # monta os dados multipart
        files = {"file": (file.filename, await file.read(), file.content_type)}

        async with httpx.AsyncClient(timeout=1800.0) as client:
            response = await client.post(
                url,
                headers={k: v for k, v in request.headers.items() if k.lower() != "content-length"},
                files=files,
            )

        response.raise_for_status()
        result = response.json()

        # 🔹 Retorna sempre job_id + tenant_id + status
        retorno = {
            "status": result.get("status"),
            "job_id": result.get("job_id"),
            "tenant_id": result.get("tenant_id"),
        }

        # 🔹 Se já houver métricas ou progresso
        for campo in ["mensagem", "total_processados", "validos", "invalidos", "progress", "step"]:
            if campo in result:
                retorno[campo] = result[campo]

        return retorno

    except Exception as e:
        logger.error("❌ Erro ao encaminhar upload para data_input", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao encaminhar upload: {str(e)}")


# 🔹 Status do job (proxy)
@router.get("/status/{job_id}", summary="Consultar status de processamento de Data Input")
async def job_status(job_id: str, request: Request):
    try:
        async with httpx.AsyncClient(timeout=1800.0) as client:
            resp = await client.get(
                f"{DATA_INPUT_URL}/data_input/status/{job_id}",
                headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            )
        resp.raise_for_status()
        result = resp.json()

        # 🔹 Normaliza a saída
        retorno = {
            "status": result.get("status"),
        }

        if "result" in result and isinstance(result["result"], dict):
            retorno.update(result["result"])
        else:
            # se o backend já devolveu progress/step diretamente
            for campo in ["job_id", "tenant_id", "total_processados", "validos", "invalidos", "mensagem", "progress", "step"]:
                if campo in result:
                    retorno[campo] = result[campo]

        return retorno

    except Exception as e:
        logger.error(f"❌ Erro no proxy status para job {job_id}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro no proxy status: {str(e)}")


@router.get("/dashboard/ultimos-30-dias")
async def ultimos_30_dias(request: Request):
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.get(
                f"{DATA_INPUT_URL}/data_input/dashboard/ultimos-30-dias",
                headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no proxy ultimos-30-dias: {str(e)}")


@router.get("/dashboard/mensal")
async def mensal(request: Request):
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.get(
                f"{DATA_INPUT_URL}/data_input/dashboard/mensal",
                headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no proxy mensal: {str(e)}")


@router.get("/dashboard/mapa")
async def mapa(request: Request):
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            resp = await client.get(
                f"{DATA_INPUT_URL}/data_input/dashboard/mapa",
                headers={k: v for k, v in request.headers.items() if k.lower() != "host"},
            )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no proxy mapa: {str(e)}")
