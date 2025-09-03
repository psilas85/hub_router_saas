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
import subprocess
import os
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
    return await forward_request("GET", f"{DATA_INPUT_URL}/data_input/health", request=request)


# 🔹 Executa pipeline (proxy)
@router.post("/processar", summary="Executar pipeline de Data Input")
async def processar_data_input(request: Request):
    body = await request.body()
    return await forward_request(
        "POST",
        f"{DATA_INPUT_URL}/data_input/processar",
        request=request,
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

        async with httpx.AsyncClient(timeout=600.0) as client:
            response = await client.post(
                url,
                headers={k: v for k, v in request.headers.items() if k.lower() != "content-length"},
                files=files,
            )

        response.raise_for_status()
        result = response.json()

        # 🔹 garante que sempre retorna métricas no JSON
        return {
            "status": result.get("status"),
            "tenant_id": result.get("tenant_id"),
            "mensagem": result.get("mensagem"),
            "total_processados": result.get("total_processados"),
            "validos": result.get("validos"),
            "invalidos": result.get("invalidos"),
        }


    except Exception as e:
        logger.error("❌ Erro ao encaminhar upload para data_input", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao encaminhar upload: {str(e)}")
