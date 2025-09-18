# api_gateway/routers/costs_last_mile_routes.py

from fastapi import APIRouter, Request, Query, HTTPException, Depends
from datetime import date
from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings
from authentication.utils.dependencies import obter_tenant_id_do_token
import os
from urllib.parse import quote

router = APIRouter(prefix="/costs_last_mile", tags=["Costs Last Mile"])

COSTS_LAST_MILE_URL = settings.COSTS_LAST_MILE_URL

OUTPUT_BASE = "/app/exports"
PUBLIC_BASE = "/exports"


# -------------------------
# Healthcheck
# -------------------------
@router.get("/health", summary="Healthcheck Costs Last Mile")
async def healthcheck(request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    result = await forward_request("GET", f"{COSTS_LAST_MILE_URL}/costs_last_mile/", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


# -------------------------
# Processar custos
# -------------------------
@router.post("/processar", summary="Calcular custos de last mile")
async def calcular_custos(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial (AAAA-MM-DD)"),
    data_final: date | None = Query(None, description="Data final (AAAA-MM-DD)"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}

    params = {
        "data_inicial": data_inicial,
        "data_final": data_final or data_inicial,
        "modo_forcar": "true",  # 🔒 sempre força
    }

    result = await forward_request(
        "POST",
        f"{COSTS_LAST_MILE_URL}/costs_last_mile/",
        headers=headers,
        params=params,
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


# -------------------------
# Visualizar relatórios
# -------------------------
@router.get("/visualizar", summary="Visualizar relatórios de custos last mile")
async def visualizar_custos(
    request: Request,
    data: date = Query(..., description="Data de envio (AAAA-MM-DD)"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}

    params = {"data": data}

    result = await forward_request(
        "GET",
        f"{COSTS_LAST_MILE_URL}/costs_last_mile/visualizar",
        headers=headers,
        params=params,
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


# -------------------------
# CRUD de veículos
# -------------------------

@router.get("/vehicles", summary="Listar veículos Last-Mile")
async def listar_veiculos(request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}

    result = await forward_request(
        "GET",
        f"{COSTS_LAST_MILE_URL}/costs_last_mile/vehicles",
        headers=headers
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.post("/vehicles", summary="Adicionar veículo Last-Mile")
async def adicionar_veiculo(request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    body = await request.json()

    result = await forward_request(
        "POST",
        f"{COSTS_LAST_MILE_URL}/costs_last_mile/vehicles",
        headers=headers,
        json=body
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.put("/vehicles/{veiculo:path}", summary="Editar veículo Last-Mile")
async def editar_veiculo(veiculo: str, request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    body = await request.json()

    veiculo_encoded = quote(veiculo, safe="")

    result = await forward_request(
        "PUT",
        f"{COSTS_LAST_MILE_URL}/costs_last_mile/vehicles/{veiculo_encoded}",
        headers=headers,
        json=body
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.delete("/vehicles/{veiculo:path}", summary="Remover veículo Last-Mile")
async def remover_veiculo(veiculo: str, request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}

    veiculo_encoded = quote(veiculo, safe="")

    result = await forward_request(
        "DELETE",
        f"{COSTS_LAST_MILE_URL}/costs_last_mile/vehicles/{veiculo_encoded}",
        headers=headers
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

# -------------------------
# Artefatos (PDF)
# -------------------------
@router.get("/artefatos", summary="Listar artefatos de custos Last Mile")
async def listar_artefatos(
    data: str = Query(..., description="Data no formato YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    artefatos = []
    pdf_file = os.path.join(
        OUTPUT_BASE, "costs_last_mile", "relatorios", tenant_id, f"costs_last_mile_{data}.pdf"
    )
    if os.path.isfile(pdf_file):
        artefatos.append(
            {
                "data": data,
                "pdf_url": f"{PUBLIC_BASE}/costs_last_mile/relatorios/{tenant_id}/costs_last_mile_{data}.pdf",
            }
        )
    return {"status": "ok", "artefatos": artefatos}
