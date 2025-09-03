# api_gateway/routers/costs_transfer_routes.py
from fastapi import APIRouter, Request, Query, HTTPException, Depends

from fastapi import Response
from datetime import date
import os
import glob
from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings
from authentication.utils.dependencies import obter_tenant_id_do_token

router = APIRouter(prefix="/costs_transfer", tags=["Costs Transfer"])
COSTS_TRANSFER_URL = settings.COSTS_TRANSFER_URL  # ex.: http://costs_transfer_service:8005

# ---------- Health ----------
@router.get("/health", summary="Healthcheck Costs Transfer")
async def healthcheck(request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    result = await forward_request("GET", f"{COSTS_TRANSFER_URL}/custos_transferencia/", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

# ---------- Processar ----------
@router.post("/processar", summary="Calcular custos de transferência")
async def calcular_custos_transferencia(
    request: Request,
    data_inicial: date = Query(...),
    data_final: date = Query(None),
    modo_forcar: bool = Query(False),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}

    params = {
        "data_inicial": data_inicial.isoformat(),
        "modo_forcar": str(modo_forcar).lower()
    }
    if data_final:
        params["data_final"] = data_final.isoformat()

    result = await forward_request(
        "POST", f"{COSTS_TRANSFER_URL}/custos_transferencia/", headers=headers, params=params
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.get("/visualizar", summary="Visualizar relatório de custos de transferência")
async def visualizar_custos_transferencia(
    request: Request,
    data: date = Query(...),
    modo_forcar: bool = Query(False),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}

    params = {"data": data.isoformat(), "modo_forcar": str(modo_forcar).lower()}

    result = await forward_request(
        "GET", f"{COSTS_TRANSFER_URL}/custos_transferencia/visualizar", headers=headers, params=params
    )

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    content_type = result["headers"].get("content-type", "").lower()

    # 👉 Se for PDF, devolve como Response binário
    if content_type.startswith("application/pdf"):
        return Response(
            content=result["content"],
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'inline; filename="relatorio_transfer_costs_{data}.pdf"'
            },
        )

    # Caso contrário (JSON, texto), devolve direto
    return result["content"]


# ---------- Tarifa CRUD ----------
@router.get("/tarifas", summary="Listar tarifas")
async def listar_tarifas(request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    result = await forward_request("GET", f"{COSTS_TRANSFER_URL}/custos_transferencia/tarifas", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.post("/tarifas", summary="Criar tarifa")
async def criar_tarifa(request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    body = await request.json()
    result = await forward_request("POST", f"{COSTS_TRANSFER_URL}/custos_transferencia/tarifas", headers=headers, json=body)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.put("/tarifas/{tipo_veiculo}", summary="Atualizar tarifa")
async def atualizar_tarifa(tipo_veiculo: str, request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    body = await request.json()
    result = await forward_request("PUT", f"{COSTS_TRANSFER_URL}/custos_transferencia/tarifas/{tipo_veiculo}", headers=headers, json=body)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.delete("/tarifas/{tipo_veiculo}", summary="Remover tarifa")
async def remover_tarifa(tipo_veiculo: str, request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"Authorization": auth} if auth else {}
    result = await forward_request("DELETE", f"{COSTS_TRANSFER_URL}/custos_transferencia/tarifas/{tipo_veiculo}", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.get("/artefatos", summary="Links e dados dos artefatos (CSV/JSON/PDF)")
def artefatos_custos_transferencia(
    data: date = Query(..., description="Data de envio (AAAA-MM-DD)"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    base = "exports/costs_transfer"
    tenant_dir = os.path.join(base, tenant_id)

    csv_dir = os.path.join(tenant_dir, "csv")
    json_dir = os.path.join(tenant_dir, "json")
    pdf_dir = os.path.join(tenant_dir, "pdf")

    envio_data = data.isoformat()

    def _latest(patterns: list[str]) -> str | None:
        candidates = []
        for pat in patterns:
            for p in glob.glob(pat):
                try:
                    candidates.append((os.path.getmtime(p), p))
                except Exception:
                    pass
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    csv_path = _latest([os.path.join(csv_dir, f"*{envio_data}*.csv")])
    json_path = _latest([os.path.join(json_dir, f"*{envio_data}*.json")])
    pdf_path = _latest([os.path.join(pdf_dir, f"*{envio_data}*.pdf")])

    def _to_public_url(local_path: str | None) -> str | None:
        if not local_path:
            return None
        rel = os.path.relpath(local_path, base)
        return f"/exports/costs_transfer/{rel}"

    # 🔥 Carrega conteúdo JSON automaticamente
    json_dados = []
    if json_path and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                json_dados = json.load(f)
        except Exception as e:
            logger.error(f"❌ Erro ao ler JSON {json_path}: {e}")

    return {
        "tenant_id": tenant_id,
        "envio_data": envio_data,
        "csv_url": _to_public_url(csv_path),
        "json_url": _to_public_url(json_path),
        "pdf_url": _to_public_url(pdf_path),
        "json_dados": json_dados,   # 👈 já vem direto
    }

