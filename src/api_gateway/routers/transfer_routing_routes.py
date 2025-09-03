# src/api_gateway/routers/transfer_routing_routes.py

from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import Response
from datetime import date
from authentication.utils.dependencies import obter_tenant_id_do_token
from api_gateway.utils.http_client import forward_request

router = APIRouter(prefix="/transfer_routing", tags=["Transfer Routing"])

TRANSFER_ROUTING_URL = "http://transfer_routing_service:8003"


@router.get("/health", summary="Healthcheck Transfer Routing")
async def healthcheck_transfer(request: Request):
    """Healthcheck repassado ao serviço interno"""
    auth = request.headers.get("authorization")
    headers = {"authorization": auth} if auth else {}
    return await forward_request("GET", f"{TRANSFER_ROUTING_URL}/health", headers=headers)


@router.post("/processar", summary="Processar Transfer Routing")
async def processar_transfer_routing(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial"),
    modo_forcar: bool = Query(False, description="Forçar reprocessamento"),
    tempo_maximo: float = Query(1200.0, description="Tempo máximo da rota (minutos)"),
    tempo_parada_leve: float = Query(10.0, description="Tempo de parada leve"),
    peso_leve_max: float = Query(50.0, description="Peso máximo para considerar parada leve"),
    tempo_parada_pesada: float = Query(20.0, description="Tempo de parada pesada"),
    tempo_por_volume: float = Query(0.4, description="Tempo de descarregamento por volume"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    """Encaminha POST para /transferencias do serviço interno"""
    headers = {"authorization": request.headers.get("authorization")}
    params = {
        "envio_data": data_inicial.isoformat(),
        "modo_forcar": modo_forcar,
        "tempo_maximo": tempo_maximo,
        "tempo_parada_leve": tempo_parada_leve,
        "peso_leve_max": peso_leve_max,
        "tempo_parada_pesada": tempo_parada_pesada,
        "tempo_por_volume": tempo_por_volume,
    }
    return await forward_request("POST", f"{TRANSFER_ROUTING_URL}/transferencias", headers=headers, params=params)


@router.get("/visualizacao", summary="Gerar relatório de transferências (PDF)")
async def visualizar_transferencias(
    request: Request,
    data_inicial: str = Query(...),
    data_final: str = Query(None),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    url = f"{TRANSFER_ROUTING_URL}/transferencias/visualizacao"
    params = {"data_inicial": data_inicial}
    if data_final:
        params["data_final"] = data_final

    headers = {
        "authorization": request.headers.get("authorization") or request.headers.get("Authorization")
    }


    result = await forward_request("GET", url, headers=headers, params=params)

    if result["headers"].get("content-type", "").lower().startswith("application/pdf"):
        return Response(
            content=result["content"],
            media_type="application/pdf",
            headers={
                "Content-Disposition": f'attachment; filename="relatorio_transferencias_{data_inicial}.pdf"'
            }
        )

    return result["content"]


@router.get("/artefatos", summary="Links públicos dos artefatos (HTML/PNG/PDF)")
async def artefatos_transferencias(
    request: Request,
    data_inicial: str = Query(..., description="YYYY-MM-DD"),
    data_final: str | None = Query(None, description="YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    url = f"{TRANSFER_ROUTING_URL}/transferencias/artefatos"
    params = {"data_inicial": data_inicial}
    if data_final:
        params["data_final"] = data_final

    # header pode vir "Authorization" ou "authorization" – normalize
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    headers = {"authorization": auth} if auth else {}

    result = await forward_request("GET", url, headers=headers, params=params)
    return result["content"]
