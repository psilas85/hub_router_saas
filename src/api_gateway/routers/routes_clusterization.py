# api_gateway/api/routes/clusterization_routes.py

import os
import logging
from fastapi import Request, APIRouter, Depends, Query, HTTPException
from typing import Optional

from authentication.utils.dependencies import obter_tenant_id_do_token
from api_gateway.utils.http_client import forward_request

router = APIRouter(prefix="/clusterization", tags=["Clusterization"])

CLUSTERIZATION_URL = os.getenv("CLUSTERIZATION_SERVICE_URL", "http://clusterization_service:8001")
logger = logging.getLogger("api_gateway.clusterization")


@router.get("/health", summary="Healthcheck Clusterization")
async def healthcheck(request: Request):
    headers = {}
    auth = request.headers.get("authorization") or request.headers.get("Authorization")
    if auth:
        headers["Authorization"] = auth

    result = await forward_request("GET", f"{CLUSTERIZATION_URL}/", headers=headers)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.post("/processar", summary="Executar clusterização")
async def processar_clusterizacao(
    request: Request,
    data: str = Query(..., description="Data no formato YYYY-MM-DD"),
    data_final: Optional[str] = Query(None, description="Data final (opcional) no formato YYYY-MM-DD"),
    k_min: int = Query(2, description="Número mínimo de clusters"),
    k_max: int = Query(50, description="Número máximo de clusters"),
    min_entregas_por_cluster: int = Query(25, description="Mínimo de entregas por cluster"),
    fundir_clusters_pequenos: bool = Query(False, description="Fundir clusters pequenos"),
    desativar_cluster_hub_central: bool = Query(False, description="Desativar cluster do hub central"),
    raio_cluster_hub_central: float = Query(80.0, description="Raio em km para cluster do hub central"),
    modo_forcar: bool = Query(False, description="Forçar reprocessamento"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    url = f"{CLUSTERIZATION_URL}/cluster/clusterizar"
    params = {
        "data": data,
        "k_min": k_min,
        "k_max": k_max,
        "min_entregas_por_cluster": min_entregas_por_cluster,
        "fundir_clusters_pequenos": str(fundir_clusters_pequenos).lower(),
        "desativar_cluster_hub_central": str(desativar_cluster_hub_central).lower(),
        "raio_cluster_hub_central": raio_cluster_hub_central,
        "modo_forcar": str(modo_forcar).lower(),
    }

    # só envia se vier
    if data_final:
        params["data_final"] = data_final

    headers = {"Authorization": request.headers.get("Authorization")}
    result = await forward_request("POST", url, headers=headers, params=params)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.get("/visualizar", summary="Visualizar clusterização")
async def visualizar_clusterizacao(
    request: Request,
    data: str = Query(..., description="Data do envio no formato YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    url = f"{CLUSTERIZATION_URL}/cluster/clusterizar/visualizacao"
    params = {"data": data}
    headers = {"Authorization": request.headers.get("Authorization")}

    result = await forward_request("GET", url, headers=headers, params=params)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    return result["content"]

