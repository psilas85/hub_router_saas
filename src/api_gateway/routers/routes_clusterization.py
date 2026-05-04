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


@router.get("/datas-disponiveis", summary="Listar datas com entregas disponíveis")
async def listar_datas_disponiveis(
    request: Request,
    limit: int = Query(30, ge=1, le=365, description="Quantidade máxima de datas retornadas"),
    offset: int = Query(0, ge=0, description="Quantidade de datas ignoradas para paginação"),
    data_inicio: Optional[str] = Query(None, description="Filtrar datas a partir de YYYY-MM-DD"),
    data_fim: Optional[str] = Query(None, description="Filtrar datas até YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    url = f"{CLUSTERIZATION_URL}/cluster/datas-disponiveis"
    params = {"limit": limit, "offset": offset}
    if data_inicio:
        params["data_inicio"] = data_inicio
    if data_fim:
        params["data_fim"] = data_fim
    headers = {"Authorization": request.headers.get("Authorization")}

    result = await forward_request("GET", url, headers=headers, params=params)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.get("/hubs-cadastro", summary="Listar hubs de clusterização")
async def listar_hubs_clusterization(
    request: Request,
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    headers = {"Authorization": request.headers.get("Authorization")}
    result = await forward_request("GET", f"{CLUSTERIZATION_URL}/cluster/hubs", headers=headers)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.post("/hubs-cadastro", summary="Criar hub de clusterização")
async def criar_hub_clusterization(
    request: Request,
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    body = await request.json()
    headers = {"Authorization": request.headers.get("Authorization")}
    result = await forward_request("POST", f"{CLUSTERIZATION_URL}/cluster/hubs", headers=headers, json=body)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.put("/hubs-cadastro/{hub_id}", summary="Atualizar hub de clusterização")
async def atualizar_hub_clusterization(
    hub_id: int,
    request: Request,
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    body = await request.json()
    headers = {"Authorization": request.headers.get("Authorization")}
    result = await forward_request("PUT", f"{CLUSTERIZATION_URL}/cluster/hubs/{hub_id}", headers=headers, json=body)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.delete("/hubs-cadastro/{hub_id}", summary="Excluir hub de clusterização")
async def excluir_hub_clusterization(
    hub_id: int,
    request: Request,
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    headers = {"Authorization": request.headers.get("Authorization")}
    result = await forward_request("DELETE", f"{CLUSTERIZATION_URL}/cluster/hubs/{hub_id}", headers=headers)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.post("/jobs", summary="Executar clusterização assíncrona")
async def criar_job_clusterizacao(
    request: Request,
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    body = await request.json()
    headers = {"Authorization": request.headers.get("Authorization")}
    result = await forward_request("POST", f"{CLUSTERIZATION_URL}/cluster/jobs", headers=headers, json=body)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.get("/jobs/{job_id}", summary="Status da clusterização assíncrona")
async def status_job_clusterizacao(
    job_id: str,
    request: Request,
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    headers = {"Authorization": request.headers.get("Authorization")}
    result = await forward_request("GET", f"{CLUSTERIZATION_URL}/cluster/jobs/{job_id}", headers=headers)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.post("/processar", summary="Executar clusterização")
async def processar_clusterizacao(
    request: Request,
    data: str = Query(..., description="Data no formato YYYY-MM-DD"),
    data_final: Optional[str] = Query(None, description="Data final (opcional) no formato YYYY-MM-DD"),
    min_entregas_por_cluster_alvo: int = Query(10, description="Mínimo alvo de entregas por cluster"),
    max_entregas_por_cluster_alvo: int = Query(100, description="Máximo alvo de entregas por cluster"),
    min_entregas_por_cluster: Optional[int] = Query(
        None,
        description="Alias legado para min_entregas_por_cluster_alvo",
        deprecated=True,
    ),
    k_min: Optional[int] = Query(None, description="Legado: ignorado no cálculo atual", deprecated=True),
    k_max: Optional[int] = Query(None, description="Legado: ignorado no cálculo atual", deprecated=True),
    fundir_clusters_pequenos: bool = Query(
        False,
        description="Legado: ignorado. O balanceamento por min/max ja funde clusters pequenos.",
        deprecated=True,
    ),
    hub_central_id: int = Query(..., description="ID do Hub Central selecionado para a clusterização"),
    desativar_cluster_hub_central: bool = Query(
        False,
        description="Legado: ignorado. Hub Central agora é obrigatório.",
        deprecated=True,
    ),
    raio_cluster_hub_central: float = Query(80.0, description="Raio em km para cluster do hub central"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    url = f"{CLUSTERIZATION_URL}/cluster/clusterizar"
    min_cluster_alvo = (
        min_entregas_por_cluster
        if min_entregas_por_cluster is not None
        else min_entregas_por_cluster_alvo
    )
    params = {
        "data": data,
        "min_entregas_por_cluster_alvo": min_cluster_alvo,
        "max_entregas_por_cluster_alvo": max_entregas_por_cluster_alvo,
        "hub_central_id": hub_central_id,
        "raio_cluster_hub_central": raio_cluster_hub_central,
        # 🚫 não aceitar mais 'modo_forcar' do usuário
    }

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
