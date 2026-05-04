# api_gateway/routers/exploratory_analysis_routes.py

from fastapi import APIRouter, HTTPException, Request
from authentication.utils.dependencies import obter_tenant_id_do_token
from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings

router = APIRouter(prefix="/exploratory", tags=["Análise Exploratória"])

EXPLORATORY_URL = settings.EXPLORATORY_ANALYSIS_URL


@router.get("/health", summary="Healthcheck Exploratory Analysis")
async def healthcheck(request: Request):
    headers = {"authorization": request.headers.get("authorization", "")}
    result = await forward_request("GET", f"{EXPLORATORY_URL}/health", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.get("/eda/{analysis_name}", summary="Executar análise exploratória")
async def get_eda(
    analysis_name: str,
    request: Request,
):
    # Auth is enforced inside the downstream service via obter_tenant_id_do_token
    headers = {"authorization": request.headers.get("authorization", "")}
    params = dict(request.query_params)

    result = await forward_request(
        "GET",
        f"{EXPLORATORY_URL}/eda/{analysis_name}",
        headers=headers,
        params=params,
    )

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    return result["content"]
