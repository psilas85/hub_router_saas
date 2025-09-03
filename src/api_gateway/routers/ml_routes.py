#api_gateway/routers/ml_routes.py

from fastapi import APIRouter, Depends, Request
from authentication.utils.dependencies import obter_tenant_id_do_token
from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings
from api_gateway.schemas import TrainRequest, PredictRequest

router = APIRouter(prefix="/ml", tags=["Machine Learning"])

# URL interna do serviço ML Pipeline
ML_URL = settings.ML_URL


def _auth_headers(request: Request) -> dict:
    """
    Captura o header 'authorization' (minúsculo na FastAPI)
    e repassa como 'Authorization' (maiúsculo), que é o padrão
    esperado pelo validador de JWT nos microserviços.
    """
    auth = request.headers.get("authorization")
    return {"Authorization": auth} if auth else {}


@router.post("/train", summary="Treinar modelo ML")
async def train(
    request: Request,
    body: TrainRequest,   # ✅ schema sem tenant_id
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    """
    Encaminha requisição de treino para o serviço ml_pipeline.
    """
    headers = _auth_headers(request)

    payload = body.dict()
    payload["tenant_id"] = tenant_id  # ✅ injeta tenant_id do token

    return await forward_request(
        "POST",
        f"{ML_URL}/train",
        headers=headers,
        json=payload,
    )


@router.post("/predict", summary="Executar previsão ML")
async def predict(
    request: Request,
    body: PredictRequest,   # ✅ schema sem tenant_id
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    """
    Encaminha requisição de predição para o serviço ml_pipeline.
    """
    headers = _auth_headers(request)

    payload = body.dict()
    payload["tenant_id"] = tenant_id  # garante tenant_id do token

    # 🔑 Normaliza: se vier só um objeto, transforma em lista
    if isinstance(payload["features"], dict):
        payload["features"] = [payload["features"]]

    return await forward_request(
        "POST",
        f"{ML_URL}/predict",
        headers=headers,
        json=payload,
    )

@router.post("/train_compare", summary="Comparar algoritmos de treino ML")
async def train_compare(
    request: Request,
    body: TrainRequest,
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    headers = _auth_headers(request)
    payload = body.dict()
    payload["tenant_id"] = tenant_id
    return await forward_request(
        "POST",
        f"{ML_URL}/train_compare",
        headers=headers,
        json=payload,
    )


@router.get("/plan", summary="Planejamento ML (próximos meses)")
async def plan(
    request: Request,
    start_date: str,
    months: int = 3,
    scenarios: str = "base,baixo,alto",
    debug: bool = False,
    fast: bool = False,
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    headers = _auth_headers(request)
    params = {
        "start_date": start_date,
        "months": months,
        "scenarios": scenarios,
        "debug": str(debug).lower(),
        "fast": str(fast).lower(),
    }
    return await forward_request(
        "GET",
        f"{ML_URL}/plan",
        headers=headers,
        params=params,
    )
