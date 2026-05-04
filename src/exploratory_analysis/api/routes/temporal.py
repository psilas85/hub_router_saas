# exploratory_analysis/api/routes/temporal.py

from fastapi import APIRouter, Query

from exploratory_analysis.api.deps import CacheLayer, TenantId
from exploratory_analysis.domain import temporal as domain
from exploratory_analysis.infrastructure.database_reader import carregar_entregas

router = APIRouter()

TTL = 3600


@router.get("/eda/temporal")
def get_temporal(
    tenant_id: TenantId,
    data_inicial: str = Query(...),
    data_final: str = Query(...),
    granularidade: str = Query("mensal"),
):
    cache = CacheLayer(tenant_id, data_inicial, data_final, granularidade)
    cached = cache.get("temporal")
    if cached:
        return cached

    df = carregar_entregas(data_inicial, data_final, tenant_id)
    result = domain.calcular(df, granularidade)
    cache.set("temporal", result, TTL)
    return result
