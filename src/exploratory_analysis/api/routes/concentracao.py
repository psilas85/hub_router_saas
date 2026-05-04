# exploratory_analysis/api/routes/concentracao.py

from fastapi import APIRouter, Query

from exploratory_analysis.api.deps import CacheLayer, TenantId
from exploratory_analysis.domain import concentracao as domain
from exploratory_analysis.infrastructure.database_reader import carregar_entregas

router = APIRouter()

TTL = 3600


@router.get("/eda/concentracao")
def get_concentracao(
    tenant_id: TenantId,
    data_inicial: str = Query(...),
    data_final: str = Query(...),
    granularidade: str = Query("mensal"),
):
    cache = CacheLayer(tenant_id, data_inicial, data_final, granularidade)
    cached = cache.get("concentracao")
    if cached:
        return cached

    df = carregar_entregas(data_inicial, data_final, tenant_id)
    result = domain.calcular(df)
    cache.set("concentracao", result, TTL)
    return result
