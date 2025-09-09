# hub_router_1.0.1/src/machine_learning/api/routes.py
from __future__ import annotations

from typing import List, Optional, Any, Dict
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
import pandas as pd

# 🔐 Auth
from authentication.utils.dependencies import get_current_user
from authentication.domain.entities import UsuarioToken

# 📈 Demand Forecast (existente)
from machine_learning.demand.application.demand_use_case import DemandUseCase
from machine_learning.demand.infrastructure.demand_repository import DemandRepository

# 🔷 K-Cluster (NOVO)
from machine_learning.k_cluster.application.k_cluster_use_case import KClusterUseCase
from machine_learning.k_cluster.infrastructure.repositories import PgResultsRepository
from machine_learning.k_cluster.infrastructure.model_store import LocalKModelStore

router = APIRouter(prefix="/ml", tags=["Machine Learning"])

# =========================================================
# 📈 Previsão de Demanda (existente)
# =========================================================
@router.get("/forecast", summary="Gerar previsão de demanda por cidade", tags=["Demand"])
def forecast_demand(
    cidade: str = Query(..., description="Cidade para previsão"),
    horizon: int = Query(14, description="Horizonte de previsão em dias"),
    usuario: UsuarioToken = Depends(get_current_user),  # 🔑 tenant do JWT
):
    tenant_id = usuario.tenant_id
    try:
        repo = DemandRepository()
        use_case = DemandUseCase(repo)

        forecast = use_case.forecast_demand(
            tenant_id=tenant_id,
            start_date="2024-01-01",   # TODO: parametrizar se desejar
            end_date="2025-12-31",
            cidade=cidade,
            horizon=horizon,
        )

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "cidade": cidade,
            "horizon": horizon,
            "forecast": forecast.to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no forecast: {str(e)}")


# =========================================================
# 🔷 K-Cluster — Treino & Predição (NOVO)
# =========================================================
class KTrainBody(BaseModel):
    """Treinar o modelo de k ótimo — tenant vem do JWT."""
    start: Optional[str] = Field(default=None, description="YYYY-MM-DD (opcional)")
    end: Optional[str] = Field(default=None, description="YYYY-MM-DD (opcional)")
    test_size: float = Field(default=0.2, ge=0.05, le=0.5)


class DemandRow(BaseModel):
    cidade: str
    quantidade_entregas: Optional[float] = 0
    cte_peso: Optional[float] = 0
    cte_volumes: Optional[float] = 0
    envio_data: Optional[str] = None  # opcional


class KPredictBody(BaseModel):
    envio_data: str
    demand_rows: List[DemandRow] = Field(default_factory=list)
    min_k: int = Field(default=2, ge=1)
    max_k: int = Field(default=12, ge=1)
    top_n: int = Field(default=3, ge=1)


def _kcluster_use_case() -> KClusterUseCase:
    """Factory simples do caso de uso de k-cluster."""
    return KClusterUseCase(
        repo=PgResultsRepository(),
        model_store=LocalKModelStore(),  # usa /app/exports/machine_learning/models/k_cluster por padrão
    )


@router.post("/k_cluster/train", summary="Treinar modelo de k ótimo", tags=["K-Cluster"])
def k_cluster_train(
    body: KTrainBody,
    usuario: UsuarioToken = Depends(get_current_user),
):
    tenant_id = usuario.tenant_id
    try:
        uc = _kcluster_use_case()
        out = uc.train(
            tenant_id=tenant_id,
            start=body.start,
            end=body.end,
            test_size=body.test_size,
        )
        return {
            "status": "ok",
            "tenant_id": tenant_id,
            **out,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no treino de k: {e}")


@router.post("/k_cluster/predict", summary="Prever k ótimo para uma demanda prevista", tags=["K-Cluster"])
def k_cluster_predict(
    body: KPredictBody,
    usuario: UsuarioToken = Depends(get_current_user),
):
    if body.min_k > body.max_k:
        raise HTTPException(status_code=400, detail="min_k não pode ser maior que max_k")

    tenant_id = usuario.tenant_id
    try:
        uc = _kcluster_use_case()

        # monta DataFrame da demanda prevista
        df = pd.DataFrame([r.model_dump() for r in body.demand_rows]) if body.demand_rows else pd.DataFrame(
            columns=["cidade", "quantidade_entregas", "cte_peso", "cte_volumes", "envio_data"]
        )

        out = uc.predict(
            tenant_id=tenant_id,
            envio_data=body.envio_data,
            demand_forecast=df,
            candidate_ks=list(range(body.min_k, body.max_k + 1)),
            top_n=body.top_n,
        )
        return {
            "status": "ok",
            "tenant_id": tenant_id,
            **out,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na predição de k: {e}")
