# src/costs_transfer/api/routes_tarifas.py
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import List, Any, Dict
import logging
import math

from authentication.utils.dependencies import obter_tenant_id_do_token
from costs_transfer.infrastructure.transfer_cost_repository import TransferCostRepository

logger = logging.getLogger("costs_transfer")

router = APIRouter(
    prefix="/custos_transferencia",
    tags=["Tarifas Transferência"]
)

repo = TransferCostRepository()

class TarifaIn(BaseModel):
    tipo_veiculo: str = Field(..., min_length=1)
    custo_km: float = Field(..., ge=0)
    capacidade_min: float = Field(..., ge=0)
    capacidade_max: float = Field(..., gt=0)

class TarifaOut(TarifaIn):
    id: int | None = None  # caso a query retorne id

# mapeia nomes vindos do banco -> nomes da API
KEYMAP = {
    "custo_por_km": "custo_km",
    "capacidade_kg_min": "capacidade_min",
    "capacidade_kg_max": "capacidade_max",
}

def _to_python(v: Any):
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return None
        if isinstance(v, (int, float, str, bool)):
            return v
        try:
            return float(v)
        except Exception:
            return str(v)
    except Exception:
        return None

def _normalize_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    # renomeia chaves do banco para as chaves da API
    out = dict(rec)
    for src, dst in KEYMAP.items():
        if src in out and dst not in out:
            out[dst] = out.pop(src)
    # garante chaves exigidas pelo response_model
    out.setdefault("custo_km", None)
    out.setdefault("capacidade_min", None)
    out.setdefault("capacidade_max", None)
    # saneia tipos
    for k, v in list(out.items()):
        out[k] = _to_python(v)
    return out

@router.get("/tarifas", response_model=List[TarifaOut], summary="Listar tarifas por tenant")
def listar_tarifas(tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        df = repo.listar_custos_transferencia(tenant_id)
        logger.info(f"[tarifas][{tenant_id}] linhas={len(df.index)}")
        records = df.to_dict(orient="records")
        return [_normalize_record(r) for r in records]
    except Exception as e:
        logger.exception(f"Erro ao listar tarifas para tenant={tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao listar tarifas: {e}")

@router.post("/tarifas", response_model=TarifaOut, summary="Criar tarifa")
def criar_tarifa(body: TarifaIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        repo.adicionar_custo_transferencia(
            body.tipo_veiculo, body.custo_km, body.capacidade_min, body.capacidade_max, tenant_id
        )
        df = repo.listar_custos_transferencia(tenant_id)
        records = df.to_dict(orient="records")
        # pega o último registro desse tipo
        items = [r for r in records if r.get("tipo_veiculo") == body.tipo_veiculo]
        if not items:
            raise HTTPException(status_code=500, detail="Falha ao criar tarifa")
        return _normalize_record(items[-1])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Erro ao criar tarifa tenant={tenant_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao criar tarifa: {e}")

@router.put("/tarifas/{tipo_veiculo}", response_model=TarifaOut, summary="Atualizar tarifa")
def atualizar_tarifa(tipo_veiculo: str, body: TarifaIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        repo.editar_custo_transferencia(
            tipo_veiculo, body.custo_km, body.capacidade_min, body.capacidade_max, tenant_id
        )
        df = repo.listar_custos_transferencia(tenant_id)
        records = df.to_dict(orient="records")
        items = [r for r in records if r.get("tipo_veiculo") == tipo_veiculo]
        if not items:
            raise HTTPException(status_code=404, detail="Tarifa não encontrada")
        return _normalize_record(items[-1])
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Erro ao atualizar tarifa tenant={tenant_id}, tipo={tipo_veiculo}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar tarifa: {e}")

@router.delete("/tarifas/{tipo_veiculo}", summary="Remover tarifa")
def remover_tarifa(tipo_veiculo: str, tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        repo.remover_custo_transferencia(tipo_veiculo, tenant_id)
        return {"detail": "Removido"}
    except Exception as e:
        logger.exception(f"Erro ao remover tarifa tenant={tenant_id}, tipo={tipo_veiculo}: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao remover tarifa: {e}")
