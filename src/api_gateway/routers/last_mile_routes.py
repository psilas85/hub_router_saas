# src/api_gateway/routers/last_mile_routes.py

import os
from fastapi import APIRouter, Request, Query, Depends, HTTPException, Response
from datetime import date
from authentication.utils.dependencies import obter_tenant_id_do_token
from authentication.domain.entities import UsuarioToken
from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings

router = APIRouter(prefix="/last_mile_routing", tags=["Last Mile Routing"])

LAST_MILE_URL = settings.LAST_MILE_URL

# Caminhos de saída (✅ corrigido para /app/output)
OUTPUT_BASE = "/app/exports"
PUBLIC_BASE = "/exports"


@router.get("/health", summary="Healthcheck Last Mile Routing")
async def healthcheck(request: Request):
    auth = request.headers.get("authorization")
    headers = {"authorization": auth} if auth else {}
    return await forward_request("GET", f"{LAST_MILE_URL}/", headers=headers)


# =========================================================
# 🚚 Roteirização
# =========================================================
@router.post("/roteirizar", summary="Executar roteirização de entregas Last Mile")
async def roteirizar(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_final: date | None = Query(None, description="Data final (YYYY-MM-DD, opcional)"),
    entregas_por_subcluster: int = Query(25, description="Número de entregas por subcluster"),
    tempo_maximo_rota: float = Query(1200.0, description="Tempo máximo de rota (min)"),
    tempo_parada_leve: float = Query(10.0, description="Tempo de parada para cargas leves (min)"),
    tempo_parada_pesada: float = Query(20.0, description="Tempo de parada para cargas pesadas (min)"),
    tempo_descarga_por_volume: float = Query(0.4, description="Tempo de descarga por volume (min)"),
    peso_leve_max: float = Query(50.0, description="Peso máximo para carga leve (kg)"),
    restricao_veiculo_leve_municipio: bool = Query(False, description="Restringe veículos leves em rotas intermunicipais"),
    modo_forcar: bool = Query(False, description="Sobrescrever roteirização existente"),
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    params = {
        "data_inicial": data_inicial,
        "data_final": data_final or data_inicial,
        "entregas_por_subcluster": entregas_por_subcluster,
        "tempo_maximo_rota": tempo_maximo_rota,
        "tempo_parada_leve": tempo_parada_leve,
        "tempo_parada_pesada": tempo_parada_pesada,
        "tempo_descarga_por_volume": tempo_descarga_por_volume,
        "peso_leve_max": peso_leve_max,
        "restricao_veiculo_leve_municipio": restricao_veiculo_leve_municipio,
        "modo_forcar": modo_forcar,
    }

    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("POST", f"{LAST_MILE_URL}/lastmile/roteirizar", headers=headers, params=params)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    return result["content"]


# =========================================================
# 🗺️ Visualização
# =========================================================
@router.get("/visualizar", summary="Visualizar rotas e relatórios")
async def visualizar(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_final: date | None = Query(None, description="Data final (YYYY-MM-DD, opcional)"),
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    params = {"data_inicial": data_inicial, "data_final": data_final or data_inicial}
    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request("GET", f"{LAST_MILE_URL}/lastmile/visualizar", headers=headers, params=params)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    return result["content"]


# =========================================================
# 📂 Artefatos (HTML/PDF)
# =========================================================
@router.get("/artefatos", summary="Listar artefatos gerados para Last Mile")
async def listar_artefatos(
    data_inicial: str = Query(..., description="Data no formato YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    data = data_inicial
    artefatos = []

    maps_dir = os.path.join(OUTPUT_BASE, "last_mile_routing", "maps", tenant_id)
    rel_dir  = os.path.join(OUTPUT_BASE, "last_mile_routing", "relatorios", tenant_id)

    map_html = os.path.join(maps_dir, f"mapa_rotas_{data}.html")
    pdf_file = os.path.join(rel_dir,  f"relatorio_last_mile_{data}.pdf")

    if os.path.isfile(map_html) and os.path.isfile(pdf_file):
        artefatos.append({
            "data": data,
            "map_html_url": f"{PUBLIC_BASE}/last_mile_routing/maps/{tenant_id}/mapa_rotas_{data}.html",
            "pdf_url":      f"{PUBLIC_BASE}/last_mile_routing/relatorios/{tenant_id}/relatorio_last_mile_{data}.pdf",
        })

    # ✅ Sempre retorne um JSON com a lista (vazia ou não)
    return {"status": "ok", "artefatos": artefatos}