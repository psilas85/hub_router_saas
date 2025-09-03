# last_mile_routing/api/routes.py

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from datetime import date, timedelta, datetime
import os

from last_mile_routing.application.routing_use_case import RoutingUseCase
from last_mile_routing.logs.logging_factory import LoggerFactory
from last_mile_routing.visualization.route_plotter import plotar_rotas
from last_mile_routing.visualization.generate_pdf_report import generate_pdf_report

from authentication.utils.dependencies import get_current_user
from authentication.domain.entities import UsuarioToken

router = APIRouter(prefix="/lastmile", tags=["Roteirização Last Mile"])
logger = LoggerFactory.get_logger("routing")


# =========================================================
# 🚚 Roteirização
# =========================================================
@router.post("/roteirizar", summary="Executar roteirização de entregas Last Mile")
def roteirizar_entregas(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_final: date | None = Query(None, description="Data final (YYYY-MM-DD)"),
    entregas_por_subcluster: int = Query(25, description="Quantidade de entregas por subcluster"),
    tempo_maximo_rota: float = Query(1200.0, description="Tempo máximo de rota em minutos"),
    tempo_parada_leve: float = Query(10.0, description="Tempo de parada para cargas leves (min)"),
    tempo_parada_pesada: float = Query(20.0, description="Tempo de parada para cargas pesadas (min)"),
    tempo_descarga_por_volume: float = Query(0.4, description="Tempo de descarga por volume (min)"),
    peso_leve_max: float = Query(50.0, description="Peso máximo para carga leve (kg)"),
    restricao_veiculo_leve_municipio: bool = Query(False, description="Restringe veículos leves em rotas intermunicipais"),
    modo_forcar: bool = Query(False, description="Força sobrescrita da roteirização existente"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    tenant_id = usuario.tenant_id
    data_final = data_final or data_inicial  # 🔑 se não informado, assume inicial

    if data_final < data_inicial:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial.")

    parametros = {
        "entregas_por_subcluster": entregas_por_subcluster,
        "tempo_maximo_rota": tempo_maximo_rota,
        "tempo_parada_leve": tempo_parada_leve,
        "tempo_parada_pesada": tempo_parada_pesada,
        "tempo_descarga_por_volume": tempo_descarga_por_volume,
        "peso_leve_max": peso_leve_max,
        "restricao_veiculo_leve_municipio": restricao_veiculo_leve_municipio,
        "modo_forcar": modo_forcar
    }

    api_key = os.getenv("GMAPS_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="API Key do Google Maps não encontrada.")

    datas = [data_inicial + timedelta(days=i) for i in range((data_final - data_inicial).days + 1)]

    try:
        for data in datas:
            logger.info(f"📅 Roteirizando {data} para tenant {tenant_id}...")
            roteirizador = RoutingUseCase(tenant_id, parametros, api_key)
            roteirizador.executar(data)

        return {
            "status": "ok",
            "mensagem": f"✅ Roteirização concluída para {len(datas)} dia(s)",
            "datas": [str(d) for d in datas]
        }

    except Exception as e:
        logger.error(f"❌ Erro na roteirização: {e}")
        raise HTTPException(status_code=500, detail="Erro ao executar a roteirização.")


# =========================================================
# 🗺️ Visualização
# =========================================================
@router.get("/visualizar", summary="Visualizar rotas e gerar relatórios PDF")
def visualizar_rotas_lastmile(
    data_inicial: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_final: date | None = Query(None, description="Data final (YYYY-MM-DD)"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    tenant_id = usuario.tenant_id
    data_final = data_final or data_inicial  # 🔑 se não informado, assume inicial

    if data_final < data_inicial:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial.")

    datas = [data_inicial + timedelta(days=i) for i in range((data_final - data_inicial).days + 1)]

    try:
        for data in datas:
            logger.info(f"🗺️ Gerando visualização de rotas para {data}")
            plotar_rotas(tenant_id, data)
            generate_pdf_report(tenant_id, data)

        return {
            "status": "ok",
            "mensagem": f"✅ Visualizações geradas para {len(datas)} dia(s)",
            "datas": [str(d) for d in datas]
        }

    except Exception as e:
        logger.error(f"❌ Erro na visualização: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao gerar visualização: {str(e)}")
