# src/costs_last_mile/api/routes.py
from fastapi import APIRouter, Depends, Query, HTTPException
from datetime import date
import os
from typing import List
from pydantic import BaseModel

from costs_last_mile.domain.cost_service_last_mile import CostService
from costs_last_mile.visualization.logging_factory import get_logger
from costs_last_mile.visualization.gerar_detalhes_last_mile import gerar_detalhes_last_mile
from costs_last_mile.visualization.gerar_resumo_last_mile import gerar_resumo_last_mile
from costs_last_mile.visualization.gerar_relatorio_last_mile import gerar_relatorio_last_mile
from authentication.utils.dependencies import obter_tenant_id_do_token
from costs_last_mile.infrastructure.cost_repository_last_mile import CostRepository


router = APIRouter(prefix="/costs_last_mile", tags=["Costs Last Mile"])
logger = get_logger("costs_last_mile")


@router.post("/", summary="Calcular custos Last Mile")
def calcular_custos_last_mile(
    data_inicial: date = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    data_final: date | None = Query(None, description="Data final no formato YYYY-MM-DD (opcional)"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    if not data_final:
        data_final = data_inicial
    if data_final < data_inicial:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial")

    try:
        service = CostService(tenant_id=tenant_id)
        service.processar_custos(data_inicial, data_final, modo_forcar=True)  # 🔒 força processamento
        return {
            "status": "ok",
            "mensagem": f"✅ Cálculo de custos de last-mile concluído de {data_inicial} até {data_final}",
            "datas": [str(data_inicial), str(data_final)]
        }
    except Exception as e:
        logger.error(f"❌ Erro no cálculo de custos last-mile: {e}")
        raise HTTPException(status_code=500, detail="Erro ao calcular custos de last-mile.")


@router.get("/visualizar", summary="Visualizar relatórios de custos Last Mile")
def visualizar_relatorios_last_mile(
    data: date = Query(..., description="Data de envio (YYYY-MM-DD)"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    logger.info(f"🔍 Visualizando relatórios de custos para tenant '{tenant_id}' na data '{data}'")
    try:
        # 🔹 Gera DataFrames (detalhe + resumo)
        df_detalhes = gerar_detalhes_last_mile(tenant_id, data)
        df_resumo = gerar_resumo_last_mile(tenant_id, data)

        if df_detalhes.empty or df_resumo.empty:
            return {"mensagem": "Nenhum dado encontrado.", "json_dados": []}

        # 🔹 Caminhos de saída
        output_base = "exports/costs_last_mile"
        detalhes_dir = os.path.join(output_base, "detalhes", tenant_id)
        resumos_dir = os.path.join(output_base, "resumos", tenant_id)
        relatorios_dir = os.path.join(output_base, "relatorios", tenant_id)
        os.makedirs(detalhes_dir, exist_ok=True)
        os.makedirs(resumos_dir, exist_ok=True)
        os.makedirs(relatorios_dir, exist_ok=True)

        # 🔹 Arquivos
        detalhes_file = os.path.join(detalhes_dir, f"detalhes_last_mile_{data}.csv")
        resumo_file = os.path.join(resumos_dir, f"resumo_last_mile_{data}.csv")
        pdf_file = os.path.join(relatorios_dir, f"costs_last_mile_{data}.pdf")

        # 🔹 Exportar CSVs (sempre sobrescreve)
        df_detalhes.to_csv(detalhes_file, index=False, encoding="utf-8-sig")
        df_resumo.to_csv(resumo_file, index=False, encoding="utf-8-sig")

        # 🔹 Gerar PDF (sempre sobrescreve)
        gerar_relatorio_last_mile(
            envio_data=str(data),
            tenant_id=tenant_id,
            df_detalhes=df_detalhes,
            df_resumo=df_resumo,
            modo_forcar=True
        )

        # 🔹 Retorno para o frontend
        return {
            "tenant_id": tenant_id,
            "envio_data": str(data),
            "detalhes_url": f"/exports/costs_last_mile/detalhes/{tenant_id}/detalhes_last_mile_{data}.csv",
            "resumo_url": f"/exports/costs_last_mile/resumos/{tenant_id}/resumo_last_mile_{data}.csv",
            "pdf_url": f"/exports/costs_last_mile/relatorios/{tenant_id}/costs_last_mile_{data}.pdf",
            # 👇 Agora a tela recebe o MESMO detalhamento do PDF
            "json_dados": df_detalhes.to_dict(orient="records"),
        }
    except Exception as e:
        logger.error(f"❌ Erro na visualização de custos last mile: {e}")
        raise HTTPException(status_code=500, detail="Erro ao gerar visualizações de custos.")


# --- MODELS ---
class VehicleCostIn(BaseModel):
    veiculo: str
    peso_minimo_kg: float
    peso_maximo_kg: float
    custo_por_km: float
    custo_por_entrega: float


class VehicleCostOut(VehicleCostIn):
    id: int | None = None   # se houver PK na tabela


# --- REPOSITORY ---
repo = CostRepository()


# --- CRUD ---
@router.get("/vehicles", response_model=List[VehicleCostOut], summary="Listar veículos")
def listar_veiculos(tenant_id: str = Depends(obter_tenant_id_do_token)):
    df = repo.buscar_custos_veiculo(tenant_id)
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.post("/vehicles", response_model=VehicleCostOut, summary="Adicionar veículo")
def adicionar_veiculo(body: VehicleCostIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        repo.adicionar_custo_veiculo(
            body.veiculo, body.custo_por_km, body.custo_por_entrega,
            body.peso_minimo_kg, body.peso_maximo_kg, tenant_id
        )
        return body
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao adicionar veículo: {e}")


@router.put("/vehicles/{veiculo}", response_model=VehicleCostOut, summary="Editar veículo")
def editar_veiculo(veiculo: str, body: VehicleCostIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        repo.editar_custo_veiculo(
            veiculo, body.custo_por_km, body.custo_por_entrega,
            body.peso_minimo_kg, body.peso_maximo_kg, tenant_id
        )
        return body
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao editar veículo: {e}")


@router.delete("/vehicles/{veiculo}", summary="Remover veículo")
def remover_veiculo(veiculo: str, tenant_id: str = Depends(obter_tenant_id_do_token)):
    try:
        repo.remover_custo_veiculo(veiculo, tenant_id)
        return {"status": "ok", "mensagem": f"✅ Veículo '{veiculo}' removido com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao remover veículo: {e}")
