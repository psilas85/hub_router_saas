# costs_transfer/api/routes.py
from fastapi import APIRouter, Query, Depends, HTTPException
from datetime import date
import os, logging, json

from authentication.utils.dependencies import obter_tenant_id_do_token
from costs_transfer.domain.transfer_service import TransferCostService
from costs_transfer.visualization.gerar_relatorio_transferencias import gerar_relatorio_transferencias
from costs_transfer.visualization.main_visualization import carregar_detalhes_transferencias, salvar_csv, salvar_json

router = APIRouter(prefix="/custos_transferencia", tags=["Custos Transferência"])
logger = logging.getLogger("costs_transfer")


@router.post("/", summary="Calcular custos de transferência")
def calcular_custos_transferencia(
    data_inicial: date = Query(...),
    data_final: date = Query(None),
    modo_forcar: bool = Query(False),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    if not data_final:
        data_final = data_inicial
    if data_final < data_inicial:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à inicial.")

    try:
        service = TransferCostService(tenant_id=tenant_id)
        service.processar_custos(
            data_inicial=data_inicial, data_final=data_final, modo_forcar=modo_forcar
        )
        return {
            "status": "ok",
            "mensagem": f"✅ Custos de transferência processados para {data_inicial} → {data_final}"
        }
    except Exception as e:
        logger.error(f"❌ Erro ao calcular custos: {e}")
        raise HTTPException(status_code=500, detail="Erro durante o processamento.")


@router.get("/visualizar", summary="Gerar, salvar e retornar custos de transferência")
def visualizar_custos_transferencia(
    data: date = Query(...),
    modo_forcar: bool = Query(False),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    envio_data = data.isoformat()
    base = "/app/exports/costs_transfer"
    tenant_dir = os.path.join(base, tenant_id)

    logger.info(f"📦 Gerando arquivos de visualização para tenant={tenant_id}, data={envio_data}")

    df = carregar_detalhes_transferencias(tenant_id, envio_data)
    if df.empty:
        logger.warning("⚠️ Nenhum dado encontrado para gerar relatórios.")
        raise HTTPException(status_code=404, detail="Nenhum dado encontrado para gerar relatórios.")

    # Geração dos arquivos
    csv_path = salvar_csv(df, tenant_id, envio_data)
    json_path = salvar_json(df, tenant_id, envio_data)
    pdf_path = gerar_relatorio_transferencias(envio_data, tenant_id, df)

    def _to_public_url(local_path: str | None) -> str | None:
        if not local_path or not os.path.exists(local_path):
            return None
        rel = os.path.relpath(local_path, base)
        return f"/exports/costs_transfer/{rel}"

    # 🔥 carrega JSON diretamente para já mandar para o frontend
    json_dados = df.to_dict(orient="records")

    return {
        "status": "ok",
        "mensagem": f"✅ Arquivos gerados para {envio_data}",
        "csv_url": _to_public_url(csv_path),
        "json_url": _to_public_url(json_path),
        "pdf_url": _to_public_url(pdf_path),
        "json_dados": json_dados
    }
