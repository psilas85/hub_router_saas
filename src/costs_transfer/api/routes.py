# hub_router_1.0.1/src/costs_transfer/api/routes.py
from fastapi import APIRouter, Query, Depends, HTTPException
from datetime import date
import os, logging, json

from authentication.utils.dependencies import obter_tenant_id_do_token
from costs_transfer.domain.transfer_service import TransferCostService
from costs_transfer.visualization.gerar_relatorio_transferencias import gerar_relatorio_transferencias
from costs_transfer.visualization.main_visualization import carregar_detalhes_transferencias, salvar_csv, salvar_json

# 🔹 Padronizado para inglês (alinhado com frontend e API Gateway)
router = APIRouter(prefix="", tags=["Costs Transfer"])
logger = logging.getLogger("costs_transfer")


@router.post("/", summary="Calculate transfer costs")
def calcular_custos_transferencia(
    data_inicial: date = Query(...),
    data_final: date = Query(None),
    modo_forcar: bool = Query(False),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    if not data_final:
        data_final = data_inicial
    if data_final < data_inicial:
        raise HTTPException(status_code=400, detail="End date cannot be earlier than start date.")

    try:
        service = TransferCostService(tenant_id=tenant_id)
        service.processar_custos(
            data_inicial=data_inicial, data_final=data_final, modo_forcar=modo_forcar
        )
        return {
            "status": "ok",
            "mensagem": f"✅ Transfer costs processed for {data_inicial} → {data_final}"
        }
    except Exception as e:
        logger.error(f"❌ Error calculating transfer costs: {e}")
        raise HTTPException(status_code=500, detail="Error during processing.")


@router.get("/visualizar", summary="Generate, save and return transfer costs")
def visualizar_custos_transferencia(
    data: date = Query(...),
    modo_forcar: bool = Query(False),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    envio_data = data.isoformat()
    base = "/app/exports/costs_transfer"
    tenant_dir = os.path.join(base, tenant_id)

    logger.info(f"📦 Generating visualization files for tenant={tenant_id}, date={envio_data}")

    df = carregar_detalhes_transferencias(tenant_id, envio_data)
    if df.empty:
        logger.warning("⚠️ No data found to generate reports.")
        raise HTTPException(status_code=404, detail="No data found to generate reports.")

    # Generate files
    csv_path = salvar_csv(df, tenant_id, envio_data)
    json_path = salvar_json(df, tenant_id, envio_data)
    pdf_path = gerar_relatorio_transferencias(envio_data, tenant_id, df)

    def _to_public_url(local_path: str | None) -> str | None:
        if not local_path or not os.path.exists(local_path):
            return None
        rel = os.path.relpath(local_path, base)
        return f"/exports/costs_transfer/{rel}"

    # 🔥 Load JSON directly to send to frontend
    json_dados = df.to_dict(orient="records")

    return {
        "status": "ok",
        "mensagem": f"✅ Files generated for {envio_data}",
        "csv_url": _to_public_url(csv_path),
        "json_url": _to_public_url(json_path),
        "pdf_url": _to_public_url(pdf_path),
        "json_dados": json_dados
    }
