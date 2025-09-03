# hub_router_1.0.1/src/data_input/api/routes.py

from fastapi import APIRouter, Query, Depends, HTTPException, UploadFile, File, Request
from typing import Optional
import subprocess
import os
import logging
import re
import json
from authentication.utils.dependencies import obter_tenant_id_do_token

router = APIRouter(tags=["Data Input"])
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# 🔹 Função auxiliar para normalizar tenant_id
def normalizar_tenant(tenant_id: str) -> str:
    try:
        # Caso venha string JSON ou formato estranho
        if tenant_id and tenant_id.startswith("{"):
            tenant_payload = json.loads(tenant_id)
            tenant_id = tenant_payload.get("tenant_id") or tenant_payload.get("tenant")
            logger.info(f"📌 Tenant normalizado do payload: {tenant_id}")
        return tenant_id
    except Exception as e:
        logger.warning(f"⚠️ Não foi possível normalizar tenant_id={tenant_id}: {e}")
        return tenant_id


# 🔹 Healthcheck
@router.get("/health", summary="Healthcheck Data Input")
def healthcheck(request: Request):
    return {"status": "ok", "servico": "Data Input via Gateway"}


# 🔹 Executa pipeline já existente (sem upload)
@router.post("/processar", summary="Executar pipeline de Data Input")
def processar_data_input(
    modo_forcar: bool = Query(False, description="Forçar reprocessamento"),
    limite_peso_kg: Optional[float] = Query(None, description="Peso máximo permitido por CTE"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    try:
        tenant_id = normalizar_tenant(tenant_id)
        logger.info(f"🔑 Tenant usado: {tenant_id}")

        comando = ["python", "-m", "data_input.main_preprocessing", "--tenant", tenant_id]
        if modo_forcar:
            comando.append("--modo_forcar")
        if limite_peso_kg is not None:
            comando.extend(["--limite-peso-kg", str(limite_peso_kg)])

        result = subprocess.run(
            comando, capture_output=True, text=True, encoding="utf-8", errors="ignore"
        )
        if result.returncode != 0:
            raise Exception(f"Subprocesso falhou:\n{result.stderr or result.stdout}")

        # Parse da saída para extrair resumo
        stdout_lines = result.stdout.strip().splitlines()
        clean_lines = [re.sub(r"^INFO:.*?:", "", l).strip() for l in stdout_lines]

        validos, invalidos, total_processados = 0, 0, 0
        for l in clean_lines:
            match = re.search(
                r".*Resumo:\s*(\d+)\s+válid(?:o|os)[,]?\s*(\d+)\s+inválid",
                l,
                re.IGNORECASE,
            )
            if match:
                validos, invalidos = int(match.group(1)), int(match.group(2))
                total_processados = validos + invalidos
                break

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "mensagem": f"✅ Data Input finalizado para tenant {tenant_id}",
            "total_processados": total_processados,
            "validos": validos,
            "invalidos": invalidos,
        }

    except Exception as e:
        logger.error("🚨 Erro no Data Input", exc_info=True)
        raise HTTPException(500, detail=f"Erro no Data Input: {str(e)}")


# 🔹 Upload de CSV + pipeline
@router.post("/upload", summary="Upload de CSV e processar Data Input")
async def upload_data_input(
    file: UploadFile = File(...),
    modo_forcar: bool = Query(False, description="Forçar reprocessamento"),
    limite_peso_kg: Optional[float] = Query(None, description="Peso máximo permitido por CTE"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    try:
        logger.info("📩 [UPLOAD] Requisição recebida no Data Input")
        logger.info(f"🔑 Tenant recebido no upload: {tenant_id}")
        logger.info(f"📎 Arquivo recebido: {file.filename}, tipo: {file.content_type}")

        # monta diretório de input
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        tenant_dir = os.path.join(project_root, "data_input", "tenants", tenant_id, "input")
        os.makedirs(tenant_dir, exist_ok=True)

        file_path = os.path.join(tenant_dir, "dados_input.csv")
        with open(file_path, "wb") as f:
            f.write(await file.read())

        logger.info(f"✅ Arquivo salvo em: {file_path}")

        # executa pipeline como subprocesso
        comando = ["python", "-m", "data_input.main_preprocessing", "--tenant", tenant_id]
        if modo_forcar:
            comando.append("--modo_forcar")
        if limite_peso_kg is not None:
            comando.extend(["--limite-peso-kg", str(limite_peso_kg)])

        result = subprocess.run(comando, capture_output=True, text=True)

        if result.returncode != 0:
            logger.error(f"❌ Subprocesso falhou:\n{result.stderr or result.stdout}")
            raise Exception(f"Subprocesso falhou: {result.stderr or result.stdout}")

        # 🔹 tenta capturar o último JSON impresso no stdout
        resumo = {}
        for line in result.stdout.splitlines():
            if line.strip().startswith("{") and line.strip().endswith("}"):
                try:
                    resumo = json.loads(line.strip())
                except Exception:
                    pass

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "mensagem": f"✅ Upload + processamento concluído para tenant {tenant_id}",
            "total_processados": resumo.get("total_processados"),
            "validos": resumo.get("validos"),
            "invalidos": resumo.get("invalidos"),
        }

    except Exception as e:
        logger.error("❌ Erro em upload", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao processar upload: {str(e)}")
