import logging
import subprocess
import re
import json
from rq import get_current_job

logger = logging.getLogger(__name__)

def processar_csv(job_id, tenant_id, file_path, modo_forcar, limite_peso_kg):
    job = get_current_job()
    logger.info(f"🚀 Iniciando job {job_id} para tenant {tenant_id}")

    try:
        comando = ["python", "-m", "data_input.main_preprocessing", "--tenant", tenant_id]

        if modo_forcar:
            comando.append("--modo_forcar")
        if limite_peso_kg:
            comando.extend(["--limite-peso-kg", str(limite_peso_kg)])

        # Etapa inicial
        job.meta["step"] = "Iniciando processamento"
        job.meta["progress"] = 0
        job.save_meta()

        logger.info(f"▶️ Executando comando: {' '.join(comando)}")
        job.meta["step"] = "Executando pipeline"
        job.meta["progress"] = 20
        job.save_meta()

        result = subprocess.run(
            comando,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore"
        )

        if result.returncode != 0:
            raise Exception(result.stderr or result.stdout)

        stdout_lines = result.stdout.strip().splitlines()
        validos, invalidos, total_processados = 0, 0, 0

        # Atualiza progresso parcial
        job.meta["step"] = "Interpretando saída"
        job.meta["progress"] = 70
        job.save_meta()

        # 🔎 1) Tenta encontrar JSON no stdout
        for line in stdout_lines:
            if line.strip().startswith("{") and line.strip().endswith("}"):
                try:
                    resumo = json.loads(line.strip())
                    validos = resumo.get("validos", 0)
                    invalidos = resumo.get("invalidos", 0)
                    total_processados = resumo.get("total_processados", validos + invalidos)
                    break
                except Exception:
                    pass

        # 🔎 2) Se não achar JSON, tenta regex no log
        if total_processados == 0:
            for line in stdout_lines:
                match = re.search(
                    r".*Resumo:\s*(\d+)\s+válid(?:o|os)[,]?\s*(\d+)\s+inválid",
                    line,
                    re.IGNORECASE,
                )
                if match:
                    validos, invalidos = int(match.group(1)), int(match.group(2))
                    total_processados = validos + invalidos
                    break

        logger.info(f"✅ Job {job_id} concluído: {validos} válidos, {invalidos} inválidos")

        job.meta["step"] = "Finalizado"
        job.meta["progress"] = 100
        job.save_meta()

        return {
            "status": "ok",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "total_processados": total_processados,
            "validos": validos,
            "invalidos": invalidos,
            "mensagem": "✅ Data Input finalizado com sucesso",
        }

    except Exception as e:
        logger.error(f"❌ Erro no job {job_id}: {e}", exc_info=True)
        job.meta["step"] = "Erro"
        job.meta["progress"] = 100
        job.save_meta()
        return {
            "status": "error",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "error": str(e),
        }
