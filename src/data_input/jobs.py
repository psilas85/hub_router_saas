# hub_router_1.0.1/src/data_input/jobs.py

import logging
import subprocess
import re
import json
from datetime import datetime
from rq import get_current_job
from data_input.infrastructure.db import Database

logger = logging.getLogger(__name__)

def salvar_historico(tenant_id, job_id, status, arquivo, total, validos, invalidos, mensagem):
    try:
        db = Database()  # ✅ sem db_name
        db.conectar()
        cur = db.conexao.cursor()
        cur.execute(
            """
            INSERT INTO historico_data_input
            (tenant_id, job_id, arquivo, status, total_processados, validos, invalidos, mensagem, criado_em)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                job_id,
                arquivo,
                status,
                total,
                validos,
                invalidos,
                mensagem,
                datetime.utcnow(),
            ),
        )
        db.conexao.commit()
        cur.close()
    except Exception as e:
        logger.error(f"❌ Erro ao salvar histórico no banco: {e}", exc_info=True)


def processar_csv(job_id, tenant_id, file_path, modo_forcar, limite_peso_kg):
    job = get_current_job()
    logger.info(f"🚀 Iniciando job {job_id} para tenant {tenant_id}")

    try:
        comando = ["python", "-u", "-m", "data_input.main_preprocessing", "--tenant", tenant_id]

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

        # Executa subprocesso com logs em tempo real
        process = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )

        stdout_lines = []
        for line in iter(process.stdout.readline, ""):
            line = line.strip()
            if line:
                logger.info(f"[{job_id}] {line}")  # 🔎 log em tempo real
                stdout_lines.append(line)

        process.wait()
        if process.returncode != 0:
            raise Exception("\n".join(stdout_lines))

        validos, invalidos, total_processados = 0, 0, 0

        # Atualiza progresso parcial
        job.meta["step"] = "Interpretando saída"
        job.meta["progress"] = 70
        job.save_meta()

        # 🔎 1) JSON no stdout
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

        # 🔎 2) Regex fallback
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

        salvar_historico(
            tenant_id,
            job_id,
            "done",
            file_path.split("/")[-1],
            total_processados,
            validos,
            invalidos,
            "✅ Data Input finalizado com sucesso",
        )

        return {
            "status": "done",
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

        salvar_historico(
            tenant_id,
            job_id,
            "error",
            file_path.split("/")[-1],
            0,
            0,
            0,
            str(e),
        )

        return {
            "status": "error",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "error": str(e),
        }
