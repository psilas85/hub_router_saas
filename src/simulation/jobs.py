#hub_router_1.0.1/src/simulation/jobs.py

import uuid
import traceback
import json
import time
from datetime import datetime, timedelta
from redis import Redis
from rq import Queue, get_current_job
from rq.job import Job

from simulation.application.simulation_use_case import SimulationUseCase
from simulation.infrastructure.simulation_database_connection import (
    conectar_clusterization_db,
    conectar_simulation_db
)
from simulation.logs.simulation_logger import configurar_logger




from simulation.domain.entities import SimulationParams


SIMULATION_JOBS_QUEUE = "simulation_jobs"
SIMULATION_DATE_JOBS_QUEUE = "simulation_date_jobs"



def _json_log(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _formatar_k_clusters(valor):
    texto = str(valor).strip()
    if not texto:
        return "k=?"
    if texto.startswith("k="):
        return texto
    return f"k={texto}"


def _extrair_mensagem_execucao(exc_info, fallback="Erro não identificado"):
    texto = str(exc_info or "").strip()
    if not texto:
        return fallback

    linhas = [linha.strip() for linha in texto.splitlines() if linha.strip()]
    if not linhas:
        return fallback

    ultima = linhas[-1]
    if ":" in ultima:
        return ultima.split(":", 1)[1].strip() or fallback
    return ultima


def _extrair_motivo_curto_invalidacao(resultado_ignorado):
    payload = resultado_ignorado[2] if len(resultado_ignorado) > 2 else None
    cenarios = (
        (payload or {}).get("cenarios_invalidados", [])
        if isinstance(payload, dict)
        else []
    )

    for cenario in cenarios:
        motivo = str(cenario.get("motivo") or "").lower()
        detalhes = cenario.get("detalhes") or []
        primeiro_erro = ""
        if isinstance(detalhes, list) and detalhes:
            primeiro_erro = str((detalhes[0] or {}).get("erro") or "").lower()

        texto_base = f"{motivo} {primeiro_erro}"
        if "falha ao encontrar subclusters viáveis" in texto_base:
            return "Nenhum cluster viável para roteirização."
        if "falha de roteirização last-mile" in texto_base:
            return "Nenhum cluster viável no last-mile."
        if "last-mile não gerou rotas válidas" in texto_base:
            return "Nenhuma rota viável no last-mile."
        if "transferência" in texto_base and "não gerou" in texto_base:
            return "Nenhuma rota viável na transferência."
        if "pré-check operacional invalidou" in texto_base:
            return "Nenhum cluster viável no pré-check operacional."

    return "Nenhum cenário viável."


def _montar_mensagem_ignoradas(data_inicial, data_final, ignoradas):
    datas_ignoradas = ", ".join(str(resultado[1]) for resultado in ignoradas)
    motivo_curto = _extrair_motivo_curto_invalidacao(ignoradas[0])
    return (
        f"Simulação de {data_inicial} a {data_final} falhou. "
        f"Datas afetadas: {datas_ignoradas}. {motivo_curto}"
    )


def _montar_mensagem_falha_data(envio_data, cenarios_invalidados):
    if not cenarios_invalidados:
        return f"Simulação da data {envio_data} falhou sem detalhes de cenário."

    partes = []
    for cenario in cenarios_invalidados:
        k_clusters = cenario.get("k_clusters")
        motivo = cenario.get("motivo") or "motivo não informado"
        partes.append(f"{_formatar_k_clusters(k_clusters)}: {motivo}")

    return (
        f"Simulação da data {envio_data} falhou: nenhum cenário viável. "
        + " | ".join(partes)
    )


def _atualizar_meta_job(job, progress, step, extra=None):
    if not job:
        return

    job.meta["progress"] = max(0, min(int(progress), 100))
    job.meta["step"] = step
    if extra:
        job.meta.update(extra)
    job.save_meta()


def _progresso_subjob(child_job):
    if child_job is None:
        return 0, "Aguardando início"

    if child_job.is_finished:
        return 100, "Concluído"

    if child_job.is_failed:
        return 100, child_job.meta.get("step") or "Erro"

    status = child_job.get_status(refresh=False)
    progress = child_job.meta.get("progress")
    step = child_job.meta.get("step")

    if progress is None:
        if status == "queued":
            progress = 5
        elif status in {"started", "deferred"}:
            progress = 15
        else:
            progress = 0

    if not step:
        if status == "queued":
            step = "Na fila"
        elif status in {"started", "deferred"}:
            step = "Processando"
        else:
            step = "Aguardando"

    return max(0, min(int(progress), 99)), step


def _processar_data_envio(envio_data, tenant_id, hub_id, params, modo_forcar):
    """Executa simulação para 1 data de envio (processo separado)."""
    simulation_id = str(uuid.uuid4())
    log_file = f"/app/logs/simulation_{envio_data}.log"
    logger = configurar_logger(log_file)
    child_job = get_current_job()

    _atualizar_meta_job(
        child_job,
        5,
        f"Preparando data {envio_data}",
        {"envio_data": str(envio_data)},
    )

    clusterization_db = conectar_clusterization_db()
    simulation_db = conectar_simulation_db()

    try:
        logger.info(f"🚀 Iniciando simulação para {envio_data}")
        logger.info(
            (
                "[simulation.worker] envio_data=%s tenant_id=%s "
                "parametros_recebidos=%s"
            ),
            envio_data,
            tenant_id,
            _json_log(params),
        )

        if not isinstance(params, SimulationParams):
            params = SimulationParams(**params)

        logger.info(
            "[simulation.worker] envio_data=%s tenant_id=%s params=%s",
            envio_data,
            tenant_id,
            _json_log(params.dict()),
        )

        _atualizar_meta_job(
            child_job,
            15,
            f"Carregando dados de {envio_data}",
        )

        use_case = SimulationUseCase(
            tenant_id=tenant_id,
            envio_data=envio_data,
            hub_id=hub_id,
            params=params,
            clusterization_db=clusterization_db,
            simulation_db=simulation_db,
            logger=logger,
            modo_forcar=modo_forcar,
            simulation_id=simulation_id,
            permitir_rotas_excedentes=params.permitir_rotas_excedentes,
            progress_callback=lambda progress, step: _atualizar_meta_job(child_job, progress, step),
        )

        _atualizar_meta_job(
            child_job,
            30,
            f"Executando simulação de {envio_data}",
        )

        ponto = use_case.executar_simulacao_completa()

        _atualizar_meta_job(
            child_job,
            90,
            f"Consolidando resultados de {envio_data}",
        )

        cenarios_invalidados = []
        if isinstance(ponto, dict):
            cenarios_invalidados = ponto.get("cenarios_invalidados", [])

        if isinstance(ponto, dict) and ponto.get("k_clusters") is None and cenarios_invalidados:
            mensagem_falha = _montar_mensagem_falha_data(
                envio_data,
                cenarios_invalidados,
            )
            logger.error(f"❌ {mensagem_falha}")
            raise RuntimeError(mensagem_falha)

        if ponto and ponto.get("k_clusters") is not None:
            logger.info(f"✅ Simulação concluída para {envio_data}")
            _atualizar_meta_job(
                child_job,
                100,
                f"Data {envio_data} concluída",
            )
            return (
                "ok",
                envio_data,
                {
                    "ponto_otimo": ponto,
                    "cenarios_invalidados": cenarios_invalidados,
                },
            )
        else:
            logger.warning(f"⚠️ Simulação ignorada para {envio_data}")
            _atualizar_meta_job(
                child_job,
                100,
                f"Data {envio_data} sem cenário viável",
            )
            return (
                "ignorada",
                envio_data,
                {
                    "cenarios_invalidados": cenarios_invalidados,
                },
            )

    except Exception as e:
        logger.error(f"❌ Erro inesperado na simulação {envio_data}: {e}")
        _atualizar_meta_job(
            child_job,
            100,
            f"Erro na data {envio_data}",
        )
        raise
    finally:
        clusterization_db.close()
        simulation_db.close()


def _enfileirar_subjobs_simulacao(
    job_id,
    lista_datas,
    tenant_id,
    hub_id,
    params,
    modo_forcar,
):
    redis_conn = Redis(host="redis", port=6379)
    queue = Queue(SIMULATION_DATE_JOBS_QUEUE, connection=redis_conn)
    timeout_subjob = 7200 if modo_forcar else 3600
    subjobs = []

    for envio_data in lista_datas:
        subjob = queue.enqueue(
            _processar_data_envio,
            str(envio_data),
            tenant_id,
            hub_id,
            params,
            modo_forcar,
            job_id=f"{job_id}:{envio_data}",
            job_timeout=timeout_subjob,
            result_ttl=86400,
            failure_ttl=86400,
        )
        subjobs.append(subjob.id)

    return redis_conn, subjobs


def _aguardar_subjobs_simulacao(
    job,
    redis_conn,
    subjobs,
    lista_datas,
    modo_forcar,
):
    timeout_segundos = max(
        7200,
        len(subjobs) * (2400 if modo_forcar else 1200),
    )
    start_time = time.time()
    processed_jobs = set()
    results_by_data = {}
    last_status = {}

    while True:
        if time.time() - start_time > timeout_segundos:
            raise TimeoutError(
                "Timeout aguardando subjobs da simulation | "
                f"timeout={timeout_segundos}s"
            )

        finished = 0
        progresso_total = 0
        etapas_ativas = []
        jobs = Job.fetch_many(subjobs, connection=redis_conn)

        for child_job in jobs:
            if child_job is None:
                continue

            child_job_id = child_job.id
            status = child_job.get_status(refresh=True)

            if last_status.get(child_job_id) != status:
                last_status[child_job_id] = status

            if child_job.is_finished:
                finished += 1

                if child_job_id not in processed_jobs:
                    processed_jobs.add(child_job_id)
                    resultado = child_job.result

                    if isinstance(resultado, tuple) and len(resultado) >= 2:
                        results_by_data[str(resultado[1])] = resultado

            elif child_job.is_failed:
                mensagem_child = _extrair_mensagem_execucao(
                    child_job.exc_info,
                    fallback=f"Subjob {child_job_id} falhou sem detalhes.",
                )
                raise RuntimeError(
                    f"Subjob da simulation falhou: {child_job_id}. {mensagem_child}"
                )

            progresso_child, etapa_child = _progresso_subjob(child_job)
            progresso_total += progresso_child
            if not child_job.is_finished and etapa_child:
                etapas_ativas.append(etapa_child)

        if job:
            progresso_medio = int(progresso_total / len(subjobs)) if subjobs else 100
            if finished == len(subjobs):
                step = "Todas as datas processadas"
                progress = 100
            elif etapas_ativas:
                step = f"{etapas_ativas[0]} ({finished}/{len(subjobs)} concluídos)"
                progress = max(1, min(progresso_medio, 99))
            else:
                step = f"Aguardando subjobs ({finished}/{len(subjobs)})"
                progress = max(1, min(progresso_medio, 99))

            job.meta["datas_processadas"] = sorted(results_by_data.keys())
            job.meta["step"] = step
            job.meta["progress"] = progress
            job.save_meta()

        if finished == len(subjobs):
            break

        time.sleep(0.5)

    return [
        results_by_data[str(envio_data)]
        for envio_data in lista_datas
        if str(envio_data) in results_by_data
    ]


def processar_simulacao(
    job_id: str,
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    hub_id: int,
    params: dict,
    modo_forcar: bool = False,
):
    """
    Executa a simulação como job coordenador e distribui uma data por subjob.
    """

    job = get_current_job()
    _atualizar_meta_job(job, 0, "Inicializando", {"datas_processadas": []})

    data_inicial_dt = datetime.strptime(data_inicial, "%Y-%m-%d").date()
    data_final_dt = datetime.strptime(data_final, "%Y-%m-%d").date()
    lista_datas = [
        data_inicial_dt + timedelta(days=i)
        for i in range((data_final_dt - data_inicial_dt).days + 1)
    ]

    try:
        redis_conn, subjobs = _enfileirar_subjobs_simulacao(
            job_id=job_id,
            lista_datas=lista_datas,
            tenant_id=tenant_id,
            hub_id=hub_id,
            params=params,
            modo_forcar=modo_forcar,
        )

        _atualizar_meta_job(
            job,
            2,
            f"Subjobs enfileirados ({len(subjobs)})",
            {"subjobs": subjobs},
        )

        results = _aguardar_subjobs_simulacao(
            job=job,
            redis_conn=redis_conn,
            subjobs=subjobs,
            lista_datas=lista_datas,
            modo_forcar=modo_forcar,
        )

        erros = [resultado for resultado in results if resultado[0] == "erro"]
        ignoradas = [resultado for resultado in results if resultado[0] == "ignorada"]
        invalidacoes = [
            {
                "envio_data": str(resultado[1]),
                "cenarios": (resultado[2] or {}).get("cenarios_invalidados", []),
            }
            for resultado in results
            if isinstance(resultado[2], dict) and (resultado[2] or {}).get("cenarios_invalidados")
        ]

        conn = conectar_simulation_db()
        try:
            cur = conn.cursor()

            if erros:
                datas_com_erro = ", ".join(str(resultado[1]) for resultado in erros)
                primeira_mensagem = erros[0][2] or "Erro não identificado"
                mensagem = (
                    f"Simulação de {data_inicial} a {data_final} falhou. "
                    f"Datas com erro: {datas_com_erro}. Detalhe: {primeira_mensagem}"
                )
                cur.execute("""
                    UPDATE historico_simulation
                    SET status = %s, mensagem = %s, datas = %s
                    WHERE job_id = %s AND tenant_id = %s
                """, (
                    "failed",
                    mensagem,
                    json.dumps([str(d) for d in lista_datas]),
                    job_id,
                    tenant_id
                ))

                _atualizar_meta_job(job, 100, "Erro")

                status_final = "error"
            else:
                status_final = "ok"
                mensagem_invalidacoes = ""
                if invalidacoes:
                    partes_invalidacoes = []
                    for item in invalidacoes:
                        cenarios = ", ".join(
                            f"{_formatar_k_clusters(cenario['k_clusters'])} ({cenario['motivo']})"
                            for cenario in item["cenarios"]
                        )
                        partes_invalidacoes.append(
                            f"{item['envio_data']}: {cenarios}"
                        )
                    mensagem_invalidacoes = (
                        " Cenários invalidados: " + " | ".join(partes_invalidacoes)
                    )

                if ignoradas and len(ignoradas) == len(results):
                    mensagem = _montar_mensagem_ignoradas(
                        data_inicial,
                        data_final,
                        ignoradas,
                    )
                    cur.execute("""
                        UPDATE historico_simulation
                        SET status = %s, mensagem = %s, datas = %s
                        WHERE job_id = %s AND tenant_id = %s
                    """, (
                        "failed",
                        mensagem,
                        json.dumps([str(d) for d in lista_datas]),
                        job_id,
                        tenant_id
                    ))

                    _atualizar_meta_job(job, 100, "Falhou")

                    status_final = "error"
                elif ignoradas:
                    mensagem = _montar_mensagem_ignoradas(
                        data_inicial,
                        data_final,
                        ignoradas,
                    )
                    mensagem = mensagem.replace(" falhou. ", " concluída com ressalvas. ", 1)
                else:
                    mensagem = f"Simulação de {data_inicial} a {data_final} concluída com sucesso."

                if status_final != "error":
                    mensagem += mensagem_invalidacoes

                    cur.execute("""
                        UPDATE historico_simulation
                        SET status = %s, mensagem = %s, datas = %s
                        WHERE job_id = %s AND tenant_id = %s
                    """, (
                        "finished",
                        mensagem,
                        json.dumps([str(d) for d in lista_datas]),
                        job_id,
                        tenant_id
                    ))

                    _atualizar_meta_job(job, 100, "Finalizado")

                    status_final = "ok"

            conn.commit()
        finally:
            cur.close()
            conn.close()

        return {
            "status": status_final,
            "tenant_id": tenant_id,
            "datas": [str(d) for d in lista_datas],
            "resultados": results,
            "cenarios_invalidados": invalidacoes,
        }

    except Exception as e:
        _atualizar_meta_job(job, 100, "Erro")

        mensagem_curta = _extrair_mensagem_execucao(
            str(e),
            fallback="Erro na simulação",
        )

        # ❌ Atualiza histórico como falhado
        conn = conectar_simulation_db()
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE historico_simulation
                SET status = %s, mensagem = %s
                WHERE job_id = %s AND tenant_id = %s
            """, (
                "failed",
                f"Erro na simulação: {mensagem_curta}",
                job_id,
                tenant_id
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        # Log detalhado fica só no worker (não vai pro campo mensagem)
        traceback.print_exc()

        raise e
