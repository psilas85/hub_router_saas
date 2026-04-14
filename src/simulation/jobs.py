#hub_router_1.0.1/src/simulation/jobs.py

import os
import uuid
import traceback
import json
from datetime import datetime, timedelta
from rq import get_current_job
from concurrent.futures import ProcessPoolExecutor, as_completed

from simulation.application.simulation_use_case import SimulationUseCase
from simulation.infrastructure.simulation_database_connection import (
    conectar_clusterization_db,
    conectar_simulation_db
)
from simulation.logs.simulation_logger import configurar_logger

from simulation.domain.entities import SimulationParams



def _json_log(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _extrair_motivo_curto_invalidacao(resultado_ignorado):
    payload = resultado_ignorado[2] if len(resultado_ignorado) > 2 else None
    cenarios = (payload or {}).get("cenarios_invalidados", []) if isinstance(payload, dict) else []

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


def _processar_data_envio(envio_data, tenant_id, hub_id, params, modo_forcar):
    """Executa simulação para 1 data de envio (processo separado)."""
    simulation_id = str(uuid.uuid4())
    log_file = f"/app/logs/simulation_{envio_data}.log"
    logger = configurar_logger(log_file)

    clusterization_db = conectar_clusterization_db()
    simulation_db = conectar_simulation_db()

    try:
        logger.info(f"🚀 Iniciando simulação para {envio_data}")
        logger.info(
            "[simulation.worker] envio_data=%s tenant_id=%s parametros_recebidos=%s",
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
        )

        ponto = use_case.executar_simulacao_completa()

        cenarios_invalidados = []
        if isinstance(ponto, dict):
            cenarios_invalidados = ponto.get("cenarios_invalidados", [])

        if ponto and ponto.get("k_clusters") is not None:
            logger.info(f"✅ Simulação concluída para {envio_data}")
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
            return (
                "ignorada",
                envio_data,
                {
                    "cenarios_invalidados": cenarios_invalidados,
                },
            )

    except Exception as e:
        logger.error(f"❌ Erro inesperado na simulação {envio_data}: {e}")
        return ("erro", envio_data, str(e))
    finally:
        clusterization_db.close()
        simulation_db.close()


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
    Executa simulação completa para um intervalo de datas (executado no worker RQ).
    Usa ProcessPoolExecutor para rodar múltiplas datas em paralelo.
    """

    job = get_current_job()
    if job:
        job.meta["step"] = "Inicializando"
        job.meta["progress"] = 0
        job.meta["datas_processadas"] = []
        job.save_meta()

    data_inicial_dt = datetime.strptime(data_inicial, "%Y-%m-%d").date()
    data_final_dt = datetime.strptime(data_final, "%Y-%m-%d").date()
    lista_datas = [data_inicial_dt + timedelta(days=i) for i in range((data_final_dt - data_inicial_dt).days + 1)]

    results = []
    try:


        max_workers = min(4, os.cpu_count() or 2)
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_processar_data_envio, str(d), tenant_id, hub_id, params, modo_forcar): d
                for d in lista_datas
            }
            for idx, future in enumerate(as_completed(futures), 1):
                result = future.result()
                results.append(result)

                if job:
                    job.meta["datas_processadas"].append(str(result[1]))
                    job.meta["step"] = f"Processando {result[1]}"
                    job.meta["progress"] = int((idx / len(lista_datas)) * 100)
                    job.save_meta()

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

                if job:
                    job.meta["step"] = "Erro"
                    job.meta["progress"] = 100
                    job.save_meta()

                status_final = "error"
            else:
                status_final = "ok"
                mensagem_invalidacoes = ""
                if invalidacoes:
                    partes_invalidacoes = []
                    for item in invalidacoes:
                        cenarios = ", ".join(
                            f"k={cenario['k_clusters']} ({cenario['motivo']})"
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

                    if job:
                        job.meta["step"] = "Falhou"
                        job.meta["progress"] = 100
                        job.save_meta()

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

                    if job:
                        job.meta["step"] = "Finalizado"
                        job.meta["progress"] = 100
                        job.save_meta()

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
        if job:
            job.meta["step"] = "Erro"
            job.meta["progress"] = 100
            job.save_meta()

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
                f"Erro na simulação: {str(e)}",
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
