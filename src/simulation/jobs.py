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
from simulation.visualization.gerar_graficos_custos_simulacao import gerar_graficos_custos_por_envio
from simulation.visualization.gerador_relatorio_final import executar_geracao_relatorio_final


def _processar_data_envio(envio_data, tenant_id, hub_id, parametros, modo_forcar):
    """Executa simulação para 1 data de envio (processo separado)."""
    simulation_id = str(uuid.uuid4())
    log_file = f"/app/logs/simulation_{envio_data}.log"
    logger = configurar_logger(log_file)

    clusterization_db = conectar_clusterization_db()
    simulation_db = conectar_simulation_db()

    try:
        logger.info(f"🚀 Iniciando simulação para {envio_data}")

        parametros_final = {
            "tempo_parada_min": parametros.get("parada_leve", 10),
            "tempo_parada_leve": parametros.get("parada_leve", 10),
            "tempo_parada_pesada": parametros.get("parada_pesada", 20),
            "tempo_descarga_por_volume": parametros.get("tempo_volume", 0.40),
            "tempo_por_volume": parametros.get("tempo_volume", 0.40),
            "velocidade_media_kmh": parametros.get("velocidade", 60),
            "limite_peso_parada": parametros.get("limite_peso", 50),
            "tempo_maximo_transferencia": parametros.get("tempo_max_transferencia", 1200),
            "peso_max_kg": parametros.get("peso_max_transferencia", 15000),
            "entregas_por_subcluster": parametros.get("entregas_por_subcluster", 25),
            "tempo_maximo_roteirizacao": parametros.get("tempo_max_roteirizacao", 1200),
            "tempo_maximo_k0": parametros.get("tempo_max_k0", 2400),
            "k_inicial_transferencia": parametros.get("k_inicial_transferencia", 1),
            "k_min": parametros.get("k_min", 2),
            "k_max": parametros.get("k_max", 50),
            "min_entregas_cluster": parametros.get("min_entregas_cluster", 25),
            "permitir_rotas_excedentes": parametros.get("permitir_rotas_excedentes", False),
            "restricao_veiculo_leve_municipio": parametros.get("restricao_veiculo_leve_municipio", False),
            "peso_leve_max": parametros.get("peso_leve_max", 50.0),
            "desativar_cluster_hub": parametros.get("desativar_cluster_hub", False),
            "raio_hub_km": parametros.get("raio_hub_km", 80.0),
        }

        use_case = SimulationUseCase(
            tenant_id=tenant_id,
            envio_data=envio_data,
            hub_id=hub_id,
            parametros=parametros_final,
            clusterization_db=clusterization_db,
            simulation_db=simulation_db,
            logger=logger,
            modo_forcar=modo_forcar,
            simulation_id=simulation_id,
            fundir_clusters_pequenos=parametros_final.get("fundir_clusters_pequenos", False),
            permitir_rotas_excedentes=parametros_final.get("permitir_rotas_excedentes", False),
        )

        ponto = use_case.executar_simulacao_completa()

        if ponto:
            gerar_graficos_custos_por_envio(
                simulation_db=simulation_db,
                tenant_id=tenant_id,
                datas_filtradas=[envio_data],
                modo_forcar=modo_forcar
            )
            executar_geracao_relatorio_final(
                tenant_id=tenant_id,
                envio_data=str(envio_data),
                simulation_id=simulation_id,
                simulation_db=simulation_db,
                modo_forcar=modo_forcar
            )
            logger.info(f"✅ Simulação concluída para {envio_data}")
            return ("ok", envio_data)
        else:
            logger.warning(f"⚠️ Simulação ignorada para {envio_data}")
            return ("ignorada", envio_data)

    except Exception as e:
        logger.error(f"❌ Erro inesperado na simulação {envio_data}: {e}")
        return ("erro", envio_data)
    finally:
        clusterization_db.close()
        simulation_db.close()


def processar_simulacao(
    job_id: str,
    tenant_id: str,
    data_inicial: str,
    data_final: str,
    hub_id: int,
    parametros: dict,
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
        max_workers = 4
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_processar_data_envio, d, tenant_id, hub_id, parametros, modo_forcar): d
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

        if job:
            job.meta["step"] = "Finalizado"
            job.meta["progress"] = 100
            job.save_meta()

        # ✅ Atualiza histórico como concluído
        conn = conectar_simulation_db()
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE historico_simulation
                SET status = %s, mensagem = %s, datas = %s
                WHERE job_id = %s AND tenant_id = %s
            """, (
                "finished",
                f"Simulação de {data_inicial} a {data_final} concluída com sucesso.",
                json.dumps([str(d) for d in lista_datas]),
                job_id,
                tenant_id
            ))
            conn.commit()
        finally:
            cur.close()
            conn.close()

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "datas": [str(d) for d in lista_datas],
            "resultados": results,
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
