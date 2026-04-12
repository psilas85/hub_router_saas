# hub_router_1.0.1/src/simulation/main_simulation.py

import uuid
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed

load_dotenv()

from simulation.application.simulation_use_case import SimulationUseCase
from simulation.infrastructure.simulation_database_connection import (
    conectar_clusterization_db,
    conectar_simulation_db
)

# 🚫 Silencia warnings repetitivos do pandas
warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")


def configurar_logger(log_file="/app/logs/simulation_debug.log"):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger(log_file)  # logger único por arquivo
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch_formatter = logging.Formatter(
            "%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s"
        )
        ch.setFormatter(ch_formatter)

        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh_formatter = logging.Formatter(
            "%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s"
        )
        fh.setFormatter(fh_formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


def parse_args():
    parser = argparse.ArgumentParser(description="Executa simulações de clusterização, roteirização e custeio.")
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--data-inicial", required=True)
    parser.add_argument("--data-final", required=True)
    parser.add_argument("--hub-id", type=int, required=True)
    parser.add_argument("--parada-leve", type=int, default=10)
    parser.add_argument("--parada-pesada", type=int, default=20)
    parser.add_argument("--tempo-volume", type=float, default=0.40)
    parser.add_argument("--velocidade", type=float, default=60)
    parser.add_argument("--limite-peso", type=float, default=50)
    parser.add_argument("--tempo-max-transferencia", type=int, default=600)
    parser.add_argument("--peso-max-transferencia", type=float, default=18000)
    parser.add_argument("--entregas-por-subcluster", type=int, default=25)
    parser.add_argument("--tempo-max-roteirizacao", type=int, default=600)
    parser.add_argument("--tempo-max-k0", type=int, default=1200)
    parser.add_argument("--permitir-rotas-excedentes", dest="permitir_rotas_excedentes", action="store_true")
    parser.add_argument("--nao-permitir-rotas-excedentes", dest="permitir_rotas_excedentes", action="store_false")
    parser.add_argument("--restricao-veiculo-leve-municipio", dest="restricao_veiculo_leve_municipio", action="store_true")
    parser.add_argument("--sem-restricao-veiculo-leve-municipio", dest="restricao_veiculo_leve_municipio", action="store_false")
    parser.add_argument("--peso-leve-max", type=float, default=50.0)
    parser.add_argument("--desativar_cluster_hub", action="store_true")
    parser.add_argument("--raio_hub_km", type=float, default=80.0)
    parser.add_argument("--usar-outlier", dest="usar_outlier", action="store_true")
    parser.add_argument("--desativar-outlier", dest="usar_outlier", action="store_false")
    parser.add_argument("--distancia-outlier-km", type=float, default=None)
    parser.add_argument("--min-entregas-por-cluster-alvo", type=int, default=10)
    parser.add_argument("--max-entregas-por-cluster-alvo", type=int, default=100)
    parser.add_argument(
        "--algoritmo-clusterizacao-principal",
        choices=["kmeans", "balanced_kmeans", "time_windows"],
        default="kmeans",
    )
    parser.add_argument("--modo-forcar", action="store_true")
    parser.set_defaults(
        usar_outlier=False,
        restricao_veiculo_leve_municipio=True,
        permitir_rotas_excedentes=True,
    )
    return parser.parse_args()


def processar_data(data_atual, tenant_id, args, parametros):
    """Executa simulação de uma data em processo separado."""
    log_file = f"/app/logs/simulation_{data_atual}.log"
    logger = configurar_logger(log_file)

    logger.info(f"🚀 Iniciando simulação paralela para {data_atual}")

    clusterization_db = conectar_clusterization_db()
    simulation_db = conectar_simulation_db()

    try:
        simulation_id = str(uuid.uuid4())
        use_case = SimulationUseCase(
            tenant_id=tenant_id,
            envio_data=data_atual,
            hub_id=args.hub_id,
            parametros=parametros,
            clusterization_db=clusterization_db,
            simulation_db=simulation_db,
            logger=logger,
            modo_forcar=args.modo_forcar,
            simulation_id=simulation_id,
            permitir_rotas_excedentes=args.permitir_rotas_excedentes
        )

        ponto = use_case.executar_simulacao_completa()
        if ponto and ponto.get("k_clusters") is not None:
            logger.info(f"✅ Simulação concluída para {data_atual}")
            return ("ok", data_atual, ponto)
        else:
            logger.warning(f"⚠️ Simulação ignorada para {data_atual}")
            return ("ignorada", data_atual, ponto)

    except Exception as e:
        logger.error(f"❌ Erro inesperado na simulação {data_atual}: {str(e)}")
        return ("erro", data_atual, None)

    finally:
        try:
            clusterization_db.close()
            simulation_db.close()
        except Exception:
            pass


if __name__ == "__main__":
    logger = configurar_logger("/app/logs/simulation_debug.log")
    args = parse_args()

    tenant_id = args.tenant
    data_inicial = datetime.strptime(args.data_inicial, "%Y-%m-%d").date()
    data_final = datetime.strptime(args.data_final, "%Y-%m-%d").date()

    parametros = {
        "tempo_parada_min": args.parada_leve,
        "tempo_parada_leve": args.parada_leve,
        "tempo_parada_pesada": args.parada_pesada,
        "tempo_descarga_por_volume": args.tempo_volume,
        "tempo_por_volume": args.tempo_volume,
        "velocidade_media_kmh": args.velocidade,
        "limite_peso_parada": args.limite_peso,
        "tempo_maximo_transferencia": args.tempo_max_transferencia,
        "peso_max_kg": args.peso_max_transferencia,
        "entregas_por_subcluster": args.entregas_por_subcluster,
        "tempo_maximo_roteirizacao": args.tempo_max_roteirizacao,
        "tempo_maximo_k0": args.tempo_max_k0,
        "k_min": 1,
        "k_max": 50,
        "min_entregas_cluster": 25,
        "permitir_rotas_excedentes": args.permitir_rotas_excedentes,
        "restricao_veiculo_leve_municipio": args.restricao_veiculo_leve_municipio,
        "peso_leve_max": args.peso_leve_max,
        "desativar_cluster_hub": args.desativar_cluster_hub,
        "raio_hub_km": args.raio_hub_km,
        "usar_outlier": args.usar_outlier,
        "distancia_outlier_km": args.distancia_outlier_km,
        "min_entregas_por_cluster_alvo": args.min_entregas_por_cluster_alvo,
        "max_entregas_por_cluster_alvo": args.max_entregas_por_cluster_alvo,
        "algoritmo_clusterizacao_principal": args.algoritmo_clusterizacao_principal,
    }

    lista_datas = [data_inicial + timedelta(days=i) for i in range((data_final - data_inicial).days + 1)]
    datas_processadas, datas_ignoradas, pontos_inflexao = [], [], []

    max_workers = 4  # ⚡ Paralelismo fixo
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(processar_data, d, tenant_id, args, parametros): d for d in lista_datas}

        for future in as_completed(futures):
            status, data, ponto = future.result()
            if status == "ok" and ponto and ponto.get("k_clusters") is not None:
                datas_processadas.append(data)
                pontos_inflexao.append((data, ponto['k_clusters'], ponto['custo_total']))
            else:
                datas_ignoradas.append(data)

    logger.info("\n🏁 RESUMO FINAL DA SIMULAÇÃO")
    logger.info(f"✅ Datas processadas com sucesso: {len(datas_processadas)}")
    logger.info(f"📭 Datas ignoradas (sem entregas ou erro): {len(datas_ignoradas)}")
    if pontos_inflexao:
        logger.info("\n📉 Pontos de inflexão identificados:")
        for envio_data, k, custo in pontos_inflexao:
            logger.info(f"🟢 {envio_data} → {k} clusters, Custo total: R${custo:,.2f}")
