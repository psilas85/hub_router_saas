#hub_router_1.0.1/src/simulation/main_simulation.py

import uuid
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging

load_dotenv()

from simulation.visualization.gerador_relatorio_final import executar_geracao_relatorio_final
from simulation.visualization.gerar_graficos_custos_simulacao import gerar_graficos_custos_por_envio
from simulation.application.simulation_use_case import SimulationUseCase
from simulation.infrastructure.simulation_database_connection import (
    conectar_clusterization_db,
    conectar_simulation_db
)


def configurar_logger(log_file="/app/logs/simulation_debug.log"):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger("simulation")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Console
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(ch_formatter)

        # Arquivo
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(fh_formatter)

        logger.addHandler(ch)
        logger.addHandler(fh)

    return logger


def parse_args():
    parser = argparse.ArgumentParser(description="Executa simulações de clusterização, roteirização e custeio.")

    # 📅 Parâmetros principais
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data-inicial", required=True, help="Data inicial (YYYY-MM-DD)")
    parser.add_argument("--data-final", required=True, help="Data final (YYYY-MM-DD)")

    # 🔗 Hub Central
    parser.add_argument("--hub-id", type=int, required=True, help="ID do hub central a ser usado")

    # 🔢 Clusterização
    parser.add_argument("--k-min", type=int, default=2, help="Valor mínimo de k_clusters")
    parser.add_argument("--k-max", type=int, default=50, help="Valor máximo de k_clusters")
    parser.add_argument("--k-inicial-transferencia", type=int, default=1, help="Valor inicial de k para clusterização de transferências")
    parser.add_argument("--fundir-clusters-pequenos", action="store_true", help="Funde clusters pequenos com menos entregas que o mínimo (default = NÃO funde)")
    parser.add_argument("--min-entregas-cluster", type=int, default=25, help="Qtd mínima de entregas por cluster")

    # ⏱️ Tempos operacionais
    parser.add_argument("--parada-leve", type=int, default=10, help="Tempo de parada leve (min)")
    parser.add_argument("--parada-pesada", type=int, default=20, help="Tempo de parada pesada (min)")
    parser.add_argument("--tempo-volume", type=float, default=0.40, help="Tempo por volume (min)")

    # 🚚 Operações e velocidade
    parser.add_argument("--velocidade", type=float, default=60, help="Velocidade média (km/h)")
    parser.add_argument("--limite-peso", type=float, default=50, help="Limite peso para considerar parada pesada (kg)")

    # 🔗 Transferências
    parser.add_argument("--tempo-max-transferencia", type=int, default=1200, help="Tempo máximo total de rota de transferência (min)")
    parser.add_argument("--peso-max-transferencia", type=float, default=15000, help="Peso máximo por rota de transferência (kg)")

    # 📦 Last-mile
    parser.add_argument("--entregas-por-subcluster", type=int, default=25, help="🎯 Quantidade alvo de entregas por subcluster (por rota) na roteirização last-mile.")
    parser.add_argument("--tempo-max-roteirizacao", type=int, default=1200, help="Tempo máximo total por rota last-mile (min)")
    parser.add_argument("--tempo-max-k1", type=int, default=2400, help="Tempo máximo para simulação direta do hub central (k=1)")

    # ⚙️ Restrições operacionais
    parser.add_argument(
        "--permitir-rotas-excedentes",
        dest="permitir_rotas_excedentes",
        action="store_true",
        help="Permite aceitar rotas que ultrapassem o tempo máximo (default: NÃO permite)"
    )
    parser.add_argument(
        "--restricao-veiculo-leve-municipio",
        action="store_true",
        help="Se ativado, impede que veículos leves operem fora da cidade do centro do cluster quando o peso do subcluster for menor ou igual a --peso-leve-max. (Default: desativado)"
    )
    parser.add_argument(
        "--peso-leve-max",
        type=float,
        default=50.0,
        help="Peso máximo (kg) para considerar que um veículo é leve para efeito de restrição municipal."
    )

    parser.add_argument("--desativar_cluster_hub", action="store_true", help="Desativa o cluster automático para entregas próximas ao hub central")
    parser.add_argument("--raio_hub_km", type=float, default=80.0, help="Raio em km para considerar entregas como parte do cluster do hub central")

    # ♻️ Modo forçar
    parser.add_argument("--modo-forcar", action="store_true", help="Força reexecução mesmo com simulações existentes.")

    return parser.parse_args()


if __name__ == "__main__":
    logger = configurar_logger("/app/logs/simulation_debug.log")
    args = parse_args()

    tenant_id = args.tenant
    data_inicial = datetime.strptime(args.data_inicial, "%Y-%m-%d").date()
    data_final = datetime.strptime(args.data_final, "%Y-%m-%d").date()

    parametros = {
        # ⏱️ Tempos
        "tempo_parada_min": args.parada_leve,
        "tempo_parada_leve": args.parada_leve,
        "tempo_parada_pesada": args.parada_pesada,
        "tempo_descarga_por_volume": args.tempo_volume,
        "tempo_por_volume": args.tempo_volume,

        # 🚚 Operações
        "velocidade_media_kmh": args.velocidade,
        "limite_peso_parada": args.limite_peso,

        # 🔗 Transferências
        "tempo_maximo_transferencia": args.tempo_max_transferencia,
        "peso_max_kg": args.peso_max_transferencia,

        # 📦 Last-mile
        "entregas_por_subcluster": args.entregas_por_subcluster,
        "tempo_maximo_roteirizacao": args.tempo_max_roteirizacao,
        "tempo_maximo_k1": args.tempo_max_k1,

        # 🔢 Clusterização
        "k_inicial_transferencia": args.k_inicial_transferencia,
        "k_min": args.k_min,
        "k_max": args.k_max,
        "min_entregas_cluster": args.min_entregas_cluster,

        # ⚙️ Restrições
        "permitir_rotas_excedentes": args.permitir_rotas_excedentes,
        "restricao_veiculo_leve_municipio": args.restricao_veiculo_leve_municipio,
        "peso_leve_max": args.peso_leve_max,

        "desativar_cluster_hub": args.desativar_cluster_hub,
        "raio_hub_km": args.raio_hub_km,
    }

    # 🔌 Conexões com bancos
    clusterization_db = conectar_clusterization_db()
    simulation_db = conectar_simulation_db()

    datas_processadas = []
    datas_ignoradas = []
    pontos_inflexao = []

    data_atual = data_inicial
    while data_atual <= data_final:
        logger.info(f"\n📦 Iniciando simulação para envio_data = {data_atual}...")
        logger.info("🔧 Parâmetros da simulação:")
        for chave, valor in parametros.items():
            logger.info(f"   • {chave}: {valor}")

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
            fundir_clusters_pequenos=args.fundir_clusters_pequenos,
            permitir_rotas_excedentes=args.permitir_rotas_excedentes
        )

        try:
            ponto = use_case.executar_simulacao_completa()
            if ponto is None:
                datas_ignoradas.append(data_atual)
            else:
                datas_processadas.append(data_atual)
                pontos_inflexao.append((data_atual, ponto['k_clusters'], ponto['custo_total']))

                # 📊 Gera gráficos apenas da data atual
                gerar_graficos_custos_por_envio(
                    simulation_db,
                    tenant_id,
                    datas_filtradas=[data_atual]
                )

                executar_geracao_relatorio_final(
                    tenant_id=tenant_id,
                    envio_data=str(data_atual),
                    simulation_id=simulation_id,
                    simulation_db=simulation_db
                )

        except Exception as e:
            logger.error(f"❌ Erro inesperado ao simular para {data_atual}: {str(e)}")
            datas_ignoradas.append(data_atual)

        data_atual += timedelta(days=1)

    logger.info("\n🏁 RESUMO FINAL DA SIMULAÇÃO")
    logger.info(f"✅ Datas processadas com sucesso: {len(datas_processadas)}")
    logger.info(f"📭 Datas ignoradas (sem entregas ou erro): {len(datas_ignoradas)}")
    if datas_ignoradas:
        logger.info(f"📅 Ignoradas: {', '.join(map(str, datas_ignoradas))}")

    if pontos_inflexao:
        logger.info("\n📉 Pontos de inflexão identificados:")
        for envio_data, k, custo in pontos_inflexao:
            if custo is not None:
                logger.info(f"🟢 {envio_data} → {k} clusters, Custo total: R${custo:,.2f}")
            else:
                logger.warning(f"🟡 {envio_data} → {k} clusters, simulação encerrada sem custo registrado.")
