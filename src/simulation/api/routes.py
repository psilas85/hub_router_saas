# simulation/api/routes.py

from fastapi import APIRouter, Depends, HTTPException, Query
from datetime import date, timedelta
import logging
import uuid
import os

from authentication.utils.dependencies import obter_tenant_id_do_token
from simulation.application.simulation_use_case import SimulationUseCase
from simulation.logs.simulation_logger import configurar_logger
from simulation.infrastructure.simulation_database_connection import (
    conectar_clusterization_db,
    conectar_simulation_db
)
from simulation.visualization.gerar_graficos_custos_simulacao import gerar_graficos_custos_por_envio
from simulation.visualization.gerador_relatorio_final import executar_geracao_relatorio_final

router = APIRouter(
    prefix="/simulacao",
    tags=["Simulation"]
)

logger = logging.getLogger("simulation_service")
logger.setLevel(logging.INFO)


@router.get("/health", summary="Health Check", description="Verifica se o serviço de simulação está online.")
def healthcheck():
    return {"status": "ok", "servico": "Simulation"}


@router.post("/executar", summary="Executar Simulação Completa")
def executar_simulacao(
    # 📅 Datas e controle
    data_inicial: date = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final no formato YYYY-MM-DD"),
    modo_forcar: bool = Query(False, description="Sobrescreve simulações existentes"),

    # 🔢 Clusterização
    k_min: int = Query(2, description="Valor mínimo de k_clusters"),
    k_max: int = Query(50, description="Valor máximo de k_clusters"),
    k_inicial_transferencia: int = Query(1, description="K inicial para clusterização de transferências"),
    min_entregas_cluster: int = Query(25, description="Qtd mínima de entregas por cluster"),
    fundir_clusters_pequenos: bool = Query(False, description="Funde clusters pequenos com menos entregas que o mínimo"),
    desativar_cluster_hub: bool = Query(False, description="Desativa cluster automático para entregas próximas ao hub central"),
    raio_hub_km: float = Query(80.0, description="Raio em km para considerar entregas como parte do cluster do hub central"),

    # ⏱️ Tempos operacionais
    parada_leve: int = Query(10, description="Tempo de parada leve (min)"),
    parada_pesada: int = Query(20, description="Tempo de parada pesada (min)"),
    tempo_volume: float = Query(0.4, description="Tempo por volume (min)"),

    # 🚚 Operações e peso
    velocidade: float = Query(60.0, description="Velocidade média (km/h)"),
    limite_peso: float = Query(50.0, description="Limite de peso para considerar parada pesada (kg)"),
    peso_leve_max: float = Query(50.0, description="Peso máximo para considerar veículo leve"),

    # 🔗 Transferências
    tempo_max_transferencia: int = Query(1200, description="Tempo máximo por rota de transferência (min)"),
    peso_max_transferencia: float = Query(15000.0, description="Peso máximo por rota de transferência (kg)"),

    # 📦 Last-mile
    entregas_por_subcluster: int = Query(25, description="Quantidade alvo de entregas por subcluster"),
    tempo_max_roteirizacao: int = Query(1200, description="Tempo máximo total por rota last-mile (min)"),
    tempo_max_k1: int = Query(2400, description="Tempo máximo para simulação direta do hub central (k=1)"),

    # ⚙️ Restrições
    permitir_rotas_excedentes: bool = Query(False, description="Permitir rotas que ultrapassem o tempo máximo"),
    restricao_veiculo_leve_municipio: bool = Query(False, description="Restringe veículos leves em rotas intermunicipais"),

    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    """
    Executa a simulação de clusterização, roteirização e custeio,
    com base nos parâmetros recebidos. Agora também gera gráficos e relatórios.
    """
    if data_final < data_inicial:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial.")

    logger.info(f"🔹 Iniciando simulação de {data_inicial} a {data_final} para tenant {tenant_id}.")

    # 🔌 Conexões com bancos
    clusterization_db = conectar_clusterization_db()
    simulation_db = conectar_simulation_db()

    parametros = {
        # ⏱️ Tempos
        "tempo_parada_min": parada_leve,
        "tempo_parada_leve": parada_leve,
        "tempo_parada_pesada": parada_pesada,
        "tempo_descarga_por_volume": tempo_volume,
        "tempo_por_volume": tempo_volume,

        # 🚚 Operações
        "velocidade_media_kmh": velocidade,
        "limite_peso_parada": limite_peso,
        "peso_leve_max": peso_leve_max,

        # 🔗 Transferências
        "tempo_maximo_transferencia": tempo_max_transferencia,
        "peso_max_kg": peso_max_transferencia,

        # 📦 Last-mile
        "entregas_por_subcluster": entregas_por_subcluster,
        "tempo_maximo_roteirizacao": tempo_max_roteirizacao,
        "tempo_maximo_k1": tempo_max_k1,

        # 🔢 Clusterização
        "k_inicial_transferencia": k_inicial_transferencia,
        "k_min": k_min,
        "k_max": k_max,
        "min_entregas_cluster": min_entregas_cluster,
        "fundir_clusters_pequenos": fundir_clusters_pequenos,
        "desativar_cluster_hub": desativar_cluster_hub,
        "raio_hub_km": raio_hub_km,

        # ⚙️ Restrições
        "permitir_rotas_excedentes": permitir_rotas_excedentes,
        "restricao_veiculo_leve_municipio": restricao_veiculo_leve_municipio,
    }

    datas_processadas = []
    datas_ignoradas = []

    data_atual = data_inicial
    while data_atual <= data_final:
        try:
            simulation_id = str(uuid.uuid4())

            use_case = SimulationUseCase(
                tenant_id=tenant_id,
                envio_data=data_atual,
                parametros=parametros,
                clusterization_db=clusterization_db,
                simulation_db=simulation_db,
                logger=logger,
                modo_forcar=modo_forcar,
                fundir_clusters_pequenos=fundir_clusters_pequenos,
                permitir_rotas_excedentes=permitir_rotas_excedentes
            )

            ponto = use_case.executar_simulacao_completa()

            if ponto:
                datas_processadas.append(str(data_atual))

                # 🔹 Gera gráficos por envio
                gerar_graficos_custos_por_envio(simulation_db, tenant_id, datas_filtradas=[data_atual])

                # 🔹 Gera relatório final
                executar_geracao_relatorio_final(
                    tenant_id=tenant_id,
                    envio_data=str(data_atual),
                    simulation_id=simulation_id,
                    simulation_db=simulation_db
                )
            else:
                datas_ignoradas.append(str(data_atual))

        except Exception as e:
            logger.error(f"❌ Erro na simulação {data_atual}: {e}")
            datas_ignoradas.append(str(data_atual))

        data_atual += timedelta(days=1)

    # 🔹 Gera gráficos consolidados para todas as datas processadas
    if datas_processadas:
        gerar_graficos_custos_por_envio(simulation_db, tenant_id, datas_filtradas=datas_processadas)

    return {
        "status": "ok",
        "mensagem": f"✅ Simulação concluída. Processadas: {len(datas_processadas)}, Ignoradas: {len(datas_ignoradas)}",
        "datas_processadas": datas_processadas,
        "datas_ignoradas": datas_ignoradas,
        "parametros": parametros
    }

@router.get("/visualizar", summary="Visualizar artefatos da simulação")
def visualizar_simulacao(
    data: date = Query(..., description="Data no formato YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    """
    Retorna os artefatos gerados (mapas, tabelas, gráficos e relatório PDF)
    para a data informada, organizados por k_clusters.
    Marca também o cenário ótimo (is_ponto_otimo).
    """
    response = {"data": str(data), "cenarios": {}}

    # PDF consolidado
    pdf_path = f"./exports/simulation/relatorios/{tenant_id}/relatorio_simulation_{data}.pdf"
    if os.path.exists(pdf_path):
        response["relatorio_pdf"] = pdf_path.replace("./", "/")

    # Gráfico comparativo
    graficos_dir = f"./exports/simulation/graphs/{tenant_id}"
    if os.path.isdir(graficos_dir):
        graficos = [
            f"/exports/simulation/graphs/{tenant_id}/{f}"
            for f in os.listdir(graficos_dir)
            if f.startswith(f"grafico_simulacao_") or f.startswith(f"grafico_custos_{data}_")
        ]
        response["graficos"] = sorted(graficos)

    # === Descobrir cenário ótimo no banco ===
    import pandas as pd
    from simulation.infrastructure.simulation_database_connection import conectar_simulation_db

    try:
        query = """
            SELECT k_clusters
            FROM resultados_simulacao
            WHERE tenant_id = %s AND envio_data = %s AND is_ponto_otimo = TRUE
            LIMIT 1
        """
        df = pd.read_sql(query, conectar_simulation_db(), params=(tenant_id, str(data)))
        otimo_k = int(df.iloc[0]["k_clusters"]) if not df.empty else None
    except Exception:
        otimo_k = None

    # Mapas por cenário
    mapas_dir = f"./exports/simulation/maps/{tenant_id}"
    if os.path.isdir(mapas_dir):
        for f in os.listdir(mapas_dir):
            if f.endswith((".html", ".png")) and f"_{data}_" in f:
                parts = f.split("_k")
                if len(parts) > 1:
                    k = parts[-1].split(".")[0]
                    response["cenarios"].setdefault(
                        k, {"mapas": [], "tabelas_lastmile": [], "tabelas_transferencias": []}
                    )
                    response["cenarios"][k]["mapas"].append(
                        f"/exports/simulation/maps/{tenant_id}/{f}"
                    )

    # Tabelas last-mile
    lastmile_dir = f"./exports/simulation/tabelas_lastmile/{tenant_id}"
    if os.path.isdir(lastmile_dir):
        for f in os.listdir(lastmile_dir):
            if f.endswith(".png") and f"_{data}_" in f:
                k = f.split("_k")[-1].split(".")[0]
                response["cenarios"].setdefault(
                    k, {"mapas": [], "tabelas_lastmile": [], "tabelas_transferencias": []}
                )
                response["cenarios"][k]["tabelas_lastmile"].append(
                    f"/exports/simulation/tabelas_lastmile/{tenant_id}/{f}"
                )

    # Tabelas transferências
    transf_dir = f"./exports/simulation/tabelas_transferencias/{tenant_id}"
    if os.path.isdir(transf_dir):
        for f in os.listdir(transf_dir):
            if f.endswith(".png") and f"_{data}_" in f:
                k = f.split("_k")[-1].split("_")[0]
                response["cenarios"].setdefault(
                    k, {"mapas": [], "tabelas_lastmile": [], "tabelas_transferencias": []}
                )
                response["cenarios"][k]["tabelas_transferencias"].append(
                    f"/exports/simulation/tabelas_transferencias/{tenant_id}/{f}"
                )

    # Marca o cenário ótimo
    if otimo_k is not None and str(otimo_k) in response["cenarios"]:
        response["cenarios"][str(otimo_k)]["otimo"] = True

    if not response["relatorio_pdf"] and not response["cenarios"] and not response.get("graficos"):
        raise HTTPException(status_code=404, detail="Nenhum artefato encontrado para esta data.")

    return response
