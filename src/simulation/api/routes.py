# simulation/api/routes.py

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from datetime import date, timedelta
import logging
import uuid
import os
from pydantic import BaseModel
from typing import List

from authentication.utils.dependencies import obter_tenant_id_do_token
from simulation.application.simulation_use_case import SimulationUseCase
from simulation.logs.simulation_logger import configurar_logger
from simulation.infrastructure.simulation_database_connection import (
    conectar_clusterization_db,
    conectar_simulation_db
)
from simulation.visualization.gerar_graficos_custos_simulacao import gerar_graficos_custos_por_envio
from simulation.visualization.gerador_relatorio_final import executar_geracao_relatorio_final
from simulation.visualization.gerar_grafico_distribuicao_k import gerar_grafico_distribuicao_k
from simulation.visualization.gerar_grafico_frequencia_cidades import gerar_grafico_frequencia_cidades
from simulation.visualization.gerar_grafico_k_fixo import gerar_grafico_k_fixo
from simulation.visualization.gerar_grafico_frota_k_fixo import gerar_grafico_frota_k_fixo


router = APIRouter(
    prefix="/simulacao",
    tags=["Simulation"]
)

logger = logging.getLogger("simulation_service")
logger.setLevel(logging.INFO)

# ================================
# MODELOS Pydantic
# ================================
class HubIn(BaseModel):
    nome: str
    cidade: str
    latitude: float
    longitude: float

class HubOut(HubIn):
    hub_id: int

class ClusterCostIn(BaseModel):
    limite_qtd_entregas: int
    custo_fixo_diario: float
    custo_variavel_por_entrega: float

class ClusterCostOut(ClusterCostIn):
    id: int


@router.get("/health", summary="Health Check", description="Verifica se o serviço de simulação está online.")
def healthcheck():
    return {"status": "ok", "servico": "Simulation"}


@router.post("/executar", summary="Executar Simulação Completa")
def executar_simulacao(
    # 📅 Datas e controle
    data_inicial: date = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final no formato YYYY-MM-DD"),
    modo_forcar: bool = Query(False, description="Sobrescreve simulações existentes"),

    # 🔗 Hub central
    hub_id: int = Query(..., description="ID do hub central"),  # 👈 ADICIONAR

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
                hub_id=hub_id,   # ✅ agora obrigatório
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
    Retorna os artefatos gerados (mapas, tabelas, gráficos, CSVs e relatório PDF)
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
            if f.startswith("grafico_simulacao_") or f.startswith(f"grafico_custos_{data}_")
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
                        k,
                        {
                            "mapas": [],
                            "tabelas_lastmile": [],
                            "tabelas_transferencias": [],
                            "tabelas_resumo": [],
                            "tabelas_detalhes": []
                        }
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
                    k,
                    {
                        "mapas": [],
                        "tabelas_lastmile": [],
                        "tabelas_transferencias": [],
                        "tabelas_resumo": [],
                        "tabelas_detalhes": []
                    }
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
                    k,
                    {
                        "mapas": [],
                        "tabelas_lastmile": [],
                        "tabelas_transferencias": [],
                        "tabelas_resumo": [],
                        "tabelas_detalhes": []
                    }
                )
                response["cenarios"][k]["tabelas_transferencias"].append(
                    f"/exports/simulation/tabelas_transferencias/{tenant_id}/{f}"
                )

    # Tabelas resumo CSV
    resumo_dir = f"./exports/simulation/resumos/{tenant_id}"
    if os.path.isdir(resumo_dir):
        for f in os.listdir(resumo_dir):
            if f.endswith(".csv") and f"_{data}_" in f:
                k = f.split("_k")[-1].split(".")[0]
                response["cenarios"].setdefault(
                    k,
                    {
                        "mapas": [],
                        "tabelas_lastmile": [],
                        "tabelas_transferencias": [],
                        "tabelas_resumo": [],
                        "tabelas_detalhes": []
                    }
                )
                response["cenarios"][k]["tabelas_resumo"].append(
                    f"/exports/simulation/resumos/{tenant_id}/{f}"
                )

    # Tabelas detalhes CSV
    detalhes_dir = f"./exports/simulation/detalhes/{tenant_id}"
    if os.path.isdir(detalhes_dir):
        for f in os.listdir(detalhes_dir):
            if f.endswith(".csv") and f"_{data}_" in f:
                k = f.split("_k")[-1].split(".")[0]
                response["cenarios"].setdefault(
                    k,
                    {
                        "mapas": [],
                        "tabelas_lastmile": [],
                        "tabelas_transferencias": [],
                        "tabelas_resumo": [],
                        "tabelas_detalhes": []
                    }
                )
                response["cenarios"][k]["tabelas_detalhes"].append(
                    f"/exports/simulation/detalhes/{tenant_id}/{f}"
                )

    # Marca o cenário ótimo
    if otimo_k is not None and str(otimo_k) in response["cenarios"]:
        response["cenarios"][str(otimo_k)]["otimo"] = True

    # 🔑 Se não achou nada, responde 404
    if not response.get("relatorio_pdf") and not response.get("cenarios") and not response.get("graficos"):
        raise HTTPException(status_code=404, detail="Nenhum artefato encontrado para esta data.")

    return response



@router.get("/distribuicao_k", summary="Distribuição de k_clusters ponto ótimo")
def distribuicao_k(
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    if (data_final - data_inicial).days > 365:
        raise HTTPException(status_code=400, detail="Período máximo permitido é 12 meses.")

    filename, data = gerar_grafico_distribuicao_k(
        tenant_id=tenant_id,
        data_inicial=str(data_inicial),
        data_final=str(data_final),
    )

    if not data:
        raise HTTPException(status_code=404, detail="Nenhum ponto ótimo encontrado no período informado.")

    return {
        "status": "ok",
        "data_inicial": str(data_inicial),
        "data_final": str(data_final),
        "grafico": filename.replace("./", "/"),
        "dados": data  # lista de {k_clusters, qtd}
    }

@router.get("/frequencia_cidades", summary="Frequência de cidades em pontos ótimos")
def frequencia_cidades(
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    """
    Retorna gráfico, CSV e dados da frequência das cidades centro (cluster_cidade)
    em simulações marcadas como ponto ótimo no período informado.
    """
    if (data_final - data_inicial).days > 365:
        raise HTTPException(status_code=400, detail="Período máximo permitido é 12 meses.")

    result = gerar_grafico_frequencia_cidades(
        tenant_id=tenant_id,
        data_inicial=str(data_inicial),
        data_final=str(data_final),
    )

    if not result or not result.get("dados"):
        raise HTTPException(status_code=404, detail="Nenhuma cidade encontrada em pontos ótimos no período informado.")

    # Retorna o próprio dicionário já no formato esperado
    return result



@router.get("/k_fixo", summary="Avaliar cenário k fixo no período")
def k_fixo(
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    usar_media: bool = Query(False, description="Usar média ao invés de soma (modo FULL)"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    """
    Avalia cenários de k_clusters fixos no período informado.
    Retorna gráfico, CSV consolidado e métricas (custo total/médio e regret).
    """
    png, csv, df = gerar_grafico_k_fixo(
        tenant_id=tenant_id,
        data_inicial=str(data_inicial),
        data_final=str(data_final),
        usar_media_em_vez_de_soma=usar_media,
    )

    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="Nenhum cenário encontrado no período informado.")

    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "data_inicial": str(data_inicial),
        "data_final": str(data_final),
        "grafico": png.replace("./", "/") if png else None,
        "csv": csv.replace("./", "/") if csv else None,
        "dados": df.to_dict(orient="records"),
    }


@router.get("/frota_k_fixo", summary="Frota média sugerida por k fixo")
def frota_k_fixo(
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    k: list[int] = Query(..., description="Um ou mais valores de k (ex: k=8&k=9&k=10)"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    """
    Avalia a frota média necessária no período para um ou mais k fixos.
    Retorna gráficos por tipo de veículo, CSV consolidado e detalhado,
    já separado em lastmile e transfer.
    """
    _, csv, df = gerar_grafico_frota_k_fixo(
        tenant_id=tenant_id,
        data_inicial=str(data_inicial),
        data_final=str(data_final),
        k_list=k,
    )

    if df is None or df.empty:
        raise HTTPException(
            status_code=404,
            detail="Nenhum dado de frota encontrado no período informado."
        )

    # 🔹 Converte DataFrame em lista de dicts
    dados = df.to_dict(orient="records")

    # 🔹 Separa por origem
    lastmile = [d for d in dados if d.get("origem") == "lastmile"]
    transfer = [d for d in dados if d.get("origem") == "transfer"]

    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "data_inicial": str(data_inicial),
        "data_final": str(data_final),
        "csv": csv.replace("./", "/") if csv else None,
        "lastmile": lastmile,
        "transfer": transfer,
    }


# ================================
# CRUD Hubs
# ================================
@router.get("/hubs", response_model=List[HubOut], summary="Listar hubs")
def listar_hubs(tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("""
        SELECT hub_id, nome, cidade, latitude, longitude
        FROM hubs WHERE tenant_id=%s ORDER BY hub_id DESC
    """, (tenant_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [HubOut(hub_id=r[0], nome=r[1], cidade=r[2], latitude=r[3], longitude=r[4]) for r in rows]

@router.post("/hubs", response_model=HubOut, summary="Criar novo hub")
def criar_hub(payload: HubIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO hubs (tenant_id, nome, cidade, latitude, longitude)
            VALUES (%s,%s,%s,%s,%s)
            RETURNING hub_id, nome, cidade, latitude, longitude
        """, (tenant_id, payload.nome, payload.cidade, payload.latitude, payload.longitude))
        row = cur.fetchone()
        conn.commit()
    except Exception as e:
        conn.rollback()
        cur.close(); conn.close()
        raise HTTPException(400, f"Erro ao inserir hub: {e}")
    cur.close(); conn.close()
    return HubOut(hub_id=row[0], nome=row[1], cidade=row[2], latitude=row[3], longitude=row[4])

@router.put("/hubs/{hub_id}", response_model=HubOut, summary="Atualizar hub existente")
def atualizar_hub(hub_id: int, payload: HubIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("""
        UPDATE hubs
        SET nome=%s, cidade=%s, latitude=%s, longitude=%s
        WHERE hub_id=%s AND tenant_id=%s
        RETURNING hub_id, nome, cidade, latitude, longitude
    """, (payload.nome, payload.cidade, payload.latitude, payload.longitude, hub_id, tenant_id))
    row = cur.fetchone()
    conn.commit(); cur.close(); conn.close()
    if not row:
        raise HTTPException(404, "Hub não encontrado")
    return HubOut(hub_id=row[0], nome=row[1], cidade=row[2], latitude=row[3], longitude=row[4])

@router.delete("/hubs/{hub_id}", summary="Remover hub")
def excluir_hub(hub_id: int, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("DELETE FROM hubs WHERE hub_id=%s AND tenant_id=%s", (hub_id, tenant_id))
    deleted = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    if not deleted:
        raise HTTPException(404, "Hub não encontrado")
    return {"deleted": True}

# ================================
# CRUD Cluster Costs (1 por tenant)
# ================================
@router.get("/cluster_costs", response_model=ClusterCostOut, summary="Obter custos do tenant")
def obter_costs(tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("""
        SELECT id, limite_qtd_entregas, custo_fixo_diario, custo_variavel_por_entrega
        FROM cluster_costs WHERE tenant_id=%s
    """, (tenant_id,))
    row = cur.fetchone()
    cur.close(); conn.close()

    if not row:
        # from fastapi import Response
        # return Response(status_code=204)  # se quiser 204
        raise HTTPException(404, "Nenhum custo cadastrado para este tenant")  # ou mantém 404

    return ClusterCostOut(
        id=row[0],
        limite_qtd_entregas=row[1],
        custo_fixo_diario=float(row[2]),
        custo_variavel_por_entrega=float(row[3])
    )

@router.post("/cluster_costs", response_model=ClusterCostOut, summary="Criar ou atualizar custos do tenant")
def upsert_costs(payload: ClusterCostIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("""
        INSERT INTO cluster_costs (tenant_id, limite_qtd_entregas, custo_fixo_diario, custo_variavel_por_entrega)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (tenant_id) DO UPDATE
        SET limite_qtd_entregas=EXCLUDED.limite_qtd_entregas,
            custo_fixo_diario=EXCLUDED.custo_fixo_diario,
            custo_variavel_por_entrega=EXCLUDED.custo_variavel_por_entrega
        RETURNING id, limite_qtd_entregas, custo_fixo_diario, custo_variavel_por_entrega
    """, (tenant_id, payload.limite_qtd_entregas, payload.custo_fixo_diario, payload.custo_variavel_por_entrega))
    row = cur.fetchone()
    conn.commit(); cur.close(); conn.close()
    return ClusterCostOut(id=row[0], limite_qtd_entregas=row[1],
                          custo_fixo_diario=float(row[2]), custo_variavel_por_entrega=float(row[3]))

@router.delete("/cluster_costs/{id}", summary="Remover custo específico do tenant")
def excluir_cost(id: int, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("DELETE FROM cluster_costs WHERE id=%s AND tenant_id=%s", (id, tenant_id))
    deleted = cur.rowcount
    conn.commit(); cur.close(); conn.close()
    if not deleted:
        raise HTTPException(404, "Custo não encontrado")
    return {"deleted": True}


@router.get(
    "/cluster_costs/list",
    response_model=List[ClusterCostOut],
    summary="Listar custos do tenant (array)"
)
def listar_costs(tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("""
        SELECT id, limite_qtd_entregas, custo_fixo_diario, custo_variavel_por_entrega
        FROM cluster_costs
        WHERE tenant_id=%s
        ORDER BY id DESC
    """, (tenant_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()

    return [
        ClusterCostOut(
            id=r[0],
            limite_qtd_entregas=r[1],
            custo_fixo_diario=float(r[2]),
            custo_variavel_por_entrega=float(r[3]),
        )
        for r in rows
    ]