# simulation/api/routes.py

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from datetime import date, timedelta
import logging
import uuid
import os
from pydantic import BaseModel
from typing import List
from rq import Queue
from redis import Redis
from rq.job import Job
from rq import get_current_job

from simulation.jobs import processar_simulacao
from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.infrastructure.simulation_database_reader import carregar_historico_simulation
from authentication.utils.dependencies import obter_tenant_id_do_token

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

router = APIRouter(prefix="/simulation", tags=["Simulation"])

# 🔌 Conexão com Redis para fila de jobs
redis_conn = Redis(host="redis", port=6379, decode_responses=True)
q = Queue("simulation", connection=redis_conn)

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
    data_inicial: date = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final no formato YYYY-MM-DD"),
    modo_forcar: bool = Query(False, description="Sobrescreve simulações existentes"),
    hub_id: int = Query(..., description="ID do hub central"),
    k_min: int = Query(2, description="Valor mínimo de k_clusters"),
    k_max: int = Query(50, description="Valor máximo de k_clusters"),
    k_inicial_transferencia: int = Query(1, description="K inicial para clusterização de transferências"),
    min_entregas_cluster: int = Query(25, description="Qtd mínima de entregas por cluster"),
    fundir_clusters_pequenos: bool = Query(False, description="Funde clusters pequenos"),
    desativar_cluster_hub: bool = Query(False, description="Desativa cluster automático próximo ao hub central"),
    raio_hub_km: float = Query(80.0, description="Raio em km para considerar entregas no cluster do hub"),
    parada_leve: int = Query(10, description="Tempo de parada leve (min)"),
    parada_pesada: int = Query(20, description="Tempo de parada pesada (min)"),
    tempo_volume: float = Query(0.4, description="Tempo por volume (min)"),
    velocidade: float = Query(60.0, description="Velocidade média (km/h)"),
    limite_peso: float = Query(50.0, description="Limite de peso para considerar parada pesada (kg)"),
    peso_leve_max: float = Query(50.0, description="Peso máximo para veículo leve"),
    tempo_max_transferencia: int = Query(1200, description="Tempo máximo por rota de transferência (min)"),
    peso_max_transferencia: float = Query(15000.0, description="Peso máximo por rota de transferência (kg)"),
    entregas_por_subcluster: int = Query(25, description="Qtd alvo de entregas por subcluster"),
    tempo_max_roteirizacao: int = Query(1200, description="Tempo máximo total por rota last-mile (min)"),
    tempo_max_k1: int = Query(2400, description="Tempo máximo para k=1"),
    permitir_rotas_excedentes: bool = Query(False, description="Permitir rotas que ultrapassem limite"),
    restricao_veiculo_leve_municipio: bool = Query(False, description="Restringe veículos leves em rotas intermunicipais"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    if data_final < data_inicial:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial.")

    job_id = str(uuid.uuid4())

    # 🔹 Define timeout dinâmico
    timeout = 7200 if modo_forcar else 3600  # 2h se modo_forcar, senão 1h

    # 🔹 Enfileira execução no worker (sem duplicar job_id no enqueue!)
    job = q.enqueue(
        processar_simulacao,
        job_id,
        tenant_id,
        str(data_inicial),
        str(data_final),
        hub_id,
        {
            "k_min": k_min,
            "k_max": k_max,
            "k_inicial_transferencia": k_inicial_transferencia,
            "min_entregas_cluster": min_entregas_cluster,
            "fundir_clusters_pequenos": fundir_clusters_pequenos,
            "desativar_cluster_hub": desativar_cluster_hub,
            "raio_hub_km": raio_hub_km,
            "parada_leve": parada_leve,
            "parada_pesada": parada_pesada,
            "tempo_volume": tempo_volume,
            "velocidade": velocidade,
            "limite_peso": limite_peso,
            "peso_leve_max": peso_leve_max,
            "tempo_max_transferencia": tempo_max_transferencia,
            "peso_max_transferencia": peso_max_transferencia,
            "entregas_por_subcluster": entregas_por_subcluster,
            "tempo_max_roteirizacao": tempo_max_roteirizacao,
            "tempo_max_k1": tempo_max_k1,
            "permitir_rotas_excedentes": permitir_rotas_excedentes,
            "restricao_veiculo_leve_municipio": restricao_veiculo_leve_municipio,
        },
        modo_forcar,
        job_timeout=timeout
    )

    # 🔹 Registra no histórico imediatamente
    try:
        import json
        conn = conectar_simulation_db()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO historico_simulation
                (tenant_id, job_id, status, mensagem, datas, parametros)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            tenant_id,
            job_id,
            "processing",
            f"Simulação de {data_inicial} a {data_final} enfileirada",
            json.dumps({"data_inicial": str(data_inicial), "data_final": str(data_final)}),
            json.dumps({"hub_id": hub_id, "k_min": k_min, "k_max": k_max})
        ))
        conn.commit()
        cur.close(); conn.close()
    except Exception as e:
        logger.error(f"❌ Erro ao registrar histórico inicial: {e}")

    return {
        "status": "processing",
        "job_id": job.get_id(),
        "tenant_id": tenant_id,
        "mensagem": f"🔄 Simulação de {data_inicial} a {data_final} enfileirada com sucesso."
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

@router.get("/k_fixo", summary="Comparativo de custos consolidados por k fixo")
def k_fixo(
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    min_cobertura_parcial: float = Query(
        0.70, description="Cobertura mínima exigida (ex: 0.70 = 70%)"
    ),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    png_path, csv_path, df_export = gerar_grafico_k_fixo(
        tenant_id=tenant_id,
        data_inicial=str(data_inicial),
        data_final=str(data_final),
        min_cobertura_parcial=min_cobertura_parcial
    )

    if df_export is None or df_export.empty:
        raise HTTPException(
            status_code=404,
            detail="Nenhum cenário encontrado para este período."
        )

    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "data_inicial": str(data_inicial),
        "data_final": str(data_final),
        "grafico": png_path.replace("./", "/") if png_path else None,
        "csv": csv_path.replace("./", "/") if csv_path else None,
        "cenarios": df_export.to_dict(orient="records"),
    }



@router.get("/frota_k_fixo", summary="Frota média sugerida para k fixo")
def frota_k_fixo(
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    k: int = Query(..., description="Valor de k_clusters fixo"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    csv_lastmile, csv_transfer, lastmile, transfer = gerar_grafico_frota_k_fixo(
        tenant_id=tenant_id,
        data_inicial=str(data_inicial),
        data_final=str(data_final),
        k_fixo=k
    )

    # 🚫 nunca retorna transferências quando k=1
    if k == 1:
        transfer = []
        csv_transfer = None

    if not lastmile and not transfer:
        raise HTTPException(
            status_code=404,
            detail="Nenhuma frota encontrada para este período e k informado."
        )

    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "data_inicial": str(data_inicial),
        "data_final": str(data_final),
        "csv_lastmile": csv_lastmile.replace("./", "/") if csv_lastmile else None,
        "csv_transfer": csv_transfer.replace("./", "/") if csv_transfer else None,
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

@router.get("/status/{job_id}", summary="Status do processamento da simulação")
def status_simulacao(job_id: str, tenant_id: str = Depends(obter_tenant_id_do_token)):
    """
    Retorna o status atual de um job de simulação, padronizado no mesmo formato do Data Input.
    """

    # 1. Primeiro tenta buscar no Redis
    try:
        job = Job.fetch(job_id, connection=redis_conn)

        if job.is_finished:
            return {
                "status": "done",   # ✅ padronizado
                "job_id": job.get_id(),
                "tenant_id": tenant_id,
                "result": job.result if isinstance(job.result, dict) else {
                    "mensagem": "✅ Simulação finalizada"
                },
            }
        elif job.is_failed:
            return {
                "status": "error",  # ✅ padronizado
                "job_id": job.get_id(),
                "tenant_id": tenant_id,
                "error": str(job.exc_info),
            }
        else:
            return {
                "status": "processing",
                "job_id": job.get_id(),
                "tenant_id": tenant_id,
                "progress": job.meta.get("progress", 0),
                "step": job.meta.get("step", "Em andamento"),
            }

    except Exception:
        pass  # se não achar no Redis, segue para o histórico

    # 2. Fallback no banco de histórico
    conn = conectar_simulation_db(); cur = conn.cursor()
    cur.execute("""
        SELECT status, mensagem, datas
        FROM historico_simulation
        WHERE job_id = %s AND tenant_id = %s
        ORDER BY criado_em DESC
        LIMIT 1
    """, (job_id, tenant_id))
    row = cur.fetchone()
    cur.close(); conn.close()

    if row:
        status_map = {"finished": "done", "failed": "error", "processing": "processing"}
        return {
            "status": status_map.get(row[0], row[0]),  # ✅ converte
            "job_id": job_id,
            "tenant_id": tenant_id,
            "mensagem": row[1],
            "datas_processadas": (
                list(row[2].values()) if isinstance(row[2], dict) else row[2]
            ),
        }

    raise HTTPException(status_code=404, detail="Job não encontrado no Redis nem no histórico.")

@router.get("/historico", summary="Histórico de simulações")
def listar_historico_simulation(
    limit: int = Query(10, description="Quantidade máxima de registros"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    db = conectar_simulation_db()
    try:
        df = carregar_historico_simulation(db, tenant_id, limit)
        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "historico": df.to_dict(orient="records"),
        }
    finally:
        db.close()