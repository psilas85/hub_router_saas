#hub_router_1.0.1/src/simulation/api/routes.py

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from datetime import date
import logging
import uuid
import os

from pydantic import BaseModel
from typing import List
from rq import Queue
from redis import Redis
from rq.job import Job
import json

from simulation.jobs import SIMULATION_JOBS_QUEUE, processar_simulacao
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
from simulation.domain.entities import SimulationParams

router = APIRouter(prefix="/simulation", tags=["Simulation"])

# 🔌 Conexão com Redis para fila de jobs
redis_conn = Redis(host="redis", port=6379, decode_responses=True)
q = Queue(SIMULATION_JOBS_QUEUE, connection=redis_conn)

logger = logging.getLogger("simulation_service")
logger.setLevel(logging.INFO)


def _serializar_log(payload: dict) -> str:
    import json

    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _parse_optional_float(value: str | float | None, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=f"{field_name} deve ser numérico.") from exc


def _calcular_timeout_simulacao(data_inicial: date, data_final: date, modo_forcar: bool) -> int:
    total_dias = (data_final - data_inicial).days + 1
    timeout_por_data = 7200 if modo_forcar else 3600
    return max(timeout_por_data, total_dias * timeout_por_data)

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

class LastMileVehicleIn(BaseModel):
    veiculo: str
    capacidade_kg_min: float
    capacidade_kg_max: float

class TransferVehicleIn(BaseModel):
    veiculo: str
    capacidade_kg_min: float
    capacidade_kg_max: float



@router.get("/health", summary="Health Check", description="Verifica se o serviço de simulação está online.")
def healthcheck():
    return {"status": "ok", "servico": "Simulation"}




@router.post("/executar", summary="Executar Simulação Completa")
def executar_simulacao(
    payload: SimulationParams = Body(...),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    job_id = str(uuid.uuid4())

    params = payload

    # 🔥 validação de datas
    if params.data_final is None:
        params.data_final = params.data_inicial

    if params.data_final < params.data_inicial:
        raise HTTPException(
            status_code=400,
            detail="Data final não pode ser anterior à inicial."
        )

    logger.info(
        "[simulation.executar] job_id=%s tenant_id=%s params=%s",
        job_id,
        tenant_id,
        _serializar_log(params.dict()),
    )

    timeout = _calcular_timeout_simulacao(
        params.data_inicial,
        params.data_final,
        params.modo_forcar,
    )

    q.enqueue(
        processar_simulacao,
        job_id,
        tenant_id,
        str(params.data_inicial),
        str(params.data_final),
        params.hub_id,
        params.dict(),
        params.modo_forcar,
        job_id=job_id,
        job_timeout=timeout
    )

    # histórico
    try:
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
            f"Simulação de {params.data_inicial} a {params.data_final} enfileirada",
            json.dumps({
                "data_inicial": str(params.data_inicial),
                "data_final": str(params.data_final)
            }),
            json.dumps(params.dict(), default=str)
        ))

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"❌ Erro ao registrar histórico: {e}")

    return {
        "status": "processing",
        "job_id": job_id,
        "tenant_id": tenant_id,
    }

@router.get("/visualizar")
def visualizar_simulacao(
    data: date = Query(...),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    data_str = str(data)

    base_path = f"exports/simulation/{tenant_id}/{data_str}"
    entregas_path = os.path.join(base_path, "tables")

    if not os.path.isdir(base_path):
        raise HTTPException(404, "Nenhum artefato encontrado")

    response = {
        "data": data_str,
        "cenarios": {},
        "graficos": [],
        "relatorio_pdf": None,
        "excel_entregas_rotas": None,
    }

    # ====================
    # 📊 GRÁFICOS
    # ====================
    graphs_dir = os.path.join(base_path, "graphs")
    if os.path.isdir(graphs_dir):
        response["graficos"] = sorted([
            f"/exports/simulation/{tenant_id}/{data_str}/graphs/{f}"
            for f in os.listdir(graphs_dir)
            if f.endswith(".png")
        ])

    # ====================
    # 📄 PDF
    # ====================
    reports_dir = os.path.join(base_path, "reports")
    if os.path.isdir(reports_dir):
        pdfs = sorted([f for f in os.listdir(reports_dir) if f.endswith(".pdf")])
        if pdfs:
            response["relatorio_pdf"] = f"/exports/simulation/{tenant_id}/{data_str}/reports/{pdfs[-1]}"

    # ====================
    # 📦 EXCEL
    # ====================
    tables_dir = os.path.join(base_path, "tables")

    if os.path.isdir(tables_dir):
        excels = sorted([
            f for f in os.listdir(tables_dir)
            if f.endswith(".xlsx")
        ])
        if excels:
            response["excel_entregas_rotas"] = f"/exports/simulation/{tenant_id}/{data_str}/tables/{excels[-1]}"

    # ====================
    # 🗺️ MAPAS (POR K)
    # ====================
    maps_dir = os.path.join(base_path, "maps")

    if os.path.isdir(maps_dir):
        for f in sorted(os.listdir(maps_dir)):

            nome = f.lower()

            if not nome.endswith(".html"):
                continue

            if "_k" not in nome:
                continue

            k = nome.split("_k")[-1].split(".")[0]

            response["cenarios"].setdefault(k, {
                "mapas": [],
                "tabelas_lastmile": [],
                "tabelas_transferencias": [],
                "tabelas_resumo": [],
                "tabelas_detalhes": [],
                "otimo": False
            })

            path = f"/exports/simulation/{tenant_id}/{data_str}/maps/{f}"

            if "mapa_clusterizacao" in nome:
                tipo = "clusterizacao"
            elif "mapa_transfer" in nome:
                tipo = "transfer"
            elif "mapa_lastmile" in nome:
                tipo = "lastmile"
            else:
                continue

            response["cenarios"][k].setdefault("mapas_dict", {})
            response["cenarios"][k]["mapas_dict"][tipo] = path

    # ====================
    # 📊 TABELAS (POR K)
    # ====================
    tables_dir = os.path.join(base_path, "tables")

    if os.path.isdir(tables_dir):
        for f in sorted(os.listdir(tables_dir)):

            nome = f.lower()

            if not nome.endswith(".csv") or "_k" not in nome:
                continue

            k = nome.split("_k")[-1].split(".")[0]

            response["cenarios"].setdefault(k, {
                "mapas": [],
                "tabelas_lastmile": [],
                "tabelas_transferencias": [],
                "tabelas_resumo": [],
                "tabelas_detalhes": [],
                "otimo": False
            })

            path = f"/exports/simulation/{tenant_id}/{data_str}/tables/{f}"

            if "lastmile" in nome:
                response["cenarios"][k]["tabelas_lastmile"].append(path)
            elif "transfer" in nome:
                response["cenarios"][k]["tabelas_transferencias"].append(path)
            elif "resumo" in nome:
                response["cenarios"][k]["tabelas_resumo"].append(path)
            elif "detalhe" in nome:
                response["cenarios"][k]["tabelas_detalhes"].append(path)

    # ====================
    # 📌 ORDENAR MAPAS
    # ====================
    ordem = ["clusterizacao", "transfer", "lastmile"]

    for k in response["cenarios"]:
        mapas_dict = response["cenarios"][k].get("mapas_dict", {})

        response["cenarios"][k]["mapas"] = [
            mapas_dict[t] for t in ordem if t in mapas_dict
        ]

        if "mapas_dict" in response["cenarios"][k]:
            del response["cenarios"][k]["mapas_dict"]

    # ====================
    # 🏆 MARCAR CENÁRIO ÓTIMO
    # ====================
    try:
        conn = conectar_simulation_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT k_clusters
            FROM resultados_simulacao
            WHERE tenant_id = %s
            AND envio_data = %s
            AND is_ponto_otimo = TRUE
            LIMIT 1
        """, (tenant_id, data_str))

        row = cur.fetchone()

        if row:
            k_otimo = str(row[0])
            if k_otimo in response["cenarios"]:
                response["cenarios"][k_otimo]["otimo"] = True

        cur.close()
        conn.close()

    except Exception as e:
        logger.warning(f"⚠️ Falha ao buscar ponto ótimo: {e}")

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


@router.get(
    "/k_fixo",
    summary="Comparativo de custos consolidados por cenário",
)
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

    # 🚫 nunca retorna transferências quando k=0 (Hub único)
    if k == 0:
        transfer = []
        csv_transfer = None

    if not lastmile and not transfer:
        raise HTTPException(
            status_code=404,
            detail="Nenhuma frota encontrada para este período e k informado."
        )

    escopo = "hub_unico" if k == 0 else "k_fixo"
    transfer_aplicavel = k != 0
    mensagem_transfer = None if transfer_aplicavel else "Transferências não se aplicam ao cenário Hub único."

    return {
        "status": "ok",
        "tenant_id": tenant_id,
        "data_inicial": str(data_inicial),
        "data_final": str(data_final),
        "k_consultado": k,
        "escopo": escopo,
        "transfer_aplicavel": transfer_aplicavel,
        "mensagem_transfer": mensagem_transfer,
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

    def extrair_mensagem_execucao(exc_info, fallback="Erro não identificado"):
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

    # 1. Primeiro tenta buscar no Redis
    try:
        job = Job.fetch(job_id, connection=redis_conn)

        if job.is_finished:
            result = job.result if isinstance(job.result, dict) else None
            if result and result.get("status") == "error":
                return {
                    "status": "error",
                    "job_id": job.get_id(),
                    "tenant_id": tenant_id,
                    "result": result,
                }

            return {
                "status": "done",   # ✅ padronizado
                "job_id": job.get_id(),
                "tenant_id": tenant_id,
                "result": result if result else {
                    "mensagem": "✅ Simulação finalizada"
                },
            }
        elif job.is_failed:
            return {
                "status": "error",  # ✅ padronizado
                "job_id": job.get_id(),
                "tenant_id": tenant_id,
                "error": extrair_mensagem_execucao(job.exc_info),
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

    return {
        "status": "processing",
        "job_id": job_id,
        "mensagem": "Inicializando..."
    }

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

# ================================
# CRUD Tarifas Veículos
# ================================
from simulation.infrastructure.simulation_database_writer import (
    inserir_ou_atualizar_tarifa_last_mile,
    remover_tarifa_last_mile,
    inserir_ou_atualizar_tarifa_transferencia,
    remover_tarifa_transferencia,
)
from simulation.infrastructure.simulation_database_reader import (
    listar_tarifas_last_mile,
    listar_tarifas_transferencia,
)

class TarifaLastMileIn(BaseModel):
    veiculo: str
    capacidade_kg_min: float
    capacidade_kg_max: float
    tarifa_km: float
    tarifa_entrega: float

class TarifaTransferenciaIn(BaseModel):
    veiculo: str
    capacidade_kg_min: float
    capacidade_kg_max: float
    tarifa_km: float
    tarifa_fixa: float


# ---------- Last-mile ----------
@router.get("/tarifas/lastmile", summary="Listar tarifas last-mile")
def listar_tarifas_lm(tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db()
    try:
        df = listar_tarifas_last_mile(conn, tenant_id)
        return df.to_dict(orient="records")
    finally:
        conn.close()


@router.post("/tarifas/lastmile", summary="Inserir ou atualizar tarifa last-mile")
def upsert_tarifa_lm(payload: TarifaLastMileIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db()
    try:
        inserir_ou_atualizar_tarifa_last_mile(
            conn,
            tenant_id,
            payload.veiculo.strip(),
            payload.capacidade_kg_min,
            payload.capacidade_kg_max,
            payload.tarifa_km,
            payload.tarifa_entrega,
        )
        conn.commit()
        return {"status": "ok", "veiculo": payload.veiculo}
    finally:
        conn.close()


@router.delete("/tarifas/lastmile/{veiculo:path}", summary="Remover tarifa last-mile")
def remover_tarifa_lm(veiculo: str, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db()
    try:
        sucesso = remover_tarifa_last_mile(conn, veiculo)
        conn.commit()
        if not sucesso:
            raise HTTPException(status_code=404, detail=f"Veículo '{veiculo}' não encontrado")
        return {"deleted": True, "veiculo": veiculo}
    finally:
        conn.close()


# ---------- Transferência ----------
@router.get("/tarifas/transferencia", summary="Listar tarifas de transferência")
def listar_tarifas_transf(tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db()
    try:
        df = listar_tarifas_transferencia(conn, tenant_id)
        return df.to_dict(orient="records")
    finally:
        conn.close()


@router.post("/tarifas/transferencia", summary="Inserir ou atualizar tarifa de transferência")
def upsert_tarifa_transf(payload: TarifaTransferenciaIn, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db()
    try:
        inserir_ou_atualizar_tarifa_transferencia(
            conn,
            tenant_id,
            payload.veiculo.strip(),
            payload.capacidade_kg_min,
            payload.capacidade_kg_max,
            payload.tarifa_km,
            payload.tarifa_fixa,
        )
        conn.commit()
        return {"status": "ok", "veiculo": payload.veiculo}
    finally:
        conn.close()


@router.delete("/tarifas/transferencia/{veiculo:path}", summary="Remover tarifa de transferência")
def remover_tarifa_transf(veiculo: str, tenant_id: str = Depends(obter_tenant_id_do_token)):
    conn = conectar_simulation_db()
    try:
        sucesso = remover_tarifa_transferencia(conn, veiculo)
        conn.commit()
        if not sucesso:
            raise HTTPException(status_code=404, detail=f"Veículo '{veiculo}' não encontrado")
        return {"deleted": True, "veiculo": veiculo}
    finally:
        conn.close()

## Removido endpoint especial de download do Excel. Agora o arquivo é exposto como estático, igual aos demais artefatos.
