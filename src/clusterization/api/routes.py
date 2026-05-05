#hub_router_1.0.1/src/clusterization/api/routes.py
import os
import logging
import uuid
import pandas as pd
from datetime import date, timedelta
from fastapi import APIRouter, Depends, Query, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from redis import Redis
from rq import Queue
from rq.job import Job

from authentication.utils.dependencies import get_current_user
from authentication.domain.entities import UsuarioToken

from clusterization.application.clusterization_runner import executar_clusterizacao_pipeline
from clusterization.jobs import CLUSTERIZATION_JOBS_QUEUE, processar_clusterizacao_job
from clusterization.infrastructure.db import Database
from clusterization.infrastructure.database_connection import conectar_banco_routing, fechar_conexao
from clusterization.infrastructure.database_reader import DatabaseReader
from clusterization.infrastructure.database_writer import DatabaseWriter
from clusterization.domain.geolocalizacao_service import GeolocalizacaoService
from clusterization.domain.centro_urbano_service import CentroUrbanoService
from clusterization.domain.clustering_service import ClusteringService
from clusterization.application.clusterization_use_case import ClusterizationUseCase
from clusterization.config import UF_BOUNDS

from clusterization.visualization.main_visualization import (
    carregar_dados_para_visualizacao,
    carregar_entregas_clusterizadas_para_excel,
    gerar_excel_entregas_clusterizadas,
)
from clusterization.visualization.plot_clusterization import gerar_mapa_clusters, gerar_mapa_estatico
from clusterization.visualization.gerar_resumo_clusterizacao import gerar_graficos_resumo_clusterizacao
from clusterization.visualization.gerador_relatorio_clusterizacao import gerar_relatorio_clusterizacao

router = APIRouter(
    prefix="/cluster",
    tags=["Clusterização"]
)

logger = logging.getLogger("clusterization")

redis_conn = Redis(host="redis", port=6379)
clusterization_queue = Queue(CLUSTERIZATION_JOBS_QUEUE, connection=redis_conn)


class HubCadastroIn(BaseModel):
    nome: str = Field(..., min_length=1, max_length=255)
    endereco: str = Field(..., min_length=1)
    latitude: float
    longitude: float
    hub_central: bool = False
    centro_cluster: bool = False
    ativo: bool = True


class HubCadastroOut(HubCadastroIn):
    id: int


class ClusterizationJobIn(BaseModel):
    data: date
    data_final: Optional[date] = None
    hub_central_id: int
    min_entregas_por_cluster_alvo: int = 10
    max_entregas_por_cluster_alvo: int = 100
    raio_cluster_hub_central: float = 80.0
    modo_clusterizacao: str = "automatico"
    centros_ids: Optional[List[int]] = None


def _ensure_hubs_schema(conn):
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE hubs_central ADD COLUMN IF NOT EXISTS hub_central BOOLEAN DEFAULT FALSE;")
        cur.execute("ALTER TABLE hubs_central ADD COLUMN IF NOT EXISTS centro_cluster BOOLEAN DEFAULT FALSE;")
        cur.execute("""
            WITH candidatos AS (
                SELECT DISTINCT ON (tenant_id) id, tenant_id
                FROM hubs_central h
                WHERE ativo = TRUE
                  AND hub_central IS DISTINCT FROM TRUE
                  AND NOT EXISTS (
                      SELECT 1
                      FROM hubs_central existente
                      WHERE existente.tenant_id = h.tenant_id
                        AND existente.hub_central = TRUE
                        AND existente.ativo = TRUE
                  )
                ORDER BY tenant_id, id
            )
            UPDATE hubs_central h
            SET hub_central = TRUE
            FROM candidatos
            WHERE h.id = candidatos.id;
        """)
        cur.execute("DROP INDEX IF EXISTS idx_hubs_central_tenant;")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_hubs_central_tenant_lookup
            ON hubs_central (tenant_id);
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_hubs_central_unico_ativo
            ON hubs_central (tenant_id)
            WHERE hub_central = TRUE AND ativo = TRUE;
        """)
    conn.commit()


def _row_to_hub(row) -> HubCadastroOut:
    return HubCadastroOut(
        id=row[0],
        nome=row[1],
        endereco=row[2],
        latitude=row[3],
        longitude=row[4],
        hub_central=bool(row[5]),
        centro_cluster=bool(row[6]),
        ativo=bool(row[7]),
    )


@router.get("/hubs", response_model=List[HubCadastroOut], summary="Listar hubs de clusterização")
def listar_hubs_clusterization(usuario: UsuarioToken = Depends(get_current_user)):
    conn = conectar_banco_routing()
    try:
        _ensure_hubs_schema(conn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, hub_central_nome, endereco, hub_central_latitude,
                       hub_central_longitude, hub_central, centro_cluster, ativo
                FROM hubs_central
                WHERE tenant_id = %s
                ORDER BY hub_central DESC, centro_cluster DESC, id DESC;
            """, (usuario.tenant_id,))
            return [_row_to_hub(row) for row in cur.fetchall()]
    finally:
        fechar_conexao(conn)


@router.post("/hubs", response_model=HubCadastroOut, summary="Criar hub de clusterização")
def criar_hub_clusterization(payload: HubCadastroIn, usuario: UsuarioToken = Depends(get_current_user)):
    conn = conectar_banco_routing()
    try:
        _ensure_hubs_schema(conn)
        with conn.cursor() as cur:
            if payload.hub_central and payload.ativo:
                cur.execute("""
                    UPDATE hubs_central
                    SET hub_central = FALSE, atualizado_em = NOW()
                    WHERE tenant_id = %s AND hub_central = TRUE;
                """, (usuario.tenant_id,))

            cur.execute("""
                INSERT INTO hubs_central (
                    hub_central_nome, endereco, hub_central_latitude, hub_central_longitude,
                    tenant_id, ativo, hub_central, centro_cluster, criado_em, atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                RETURNING id, hub_central_nome, endereco, hub_central_latitude,
                          hub_central_longitude, hub_central, centro_cluster, ativo;
            """, (
                payload.nome,
                payload.endereco,
                payload.latitude,
                payload.longitude,
                usuario.tenant_id,
                payload.ativo,
                payload.hub_central,
                payload.centro_cluster,
            ))
            row = cur.fetchone()
        conn.commit()
        return _row_to_hub(row)
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao criar hub: {exc}")
    finally:
        fechar_conexao(conn)


@router.put("/hubs/{hub_id}", response_model=HubCadastroOut, summary="Atualizar hub de clusterização")
def atualizar_hub_clusterization(
    hub_id: int,
    payload: HubCadastroIn,
    usuario: UsuarioToken = Depends(get_current_user)
):
    conn = conectar_banco_routing()
    try:
        _ensure_hubs_schema(conn)
        with conn.cursor() as cur:
            if payload.hub_central and payload.ativo:
                cur.execute("""
                    UPDATE hubs_central
                    SET hub_central = FALSE, atualizado_em = NOW()
                    WHERE tenant_id = %s AND id <> %s AND hub_central = TRUE;
                """, (usuario.tenant_id, hub_id))

            cur.execute("""
                UPDATE hubs_central
                SET hub_central_nome = %s,
                    endereco = %s,
                    hub_central_latitude = %s,
                    hub_central_longitude = %s,
                    hub_central = %s,
                    centro_cluster = %s,
                    ativo = %s,
                    atualizado_em = NOW()
                WHERE id = %s AND tenant_id = %s
                RETURNING id, hub_central_nome, endereco, hub_central_latitude,
                          hub_central_longitude, hub_central, centro_cluster, ativo;
            """, (
                payload.nome,
                payload.endereco,
                payload.latitude,
                payload.longitude,
                payload.hub_central,
                payload.centro_cluster,
                payload.ativo,
                hub_id,
                usuario.tenant_id,
            ))
            row = cur.fetchone()
        conn.commit()
        if not row:
            raise HTTPException(status_code=404, detail="Hub não encontrado")
        return _row_to_hub(row)
    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao atualizar hub: {exc}")
    finally:
        fechar_conexao(conn)


@router.delete("/hubs/{hub_id}", summary="Excluir hub de clusterização")
def excluir_hub_clusterization(hub_id: int, usuario: UsuarioToken = Depends(get_current_user)):
    conn = conectar_banco_routing()
    try:
        _ensure_hubs_schema(conn)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM hubs_central WHERE id = %s AND tenant_id = %s", (hub_id, usuario.tenant_id))
            deleted = cur.rowcount
        conn.commit()
        if not deleted:
            raise HTTPException(status_code=404, detail="Hub não encontrado")
        return {"deleted": True}
    finally:
        fechar_conexao(conn)


@router.get("/datas-disponiveis", summary="Listar datas com entregas disponíveis")
def listar_datas_disponiveis(
    limit: int = Query(30, ge=1, le=365, description="Quantidade máxima de datas retornadas"),
    offset: int = Query(0, ge=0, description="Quantidade de datas ignoradas para paginação"),
    data_inicio: Optional[date] = Query(None, description="Filtrar datas a partir de YYYY-MM-DD"),
    data_fim: Optional[date] = Query(None, description="Filtrar datas até YYYY-MM-DD"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    if data_inicio and data_fim and data_fim < data_inicio:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial")

    tenant_id = usuario.tenant_id
    db = Database()
    db.conectar()

    try:
        df_datas = db.buscar_datas_disponiveis_por_tenant(
            tenant_id,
            limit=limit + 1,
            offset=offset,
            data_inicio=data_inicio,
            data_fim=data_fim,
        )
        has_more = len(df_datas) > limit
        df_datas = df_datas.head(limit)
        datas = [
            {
                "data": str(row["data"]),
                "quantidade_entregas": int(row["quantidade_entregas"]),
            }
            for _, row in df_datas.iterrows()
        ]

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "datas": datas,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "has_more": has_more,
                "next_offset": offset + limit if has_more else None,
            },
        }
    finally:
        db.fechar_conexao()


@router.post("/jobs", summary="Executar clusterização assíncrona")
def criar_job_clusterizacao(
    payload: ClusterizationJobIn,
    usuario: UsuarioToken = Depends(get_current_user),
):
    if payload.data_final and payload.data_final < payload.data:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial")
    if payload.modo_clusterizacao not in ("automatico", "predefinido"):
        raise HTTPException(status_code=400, detail="modo_clusterizacao deve ser 'automatico' ou 'predefinido'")
    if payload.modo_clusterizacao == "automatico":
        if payload.min_entregas_por_cluster_alvo < 1 or payload.max_entregas_por_cluster_alvo < 1:
            raise HTTPException(status_code=400, detail="Min/max de entregas por cluster devem ser maiores que zero")
        if payload.min_entregas_por_cluster_alvo > payload.max_entregas_por_cluster_alvo:
            raise HTTPException(status_code=400, detail="Mínimo de entregas não pode ser maior que o máximo")
    if payload.modo_clusterizacao == "predefinido" and not payload.centros_ids:
        raise HTTPException(status_code=400, detail="Selecione ao menos um centro pré-definido.")

    job_id = str(uuid.uuid4())
    job = clusterization_queue.enqueue(
        processar_clusterizacao_job,
        usuario.tenant_id,
        str(payload.data),
        str(payload.data_final) if payload.data_final else None,
        payload.hub_central_id,
        payload.min_entregas_por_cluster_alvo,
        payload.max_entregas_por_cluster_alvo,
        payload.raio_cluster_hub_central,
        payload.modo_clusterizacao,
        payload.centros_ids,
        job_id=job_id,
        job_timeout=3600,
        result_ttl=86400,
        failure_ttl=86400,
    )
    job.meta["progress"] = 0
    job.meta["step"] = "Enfileirado"
    job.save_meta()

    return {
        "status": "processing",
        "job_id": job.id,
        "progress": 0,
        "step": "Enfileirado",
    }


@router.get("/jobs/{job_id}", summary="Status da clusterização assíncrona")
def status_job_clusterizacao(
    job_id: str,
    usuario: UsuarioToken = Depends(get_current_user),
):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    status = job.get_status(refresh=True)
    if job.is_finished:
        result = job.result or job.meta.get("result")
        return {
            "status": "done",
            "job_id": job.id,
            "progress": 100,
            "step": "Concluído",
            "result": result,
        }

    if job.is_failed:
        return {
            "status": "error",
            "job_id": job.id,
            "progress": 100,
            "step": job.meta.get("step") or "Erro",
            "error": job.meta.get("error") or "Erro ao executar clusterização",
        }

    return {
        "status": "processing",
        "job_id": job.id,
        "progress": job.meta.get("progress", 0),
        "step": job.meta.get("step", "Processando"),
        "rq_status": status,
    }


@router.post("/clusterizar", summary="Executar clusterização de entregas")
def clusterizar(
    data: date = Query(..., description="Data de envio (YYYY-MM-DD)"),
    data_final: Optional[date] = Query(None, description="(Opcional) Data final para intervalo"),
    min_entregas_por_cluster_alvo: int = Query(10, description="Mínimo alvo de entregas por cluster"),
    max_entregas_por_cluster_alvo: int = Query(100, description="Máximo alvo de entregas por cluster"),
    min_entregas_por_cluster: Optional[int] = Query(
        None,
        description="Alias legado para min_entregas_por_cluster_alvo",
        deprecated=True
    ),
    k_min: Optional[int] = Query(None, description="Legado: ignorado no cálculo atual", deprecated=True),
    k_max: Optional[int] = Query(None, description="Legado: ignorado no cálculo atual", deprecated=True),
    fundir_clusters_pequenos: bool = Query(
        False,
        description="Legado: ignorado. O balanceamento por min/max ja funde clusters pequenos.",
        deprecated=True
    ),
    hub_central_id: int = Query(..., description="ID do Hub Central selecionado para a clusterização"),
    desativar_cluster_hub_central: bool = Query(
        False,
        description="Legado: ignorado. Hub Central agora é obrigatório.",
        deprecated=True
    ),
    raio_cluster_hub_central: float = Query(80.0, description="Raio (km) para cluster do hub central"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    tenant_id = usuario.tenant_id
    modo_forcar = True  # 🔒 sempre força sobrescrita

    # Se não vier data_final, assume igual à inicial
    if data_final is None:
        data_final = data

    if data_final < data:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial")

    datas_envio = [data + timedelta(days=i) for i in range((data_final - data).days + 1)]
    db = Database()
    db.conectar()

    try:
        conn = db.conexao
        reader = DatabaseReader(conn)
        writer = DatabaseWriter(conn)
        geo_service = GeolocalizacaoService(reader, writer)
        centro_service = CentroUrbanoService(reader)

        # ✅ Buscar entregas com base em data e data_final
        df_entregas = db.buscar_entregas_por_tenant(data, data_final, tenant_id)
        df_entregas["envio_data"] = pd.to_datetime(df_entregas["envio_data"]).dt.date

        if df_entregas.empty:
            raise HTTPException(status_code=404, detail="Nenhum dado encontrado para as datas informadas")

        for envio_data in datas_envio:
            if writer.existe_resumo_clusterizacao(envio_data, tenant_id):
                if modo_forcar:
                    writer.excluir_clusterizacao_por_data_tenant(envio_data, tenant_id)
                else:
                    logger.info(f"⚠️ Clusterização já existe para {envio_data}, use modo_forcar=True para sobrescrever.")
                    continue

            df_envio = df_entregas[df_entregas["envio_data"] == envio_data]
            df_envio["tenant_id"] = tenant_id

            use_case = ClusterizationUseCase(
                clustering_service=ClusteringService(UF_BOUNDS, random_state=42, max_clusters=15, logger=logger),
                min_entregas_por_cluster_alvo=(
                    min_entregas_por_cluster
                    if min_entregas_por_cluster is not None
                    else min_entregas_por_cluster_alvo
                ),
                max_entregas_por_cluster_alvo=max_entregas_por_cluster_alvo,
                usar_cluster_hub_central=True,
                hub_central_id=hub_central_id,
                raio_cluster_hub_central_km=raio_cluster_hub_central,
                centro_service=centro_service
            )

            try:
                df_clusterizado, df_centros, df_outliers = use_case.executar(df_envio)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))

            if df_clusterizado["id_entrega"].isna().any():
                df_clusterizado = df_clusterizado[df_clusterizado["id_entrega"].notna()]

            df_clusterizado = centro_service.ajustar_centros(df_clusterizado)
            df_clusterizado["tenant_id"] = tenant_id
            df_centros = df_clusterizado[
                ["cluster", "centro_lat", "centro_lon", "cluster_cidade"]
            ].drop_duplicates(subset=["cluster"])

            resumo = (
                df_clusterizado.groupby("cluster").agg(
                    quantidade_entregas=("id_entrega", "count"),
                    peso_total_kg=("cte_peso", "sum"),
                    quantidade_volumes=("cte_volumes", "sum"),
                    cte_valor_nf_total=("cte_valor_nf", "sum"),
                    cte_valor_frete_total=("cte_valor_frete", "sum")
                ).reset_index()
            )

            df_resumo = pd.merge(resumo, df_centros, on="cluster", how="left")
            df_resumo["distancia_media_km"] = 0
            df_resumo["tempo_estimado_min"] = 0
            df_resumo["cte_valor_nf_total"] = df_resumo["cte_valor_nf_total"].fillna(0)
            df_resumo["cte_valor_frete_total"] = df_resumo["cte_valor_frete_total"].fillna(0)

            writer.salvar_clusterizacao(df_clusterizado)
            writer.salvar_resumo_clusters(
                df_resumo.assign(envio_data=envio_data, tenant_id=tenant_id),
                envio_data,
                tenant_id
            )

        return {
            "status": "ok",
            "mensagem": f"✅ Clusterização finalizada para {len(datas_envio)} dia(s)",
            "datas": [str(d) for d in datas_envio],
            "parametros": {
                "min_entregas_por_cluster_alvo": (
                    min_entregas_por_cluster
                    if min_entregas_por_cluster is not None
                    else min_entregas_por_cluster_alvo
                ),
                "max_entregas_por_cluster_alvo": max_entregas_por_cluster_alvo,
                "hub_central_id": hub_central_id,
                "raio_cluster_hub_central": raio_cluster_hub_central,
                "modo_forcar": True
            }
        }

    finally:
        db.fechar_conexao()


@router.get("/resultado", summary="Resultado da clusterização em JSON")
def resultado_clusterizacao(
    data: date = Query(..., description="Data de envio (YYYY-MM-DD)"),
    usuario: UsuarioToken = Depends(get_current_user),
):
    tenant_id = usuario.tenant_id
    from clusterization.infrastructure.database_connection import conectar_banco
    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT rc.cluster,
                       (SELECT ec.cluster_cidade FROM entregas_clusterizadas ec
                        WHERE ec.tenant_id = rc.tenant_id AND ec.envio_data = rc.envio_data
                          AND ec.cluster = rc.cluster AND ec.cluster_cidade IS NOT NULL
                        LIMIT 1) AS cluster_cidade,
                       rc.centro_lat, rc.centro_lon,
                       rc.quantidade_entregas, rc.peso_total_kg, rc.quantidade_volumes,
                       rc.cte_valor_nf_total, rc.cte_valor_frete_total
                FROM resumo_clusterizacao rc
                WHERE rc.tenant_id = %s AND rc.envio_data = %s
                ORDER BY rc.quantidade_entregas DESC
            """, (tenant_id, data))
            cols = [d[0] for d in cur.description]
            clusters = [dict(zip(cols, row)) for row in cur.fetchall()]

        if not clusters:
            raise HTTPException(status_code=404, detail="Nenhuma clusterização encontrada para esta data.")

        with conn.cursor() as cur:
            cur.execute("""
                SELECT ec.cluster, e.destino_latitude AS lat, e.destino_longitude AS lon
                FROM entregas_clusterizadas ec
                JOIN entregas e
                  ON ec.cte_numero = e.cte_numero AND ec.transportadora = e.transportadora
                WHERE ec.tenant_id = %s AND ec.envio_data = %s
                  AND e.destino_latitude IS NOT NULL AND e.destino_longitude IS NOT NULL
            """, (tenant_id, data))
            pontos = [{"cluster": r[0], "lat": float(r[1]), "lon": float(r[2])} for r in cur.fetchall()]

        return {
            "data": str(data),
            "total_clusters": len(clusters),
            "total_entregas": sum(c["quantidade_entregas"] for c in clusters),
            "clusters": [
                {k: (float(v) if v is not None and k in ("centro_lat", "centro_lon", "peso_total_kg", "cte_valor_nf_total", "cte_valor_frete_total") else v)
                 for k, v in c.items()}
                for c in clusters
            ],
            "pontos": pontos,
        }
    finally:
        fechar_conexao(conn)


@router.get("/clusterizar/visualizacao", summary="Visualizar clusterização")
def visualizar_clusterizacao(
    data: date = Query(..., description="Data de envio (YYYY-MM-DD)"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    tenant_id = usuario.tenant_id

    try:
        df_clusterizado, df_resumo = carregar_dados_para_visualizacao(tenant_id, data)
        if df_clusterizado.empty or df_resumo.empty:
            raise HTTPException(status_code=404, detail="Dados não encontrados para visualização.")

        # ✅ Estrutura de saída no volume ./exports/clusterization
        BASE_OUTPUT_DIR = "/app/output"
        tenant_base = os.path.join(BASE_OUTPUT_DIR, tenant_id)

        maps_dir = os.path.join(tenant_base, "maps")
        graphs_dir = os.path.join(tenant_base, "graphs")
        relatorios_dir = os.path.join(tenant_base, "relatorios")
        planilhas_dir = os.path.join(tenant_base, "planilhas")

        os.makedirs(maps_dir, exist_ok=True)
        os.makedirs(graphs_dir, exist_ok=True)
        os.makedirs(relatorios_dir, exist_ok=True)
        os.makedirs(planilhas_dir, exist_ok=True)

        # Gera arquivos
        caminho_mapa_html = gerar_mapa_clusters(df_clusterizado, data, tenant_id, output_path=maps_dir)
        caminho_mapa_png = gerar_mapa_estatico(df_clusterizado, data, tenant_id, output_path=maps_dir)
        caminhos_graficos = gerar_graficos_resumo_clusterizacao(df_resumo, data, tenant_id, output_path=graphs_dir)
        df_excel = carregar_entregas_clusterizadas_para_excel(tenant_id, data)
        caminho_excel = gerar_excel_entregas_clusterizadas(
            df_excel,
            str(data),
            tenant_id,
            output_path=planilhas_dir,
        )
        caminho_pdf = gerar_relatorio_clusterizacao(
            caminho_mapa_html,
            caminhos_graficos,
            data,
            tenant_id,
            df_resumo,
            output_path=relatorios_dir,
            caminho_mapa_png=caminho_mapa_png
        )

        logger.info(f"✅ Relatórios e mapas salvos no volume: {tenant_base}")

        # 🔗 Caminhos relativos via Gateway (/exports/clusterization/...)
        base_url = f"/exports/clusterization/{tenant_id}"
        arquivos = {
            "mapa_html": f"{base_url}/maps/mapa_clusters.html",
            "mapa_png": f"{base_url}/maps/mapa_clusters.png",
            "pdf": f"{base_url}/relatorios/relatorio_clusterizacao.pdf",
            "xlsx": f"{base_url}/planilhas/{os.path.basename(caminho_excel)}"
        }

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "data": str(data),
            "arquivos": arquivos
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar visualização: {e}")
