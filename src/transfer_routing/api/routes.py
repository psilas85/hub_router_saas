# src/transfer_routing/api/routes.py

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from datetime import date
from decimal import Decimal
from typing import Optional
import os
from fastapi.responses import FileResponse

from authentication.utils.dependencies import get_current_user
from authentication.domain.entities import UsuarioToken
from transfer_routing.application.transfer_routing_use_case import TransferRoutingUseCase
from transfer_routing.logs.logging_factory import LoggerFactory

# Imports das funções de visualização
from transfer_routing.visualization.route_plotter import gerar_mapa
from transfer_routing.visualization.mapa_estatico import gerar_mapa_estatico_transferencias
from transfer_routing.visualization.gerador_relatorio_transferencias import gerar_relatorio_transferencias
from transfer_routing.infrastructure.database_connection import (
    conectar_banco_cluster,
    conectar_banco_routing,
    fechar_conexao,
)

router = APIRouter(tags=["Transfer Routing"])
logger = LoggerFactory.get_logger("transfer_routing")


def _json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _carregar_resumo_artefatos_transferencias(tenant_id: str, data_inicial: date, data_final: date):
    conn = conectar_banco_routing()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    rota_transf,
                    tipo_veiculo,
                    quantidade_entregas,
                    clusters_qde,
                    volumes_total,
                    peso_total_kg,
                    cte_peso,
                    cte_valor_nf,
                    cte_valor_frete,
                    distancia_ida_km,
                    distancia_total_km,
                    tempo_ida_min,
                    tempo_total_min,
                    tempo_paradas,
                    tempo_descarga,
                    aproveitamento_percentual
                FROM transferencias_resumo
                WHERE tenant_id = %s
                  AND envio_data BETWEEN %s AND %s
                ORDER BY
                    CASE WHEN rota_transf = 'HUB' THEN 1 ELSE 0 END,
                    rota_transf
                """,
                (tenant_id, data_inicial, data_final),
            )
            columns = [desc[0] for desc in cur.description]
            rotas = [
                {
                    **{col: _json_safe(value) for col, value in zip(columns, row)},
                    "is_hub_central": str(row[0]) == "HUB",
                }
                for row in cur.fetchall()
            ]
    finally:
        fechar_conexao(conn)

    rotas_transferencia = [r for r in rotas if not r.get("is_hub_central")]
    rotas_hub = [r for r in rotas if r.get("is_hub_central")]

    def _totais(itens):
        return {
            "rotas": len(itens),
            "entregas": sum(float(r.get("quantidade_entregas") or 0) for r in itens),
            "paradas": sum(float(r.get("clusters_qde") or 0) for r in itens),
            "volumes": sum(float(r.get("volumes_total") or 0) for r in itens),
            "peso_total_kg": sum(float(r.get("peso_total_kg") or r.get("cte_peso") or 0) for r in itens),
            "valor_nf": sum(float(r.get("cte_valor_nf") or 0) for r in itens),
            "valor_frete": sum(float(r.get("cte_valor_frete") or 0) for r in itens),
            "distancia_ida_km": sum(float(r.get("distancia_ida_km") or 0) for r in itens),
            "distancia_total_km": sum(float(r.get("distancia_total_km") or 0) for r in itens),
            "tempo_ida_min": sum(float(r.get("tempo_ida_min") or 0) for r in itens),
            "tempo_total_min": sum(float(r.get("tempo_total_min") or 0) for r in itens),
        }

    totais = {
        **_totais(rotas),
        "rotas_transferencia": len(rotas_transferencia),
    }
    return {
        "totais": totais,
        "totais_transferencia": _totais(rotas_transferencia),
        "hub_central": _totais(rotas_hub),
        "rotas": rotas,
    }


@router.get("/health", summary="Health Check", tags=["Monitoramento"])
def healthcheck():
    return {"status": "ok", "service": "transfer_routing"}


@router.get("/transferencias/clusterizacoes-disponiveis", summary="Listar clusterizações disponíveis para transferência", tags=["Transferências"])
def listar_clusterizacoes_disponiveis(
    limit: int = Query(30, ge=1, le=365, description="Quantidade máxima de datas retornadas"),
    offset: int = Query(0, ge=0, description="Quantidade de datas ignoradas para paginação"),
    data_inicio: Optional[date] = Query(None, description="Filtrar datas a partir de YYYY-MM-DD"),
    data_fim: Optional[date] = Query(None, description="Filtrar datas até YYYY-MM-DD"),
    usuario: UsuarioToken = Depends(get_current_user),
):
    if data_inicio and data_fim and data_fim < data_inicio:
        raise HTTPException(status_code=400, detail="Data final não pode ser anterior à data inicial")

    tenant_id = usuario.tenant_id
    conn_cluster = conectar_banco_cluster()
    conn_routing = conectar_banco_routing()

    try:
        filtros = ["ec.tenant_id = %s"]
        params = [tenant_id]
        if data_inicio:
            filtros.append("ec.envio_data >= %s")
            params.append(data_inicio)
        if data_fim:
            filtros.append("ec.envio_data <= %s")
            params.append(data_fim)

        where_sql = " AND ".join(filtros)
        with conn_cluster.cursor() as cur:
            cur.execute(
                f"""
                SELECT ec.envio_data,
                       COUNT(DISTINCT ec.cte_numero) AS quantidade_entregas,
                       COUNT(DISTINCT ec.cluster) AS total_clusters,
                       COUNT(DISTINCT CASE WHEN CAST(ec.cluster AS TEXT) NOT LIKE '9999%%' THEN ec.cluster END) AS clusters_transferiveis,
                       COALESCE(SUM(e.cte_peso), 0) AS peso_total_kg,
                       COALESCE(SUM(e.cte_volumes), 0) AS volumes_total
                FROM entregas_clusterizadas ec
                JOIN entregas e
                  ON ec.cte_numero = e.cte_numero
                 AND ec.transportadora = e.transportadora
                 AND ec.tenant_id = e.tenant_id
                 AND ec.envio_data = e.envio_data
                WHERE {where_sql}
                GROUP BY ec.envio_data
                ORDER BY ec.envio_data DESC
                LIMIT %s OFFSET %s
                """,
                tuple(params + [limit + 1, offset]),
            )
            rows = cur.fetchall()

        has_more = len(rows) > limit
        rows = rows[:limit]
        datas = []

        for row in rows:
            envio_data = row[0]
            with conn_routing.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM transferencias_resumo
                    WHERE tenant_id = %s AND envio_data = %s
                    """,
                    (tenant_id, envio_data),
                )
                rotas_processadas = int(cur.fetchone()[0] or 0)

            datas.append({
                "data": str(envio_data),
                "quantidade_entregas": int(row[1] or 0),
                "total_clusters": int(row[2] or 0),
                "clusters_transferiveis": int(row[3] or 0),
                "peso_total_kg": float(row[4] or 0.0),
                "volumes_total": int(row[5] or 0),
                "roteirizacao_existente": rotas_processadas > 0,
                "rotas_processadas": rotas_processadas,
            })

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "clusterizacoes": datas,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "has_more": has_more,
                "next_offset": offset + limit if has_more else None,
            },
        }
    finally:
        fechar_conexao(conn_cluster)
        fechar_conexao(conn_routing)


@router.post("/transferencias", summary="Processar Transferências", tags=["Transferências"])
def executar_transferencias(
    envio_data: date = Query(..., description="Data do envio no formato YYYY-MM-DD"),
    modo_forcar: bool = Query(False, description="Forçar sobrescrita de dados existentes"),
    tempo_maximo: float = Query(1200.0, description="Tempo máximo da rota (minutos)"),
    tempo_parada_leve: float = Query(10.0, description="Tempo de parada leve (peso ≤ peso-leve-max)"),
    peso_leve_max: float = Query(50.0, description="Limite de peso (kg) para considerar parada leve"),
    tempo_parada_pesada: float = Query(20.0, description="Tempo de parada pesada (peso > peso-leve-max)"),
    tempo_por_volume: float = Query(0.4, description="Tempo de descarregamento por volume (min/volume)"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    """
    Processa transferências para uma data específica (`envio_data`),
    aplicando os parâmetros de roteirização informados.
    O `tenant_id` é extraído do token JWT.
    """
    roteirizador = TransferRoutingUseCase(
        tenant_id=usuario.tenant_id,
        modo_forcar=modo_forcar,
        tempo_maximo=tempo_maximo,
        tempo_parada_leve=tempo_parada_leve,
        peso_leve_max=peso_leve_max,
        tempo_parada_pesada=tempo_parada_pesada,
        tempo_por_volume=tempo_por_volume
    )
    resultado = roteirizador.run(data_inicial=envio_data, data_final=envio_data)
    return resultado or {
        "status": "processed",
        "processed": True,
        "envio_data": str(envio_data),
        "mensagem": f"Transferências processadas com sucesso para {envio_data}.",
    }


@router.get("/transferencias/visualizacao", summary="Visualizar transferências", tags=["Transferências"])
def visualizar_transferencias(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_final: date = Query(None, description="Data final (YYYY-MM-DD, opcional)"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    """
    Gera mapa HTML, imagem PNG e relatório PDF consolidado
    para as transferências do período selecionado.
    O `tenant_id` é extraído do token JWT.
    """
    print("🔍 AUTH HEADER RECEBIDO:", request.headers.get("authorization"))

    tenant_id = usuario.tenant_id
    if not data_final:
        data_final = data_inicial

    conn = None

    try:
        # === Pastas organizadas por tenant ===
        output_base = "/app/output"
        tenant_base = os.path.join(output_base, tenant_id)

        maps_dir = os.path.join(tenant_base, "maps")
        graphs_dir = os.path.join(tenant_base, "graphs")
        relatorios_dir = os.path.join(tenant_base, "relatorios")

        os.makedirs(maps_dir, exist_ok=True)
        os.makedirs(graphs_dir, exist_ok=True)
        os.makedirs(relatorios_dir, exist_ok=True)

        # === Geração dos artefatos ===
        caminho_mapa_html = gerar_mapa(
            tenant_id, data_inicial, data_final, output_path=maps_dir
        )
        caminho_mapa_png = gerar_mapa_estatico_transferencias(
            tenant_id, data_inicial, data_final, output_path=maps_dir
        )

        # ✅ Conexão aberta explicitamente
        conn = conectar_banco_routing()

        caminho_pdf = gerar_relatorio_transferencias(
            tenant_id=tenant_id,
            envio_data=data_inicial,
            data_final=data_final,
            output_path=relatorios_dir,
            caminho_mapa_html=caminho_mapa_html,
            caminho_mapa_png=caminho_mapa_png,
            conn=conn,
            logger=logger
        )

        logger.info(f"✅ Relatório salvo no volume: {caminho_pdf}")

        return FileResponse(
            caminho_pdf,
            media_type="application/pdf",
            filename=os.path.basename(caminho_pdf)
        )

    except Exception as e:
        logger.error(f"❌ Erro ao gerar visualização: {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao gerar visualização: {e}")

    finally:
        if conn:
            fechar_conexao(conn)

@router.get("/transferencias/artefatos", summary="Links dos artefatos (HTML/PNG/PDF)", tags=["Transferências"])
def artefatos_transferencias(
    data_inicial: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_final: date = Query(None, description="Data final (YYYY-MM-DD, opcional)"),
    usuario: UsuarioToken = Depends(get_current_user)
):
    """
    Retorna links públicos (via Nginx do frontend em /exports/transfer_routing/...) dos artefatos já gerados:
    - mapa interativo (HTML)
    - mapa estático (PNG)
    - relatório consolidado (PDF)
    NÃO reprocessa: apenas encontra os arquivos existentes mais recentes.
    """
    import glob

    tenant_id = usuario.tenant_id
    if not data_final:
        data_final = data_inicial

    # Dentro do container do transfer_routing_service:
    # ./exports/transfer_routing (host) -> /app/output (container)
    base = "/app/output"
    maps_dir = os.path.join(base, tenant_id, "maps")
    rels_dir = os.path.join(base, tenant_id, "relatorios")

    if not os.path.isdir(maps_dir):
        raise HTTPException(status_code=404, detail="Mapas ainda não gerados para este período/tenant.")

    di = data_inicial.isoformat()
    df = data_final.isoformat() if data_final else None

    def _latest(patterns: list[str]) -> str | None:
        candidates = []
        for pat in patterns:
            for p in glob.glob(pat):
                try:
                    candidates.append((os.path.getmtime(p), p))
                except Exception:
                    pass
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    # 1) tenta com as datas no nome; 2) cai para "apenas inicial"; 3) pega o mais recente.
    html_path = _latest([
        os.path.join(maps_dir, f"*{di}*{df or ''}*.html"),
        os.path.join(maps_dir, f"*{di}*.html"),
        os.path.join(maps_dir, "*.html"),
    ])
    png_path = _latest([
        os.path.join(maps_dir, f"*{di}*{df or ''}*.png"),
        os.path.join(maps_dir, f"*{di}*.png"),
        os.path.join(maps_dir, "*.png"),
    ])
    pdf_path = None
    if os.path.isdir(rels_dir):
        pdf_path = _latest([
            os.path.join(rels_dir, f"*{di}*{df or ''}*.pdf"),
            os.path.join(rels_dir, f"*{di}*.pdf"),
            os.path.join(rels_dir, "*.pdf"),
        ])

    def _to_public_url(local_path: str | None) -> str | None:
        if not local_path:
            return None
        # Ex.: /app/output/<tenant>/maps/arquivo.html → /exports/transfer_routing/<tenant>/maps/arquivo.html
        rel = os.path.relpath(local_path, base)  # "<tenant>/maps/arquivo.ext"
        return f"/exports/transfer_routing/{rel}"

    return {
        "tenant_id": tenant_id,
        "data_inicial": di,
        "data_final": df,
        "map_html_url": _to_public_url(html_path),
        "map_png_url": _to_public_url(png_path),
        "pdf_url": _to_public_url(pdf_path),
        "resumo": _carregar_resumo_artefatos_transferencias(tenant_id, data_inicial, data_final),
    }
