# src/transfer_routing/api/routes.py

from fastapi import APIRouter, Depends, Query, HTTPException, Request
from datetime import date
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
from transfer_routing.infrastructure.database_connection import conectar_banco_routing, fechar_conexao

router = APIRouter(tags=["Transfer Routing"])
logger = LoggerFactory.get_logger("transfer_routing")


@router.get("/health", summary="Health Check", tags=["Monitoramento"])
def healthcheck():
    return {"status": "ok", "service": "transfer_routing"}


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
    roteirizador.run(data_inicial=envio_data, data_final=envio_data)
    return {"mensagem": f"✅ Transferências processadas com sucesso para {envio_data}"}


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
    }
