#hub_router_1.0.1/src/clusterization/api/routes.py
import os
import logging
import pandas as pd
from datetime import date, timedelta
from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional

from authentication.utils.dependencies import get_current_user
from authentication.domain.entities import UsuarioToken

from clusterization.infrastructure.db import Database
from clusterization.infrastructure.database_reader import DatabaseReader
from clusterization.infrastructure.database_writer import DatabaseWriter
from clusterization.domain.geolocalizacao_service import GeolocalizacaoService
from clusterization.domain.centro_urbano_service import CentroUrbanoService
from clusterization.domain.clustering_service import ClusteringService
from clusterization.application.clusterization_use_case import ClusterizationUseCase
from clusterization.config import UF_BOUNDS

from clusterization.visualization.main_visualization import carregar_dados_para_visualizacao
from clusterization.visualization.plot_clusterization import gerar_mapa_clusters, gerar_mapa_estatico
from clusterization.visualization.gerar_resumo_clusterizacao import gerar_graficos_resumo_clusterizacao
from clusterization.visualization.gerador_relatorio_clusterizacao import gerar_relatorio_clusterizacao

router = APIRouter(
    prefix="/cluster",
    tags=["Clusterização"]
)

logger = logging.getLogger("clusterization")


@router.post("/clusterizar", summary="Executar clusterização de entregas")
def clusterizar(
    data: date = Query(..., description="Data de envio (YYYY-MM-DD)"),
    data_final: Optional[date] = Query(None, description="(Opcional) Data final para intervalo"),
    k_min: int = Query(2, description="Número mínimo de clusters"),
    k_max: int = Query(50, description="Número máximo de clusters"),
    min_entregas_por_cluster: int = Query(25, description="Mínimo de entregas por cluster"),
    fundir_clusters_pequenos: bool = Query(False, description="Fundir clusters pequenos"),
    desativar_cluster_hub_central: bool = Query(False, description="Desativar cluster do hub central"),
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
                k_min=k_min,
                k_max=k_max,
                min_entregas_por_cluster=min_entregas_por_cluster,
                fundir_clusters_pequenos=fundir_clusters_pequenos,
                usar_cluster_hub_central=not desativar_cluster_hub_central,
                raio_cluster_hub_central_km=raio_cluster_hub_central,
                centro_service=centro_service
            )

            df_clusterizado, df_centros, df_outliers = use_case.executar(df_envio)

            if df_clusterizado["id_entrega"].isna().any():
                df_clusterizado = df_clusterizado[df_clusterizado["id_entrega"].notna()]

            df_clusterizado = centro_service.ajustar_centros(df_clusterizado)
            df_clusterizado["tenant_id"] = tenant_id

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
                "k_min": k_min,
                "k_max": k_max,
                "min_entregas_por_cluster": min_entregas_por_cluster,
                "fundir_clusters_pequenos": fundir_clusters_pequenos,
                "desativar_cluster_hub_central": desativar_cluster_hub_central,
                "raio_cluster_hub_central": raio_cluster_hub_central,
                "modo_forcar": True
            }
        }

    finally:
        db.fechar_conexao()


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

        os.makedirs(maps_dir, exist_ok=True)
        os.makedirs(graphs_dir, exist_ok=True)
        os.makedirs(relatorios_dir, exist_ok=True)

        # Gera arquivos
        caminho_mapa_html = gerar_mapa_clusters(df_clusterizado, data, tenant_id, output_path=maps_dir)
        caminho_mapa_png = gerar_mapa_estatico(df_clusterizado, data, tenant_id, output_path=maps_dir)
        caminhos_graficos = gerar_graficos_resumo_clusterizacao(df_resumo, data, tenant_id, output_path=graphs_dir)
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
            "pdf": f"{base_url}/relatorios/relatorio_clusterizacao.pdf"
        }

        return {
            "status": "ok",
            "tenant_id": tenant_id,
            "data": str(data),
            "arquivos": arquivos
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar visualização: {e}")
