from datetime import date, timedelta
from typing import Callable, Optional

import pandas as pd

from clusterization.application.clusterization_use_case import ClusterizationUseCase
from clusterization.config import UF_BOUNDS
from clusterization.domain.centro_urbano_service import CentroUrbanoService
from clusterization.domain.clustering_service import ClusteringService
from clusterization.domain.geolocalizacao_service import GeolocalizacaoService
from clusterization.infrastructure.database_reader import DatabaseReader
from clusterization.infrastructure.database_writer import DatabaseWriter
from clusterization.infrastructure.db import Database


ProgressCallback = Optional[Callable[[int, str, Optional[dict]], None]]


def executar_clusterizacao_pipeline(
    *,
    tenant_id: str,
    data: date,
    data_final: Optional[date],
    hub_central_id: int,
    min_entregas_por_cluster_alvo: int,
    max_entregas_por_cluster_alvo: int,
    raio_cluster_hub_central: float,
    logger=None,
    progress: ProgressCallback = None,
):
    def notify(percent: int, step: str, extra: Optional[dict] = None):
        if progress:
            progress(percent, step, extra)

    modo_forcar = True
    if data_final is None:
        data_final = data
    if data_final < data:
        raise ValueError("Data final não pode ser anterior à data inicial")

    datas_envio = [data + timedelta(days=i) for i in range((data_final - data).days + 1)]
    db = Database()
    db.conectar()

    try:
        notify(5, "Conectado ao banco de clusterização")
        conn = db.conexao
        reader = DatabaseReader(conn)
        writer = DatabaseWriter(conn)
        geo_service = GeolocalizacaoService(reader, writer)
        centro_service = CentroUrbanoService(reader)

        notify(15, "Buscando entregas")
        df_entregas = db.buscar_entregas_por_tenant(data, data_final, tenant_id)
        if df_entregas.empty:
            raise ValueError("Nenhum dado encontrado para as datas informadas")

        df_entregas["envio_data"] = pd.to_datetime(df_entregas["envio_data"]).dt.date

        parametros = {
            "min_entregas_por_cluster_alvo": min_entregas_por_cluster_alvo,
            "max_entregas_por_cluster_alvo": max_entregas_por_cluster_alvo,
            "hub_central_id": hub_central_id,
            "raio_cluster_hub_central": raio_cluster_hub_central,
            "modo_forcar": True,
        }
        resumo_datas = []

        for index, envio_data in enumerate(datas_envio):
            base_progress = 20 + int((index / max(1, len(datas_envio))) * 60)
            notify(base_progress, f"Preparando data {envio_data}")

            if writer.existe_resumo_clusterizacao(envio_data, tenant_id):
                if modo_forcar:
                    writer.excluir_clusterizacao_por_data_tenant(envio_data, tenant_id)
                else:
                    continue

            df_envio = df_entregas[df_entregas["envio_data"] == envio_data].copy()
            df_envio["tenant_id"] = tenant_id

            use_case = ClusterizationUseCase(
                clustering_service=ClusteringService(UF_BOUNDS, random_state=42, max_clusters=15, logger=logger),
                min_entregas_por_cluster_alvo=min_entregas_por_cluster_alvo,
                max_entregas_por_cluster_alvo=max_entregas_por_cluster_alvo,
                usar_cluster_hub_central=True,
                hub_central_id=hub_central_id,
                raio_cluster_hub_central_km=raio_cluster_hub_central,
                centro_service=centro_service,
            )

            notify(base_progress + 10, f"Calculando clusters de {envio_data}")
            df_clusterizado, df_centros, df_outliers = use_case.executar(df_envio)

            if df_clusterizado["id_entrega"].isna().any():
                df_clusterizado = df_clusterizado[df_clusterizado["id_entrega"].notna()]

            notify(base_progress + 20, f"Ajustando centros de {envio_data}")
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
                    cte_valor_frete_total=("cte_valor_frete", "sum"),
                ).reset_index()
            )

            df_resumo = pd.merge(resumo, df_centros, on="cluster", how="left")
            df_resumo["distancia_media_km"] = 0
            df_resumo["tempo_estimado_min"] = 0
            df_resumo["cte_valor_nf_total"] = df_resumo["cte_valor_nf_total"].fillna(0)
            df_resumo["cte_valor_frete_total"] = df_resumo["cte_valor_frete_total"].fillna(0)

            notify(base_progress + 35, f"Salvando resultados de {envio_data}")
            writer.salvar_clusterizacao(df_clusterizado)
            writer.salvar_resumo_clusters(
                df_resumo.assign(envio_data=envio_data, tenant_id=tenant_id),
                envio_data,
                tenant_id,
            )

            distribuicao = {
                str(row["cluster"]): int(row["quantidade_entregas"])
                for _, row in df_resumo[["cluster", "quantidade_entregas"]].iterrows()
            }
            resumo_datas.append(
                {
                    "data": str(envio_data),
                    "total_entregas": int(len(df_clusterizado)),
                    "total_outliers": int(len(df_outliers)),
                    "total_clusters": int(df_clusterizado["cluster"].nunique()),
                    "distribuicao_clusters": distribuicao,
                }
            )

        notify(95, "Finalizando clusterização")
        return {
            "status": "ok",
            "mensagem": f"✅ Clusterização finalizada para {len(datas_envio)} dia(s)",
            "datas": [str(d) for d in datas_envio],
            "parametros": parametros,
            "resumo": resumo_datas,
        }
    finally:
        db.fechar_conexao()
