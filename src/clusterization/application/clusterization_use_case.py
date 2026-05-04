# clusterization/application/clusterization_use_case.py

import math
from typing import Optional, Tuple
import numpy as np
import pandas as pd

from clusterization.domain.clustering_service import ClusteringService


class ClusterizationUseCase:

    def __init__(
        self,
        clustering_service: ClusteringService,
        min_entregas_por_cluster_alvo: int = 10,
        max_entregas_por_cluster_alvo: int = 100,
        k_min: Optional[int] = None,
        k_max: Optional[int] = None,
        min_entregas_por_cluster: Optional[int] = None,
        usar_cluster_hub_central: bool = True,
        hub_central_id: Optional[int] = None,
        raio_cluster_hub_central_km: float = 15.0,
        raio_maximo_cluster_km: float = 120.0,
        centro_service=None
    ):
        self.clustering_service = clustering_service
        if min_entregas_por_cluster is not None and min_entregas_por_cluster_alvo == 10:
            min_entregas_por_cluster_alvo = min_entregas_por_cluster

        self.min_entregas_por_cluster_alvo = max(1, int(min_entregas_por_cluster_alvo))
        self.max_entregas_por_cluster_alvo = max(
            self.min_entregas_por_cluster_alvo,
            int(max_entregas_por_cluster_alvo)
        )
        self.usar_cluster_hub_central = usar_cluster_hub_central
        self.hub_central_id = hub_central_id
        self.raio_cluster_hub_central_km = raio_cluster_hub_central_km
        self.raio_maximo_cluster_km = max(1.0, float(raio_maximo_cluster_km))
        self.centro_service = centro_service

    def _calcular_distancia_maxima_km(self, entregas_df: pd.DataFrame) -> float:
        if entregas_df is None or len(entregas_df) <= 1:
            return 0.0

        lat_col = self.clustering_service.LAT_COL
        lon_col = self.clustering_service.LON_COL
        coords = entregas_df[[lat_col, lon_col]].dropna().astype(float).to_numpy()
        if len(coords) <= 1:
            return 0.0

        lat = np.radians(coords[:, 0])
        lon = np.radians(coords[:, 1])
        dlat = lat[:, None] - lat[None, :]
        dlon = lon[:, None] - lon[None, :]
        a = np.sin(dlat / 2) ** 2 + np.cos(lat[:, None]) * np.cos(lat[None, :]) * np.sin(dlon / 2) ** 2
        distancia = 6371.0088 * 2 * np.arcsin(np.minimum(1, np.sqrt(a)))
        return float(np.nanmax(distancia))

    def calcular_quantidade_clusters(self, total_entregas: int, entregas_df: pd.DataFrame = None) -> int:
        if total_entregas <= 0:
            return 0

        min_cluster = self.min_entregas_por_cluster_alvo
        max_cluster = self.max_entregas_por_cluster_alvo

        k_min_operacional = max(1, math.ceil(total_entregas / max_cluster))
        k_max_operacional = max(1, math.floor(total_entregas / min_cluster))

        if k_min_operacional > k_max_operacional:
            k_min_operacional = k_max_operacional

        alvo_entregas_por_cluster = (min_cluster + max_cluster) / 2
        k_alvo = max(1, int(math.floor((total_entregas / alvo_entregas_por_cluster) + 0.5)))
        distancia_maxima_km = self._calcular_distancia_maxima_km(entregas_df)
        k_espacial = max(1, math.ceil(distancia_maxima_km / self.raio_maximo_cluster_km))
        k_calculado = max(k_min_operacional, k_espacial, min(k_alvo, k_max_operacional))
        k_calculado = min(k_calculado, total_entregas)

        if self.clustering_service.logger:
            self.clustering_service.logger.info(
                "🔢 k calculado por faixa de entregas por cluster | "
                f"total={total_entregas} "
                f"min_cluster={min_cluster} "
                f"max_cluster={max_cluster} "
                f"k_min_operacional={k_min_operacional} "
                f"k_max_operacional={k_max_operacional} "
                f"distancia_maxima_km={distancia_maxima_km:.1f} "
                f"raio_maximo_cluster_km={self.raio_maximo_cluster_km:.1f} "
                f"k_espacial={k_espacial} "
                f"k={k_calculado}"
            )

        return k_calculado

    def executar(
        self,
        entregas_df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Aplica a clusterização em entregas com base em coordenadas geográficas.
        Retorna:
        - df_clusterizado: DataFrame com as entregas e suas atribuições de cluster
        - df_centros: DataFrame com os centros de cada cluster
        - df_outliers: DataFrame com registros removidos (fora dos limites da UF)
        """

        df_validos, df_outliers = self.clustering_service.filter_outliers_by_uf(entregas_df)

        if df_validos.empty:
            raise ValueError("Nenhuma entrega válida disponível para clusterização.")

        # 🚩 Separar entregas do cluster Hub Central (se habilitado)
        if self.usar_cluster_hub_central:
            tenant_id = entregas_df["tenant_id"].iloc[0]
            if self.hub_central_id is None:
                raise ValueError("Hub Central é obrigatório para executar a clusterização.")

            try:
                hub_coords = self.centro_service.buscar_hub_central(
                    tenant_id=tenant_id,
                    hub_central_id=self.hub_central_id,
                )
            except ValueError as exc:
                hub_coords = None
                if self.clustering_service.logger:
                    self.clustering_service.logger.warning(
                        f"Hub Central indisponivel para tenant={tenant_id}. "
                        f"Clusterizacao seguira sem cluster HUB_CENTRAL. Detalhe: {exc}"
                    )

            if hub_coords is None:
                raise ValueError("Hub Central selecionado não foi encontrado ou não está ativo.")
            else:
                hub_lat, hub_lon = hub_coords

                df_hub, df_validos = self.clustering_service.atribuir_cluster_hub_central(
                    df_validos,
                    hub_lat=hub_lat,
                    hub_lon=hub_lon,
                    raio_km=self.raio_cluster_hub_central_km
                )
                if not df_hub.empty:
                    if self.clustering_service.logger:
                        self.clustering_service.logger.info(
                            f"🏢 {len(df_hub)} entregas atribuídas ao cluster HUB_CENTRAL (tenant={tenant_id})."
                        )
        else:
            df_hub = pd.DataFrame()

        n_entregas = len(df_validos)

        if df_validos.empty:
            if df_hub.empty:
                raise ValueError("Nenhuma entrega válida disponível para clusterização.")

            df_centros = self.clustering_service._recalculate_centers(df_hub)
            return df_hub, df_centros, df_outliers

        # 🔢 Determinação operacional do número de clusters.
        # Similar à simulation: deriva k pela faixa de entregas por cluster,
        # sem Elbow e sem limites de k informados pelo usuário.
        n_clusters_inicial = self.calcular_quantidade_clusters(n_entregas, df_validos)

        # 🚀 Clusterização principal com capacidade operacional por cluster.
        # O k nasce da faixa min/max, e o motor corrige clusters que saiam
        # dessa faixa quando houver capacidade para redistribuir.
        df_clusterizado, df_centros = self.clustering_service.perform_capacity_clustering(
            data=df_validos.copy(),
            n_clusters=n_clusters_inicial,
            min_entregas_por_cluster=self.min_entregas_por_cluster_alvo,
            max_entregas_por_cluster=self.max_entregas_por_cluster_alvo,
            max_distancia_fusao_km=self.raio_maximo_cluster_km,
        )

        # ✅ Adiciona o cluster hub (se existir)
        if not df_hub.empty:
            df_clusterizado = pd.concat([df_clusterizado, df_hub], ignore_index=True)
            df_centros = self.clustering_service._recalculate_centers(df_clusterizado)

        return df_clusterizado, df_centros, df_outliers
