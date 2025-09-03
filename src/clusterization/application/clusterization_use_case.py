# clusterization/application/clusterization_use_case.py

from typing import Optional, Tuple
import pandas as pd

from clusterization.domain.clustering_service import ClusteringService


class ClusterizationUseCase:

    def __init__(
        self,
        clustering_service: ClusteringService,
        k_min: int,
        k_max: int,
        min_entregas_por_cluster: int,
        fundir_clusters_pequenos: bool = False,
        usar_cluster_hub_central: bool = True,
        raio_cluster_hub_central_km: float = 15.0,
        centro_service=None
    ):
        self.clustering_service = clustering_service
        self.k_min = k_min
        self.k_max = k_max
        self.min_entregas_por_cluster = min_entregas_por_cluster
        self.fundir_clusters_pequenos = fundir_clusters_pequenos
        self.usar_cluster_hub_central = usar_cluster_hub_central
        self.raio_cluster_hub_central_km = raio_cluster_hub_central_km
        self.centro_service = centro_service


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

        n_entregas = len(df_validos)

        # 🚩 Separar entregas do cluster Hub Central (se habilitado)
        if self.usar_cluster_hub_central:
            tenant_id = entregas_df["tenant_id"].iloc[0]
            hub_coords = self.centro_service.buscar_hub_central(tenant_id=tenant_id)

            if hub_coords is None:
                raise ValueError(f"❌ Hub Central não encontrado no banco para tenant '{tenant_id}'.")

            hub_lat, hub_lon = hub_coords

            df_hub, df_validos = self.clustering_service.atribuir_cluster_hub_central(
                df_validos,
                hub_lat=hub_lat,
                hub_lon=hub_lon,
                raio_km=self.raio_cluster_hub_central_km
            )
            if not df_hub.empty:
                self.clustering_service.logger.info(
                    f"🏢 {len(df_hub)} entregas atribuídas ao cluster HUB_CENTRAL (tenant={tenant_id})."
                )
        else:
            df_hub = pd.DataFrame()

        # 🔢 Determinação do número de clusters
        try:
            n_clusters_otimo = self.clustering_service.determine_optimal_clusters_elbow(df_validos)
            self.clustering_service.logger.info(f"🔎 k sugerido pelo método do cotovelo: {n_clusters_otimo}")
            n_clusters_inicial = max(self.k_min, min(n_clusters_otimo, self.k_max))
        except Exception as e:
            self.clustering_service.logger.warning(f"⚠️ Erro ao calcular Elbow: {e} — usando heurística padrão.")
            n_clusters_inicial = max(self.k_min, n_entregas // self.min_entregas_por_cluster)
            n_clusters_inicial = min(n_clusters_inicial, self.k_max)

        n_clusters_inicial = min(n_clusters_inicial, self.k_max)

        # 🚀 Clusterização principal
        df_clusterizado, df_centros = self.clustering_service.perform_clustering(
            data=df_validos.copy(),
            n_clusters=n_clusters_inicial
        )

        # 🔗 Fusão de clusters pequenos (se ativado)
        if self.fundir_clusters_pequenos:
            df_clusterizado = self.clustering_service.fundir_clusters_pequenos(
                df=df_clusterizado,
                min_entregas=self.min_entregas_por_cluster
            )
            df_centros = self.clustering_service._recalculate_centers(df_clusterizado)

        # ✅ Adiciona o cluster hub (se existir)
        if not df_hub.empty:
            df_clusterizado = pd.concat([df_clusterizado, df_hub], ignore_index=True)

        return df_clusterizado, df_centros, df_outliers
