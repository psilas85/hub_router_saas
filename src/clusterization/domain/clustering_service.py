#hub_router_1.0.1/src/clusterization/domain/clustering_service.py

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from typing import Optional, Tuple
from geopy.distance import geodesic


class ClusteringService:
    def __init__(
        self,
        uf_bounds: dict,
        random_state: int = 42,
        max_clusters: int = 15,
        reader=None,
        logger=None
    ):
        self.uf_bounds = uf_bounds
        self.random_state = random_state
        self.max_clusters = max_clusters
        self.reader = reader
        self.logger = logger

    def is_within_uf(self, latitude: float, longitude: float, uf_code: str) -> bool:
        bounds = self.uf_bounds.get(uf_code)
        if not bounds:
            return False
        return (
            bounds['lat_min'] <= latitude <= bounds['lat_max'] and
            bounds['lon_min'] <= longitude <= bounds['lon_max']
        )

    def filter_outliers_by_uf(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if data.empty:
            return data, pd.DataFrame()

        valid_data = []
        removed_rows = []

        for _, row in data.iterrows():
            if self.is_within_uf(row['destino_latitude'], row['destino_longitude'], row['cte_uf'].strip()):
                valid_data.append(row)
            else:
                removed_rows.append(row)

        return pd.DataFrame(valid_data), pd.DataFrame(removed_rows)

    def determine_optimal_clusters_elbow(self, data: pd.DataFrame) -> int:
        k_values = list(range(2, min(len(data), self.max_clusters) + 1))
        if not k_values:
            return 1

        inertia = []
        for k in k_values:
            kmeans = KMeans(n_clusters=k, random_state=self.random_state, n_init=10)
            kmeans.fit(data[['destino_latitude', 'destino_longitude']])
            inertia.append(kmeans.inertia_)

        return self._find_elbow_point(k_values, inertia)

    def _find_elbow_point(self, k_values, inertia) -> int:
        line_start = np.array([k_values[0], inertia[0]])
        line_end = np.array([k_values[-1], inertia[-1]])
        distances = [
            np.abs(
                np.cross(line_end - line_start, np.array([k, i]) - line_start) /
                np.linalg.norm(line_end - line_start)
            )
            for k, i in zip(k_values, inertia)
        ]
        return k_values[np.argmax(distances)]

    def perform_clustering(self, data: pd.DataFrame, n_clusters: Optional[int] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if n_clusters is None:
            n_clusters = self.determine_optimal_clusters_elbow(data)

        kmeans = KMeans(n_clusters=n_clusters, random_state=self.random_state, n_init=10)
        data['cluster'] = kmeans.fit_predict(data[['destino_latitude', 'destino_longitude']])

        centers_df = self._recalculate_centers(data)
        for cluster_id, row in centers_df.iterrows():
            data.loc[data['cluster'] == cluster_id, 'centro_lat'] = row['centro_lat']
            data.loc[data['cluster'] == cluster_id, 'centro_lon'] = row['centro_lon']
        return data, centers_df

    def encontrar_centro_mais_denso(self, cluster_data: pd.DataFrame) -> Tuple[float, float]:
        coords = cluster_data[['destino_latitude', 'destino_longitude']].dropna().values
        if len(coords) == 0:
            return None, None
        dist_matrix = np.sqrt(((coords[:, None, :] - coords[None, :, :]) ** 2).sum(axis=2))
        densidade = dist_matrix.sum(axis=1)
        idx = np.argmin(densidade)
        return coords[idx]

    def _recalculate_centers(self, data: pd.DataFrame) -> pd.DataFrame:
        centers = []
        for cluster_id in sorted(data['cluster'].unique()):
            cluster_data = data[data['cluster'] == cluster_id]
            centro_lat, centro_lon = self.encontrar_centro_mais_denso(cluster_data)
            cidade_mais_comum = (
                cluster_data['cte_cidade'].mode().values[0]
                if not cluster_data['cte_cidade'].isna().all()
                else None
            )
            centers.append((cluster_id, centro_lat, centro_lon, cidade_mais_comum))
        return pd.DataFrame(centers, columns=['cluster', 'centro_lat', 'centro_lon', 'cluster_cidade'])

    def fundir_clusters_pequenos(self, df: pd.DataFrame, min_entregas: int) -> pd.DataFrame:
        df = df.copy()
        cluster_sizes = df.groupby('cluster').size().to_dict()

        clusters_pequenos = [c for c, size in cluster_sizes.items() if size < min_entregas]

        if not clusters_pequenos:
            self.logger.info("✅ Nenhum cluster pequeno encontrado para fusão.")
            return df

        for cluster_id in clusters_pequenos:
            centro_cluster = df[df['cluster'] == cluster_id][['centro_lat', 'centro_lon']].iloc[0]

            outros_centros = (
                df[df['cluster'] != cluster_id]
                .groupby('cluster')[['centro_lat', 'centro_lon']]
                .mean()
                .reset_index()
            )

            outros_centros['distancia'] = np.sqrt(
                (outros_centros['centro_lat'] - centro_cluster['centro_lat']) ** 2 +
                (outros_centros['centro_lon'] - centro_cluster['centro_lon']) ** 2
            )

            cluster_mais_proximo = int(outros_centros.sort_values('distancia').iloc[0]['cluster'])

            self.logger.info(
                f"🔗 Cluster {cluster_id} (com {cluster_sizes[cluster_id]} entregas) "
                f"fundido com Cluster {cluster_mais_proximo}."
            )

            df.loc[df['cluster'] == cluster_id, 'cluster'] = cluster_mais_proximo

        return df

    def atribuir_cluster_hub_central(
        self,
        df: pd.DataFrame,
        hub_lat: float,
        hub_lon: float,
        raio_km: float,
        cluster_id: int = 9999
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Separa as entregas que estão dentro do raio definido do hub central.
        Retorna:
        - df_hub: entregas atribuídas ao cluster do hub central
        - df_restante: entregas que seguem para clusterização normal
        """
        df = df.copy()
        distancias = df.apply(
            lambda row: geodesic(
                (row['destino_latitude'], row['destino_longitude']),
                (hub_lat, hub_lon)
            ).km,
            axis=1
        )
        df['distancia_ao_hub'] = distancias

        df_hub = df[df['distancia_ao_hub'] <= raio_km].copy()
        df_restante = df[df['distancia_ao_hub'] > raio_km].copy()

        if not df_hub.empty:
            df_hub['cluster'] = cluster_id
            df_hub['centro_lat'] = hub_lat
            df_hub['centro_lon'] = hub_lon
            df_hub['cluster_cidade'] = 'HUB_CENTRAL'
            df_hub['cluster_endereco'] = 'HUB_CENTRAL'

        return df_hub, df_restante

    def clusterizar(
        self,
        df: pd.DataFrame,
        k: int,
        algoritmo: str = "kmeans",
        **kwargs
    ) -> pd.DataFrame:

        if df.empty:
            return df

        if algoritmo == "kmeans":
            df_clusterizado, _ = self.perform_clustering(df, n_clusters=k)
            return df_clusterizado

        elif algoritmo == "balanced_kmeans":
            return self.balanced_kmeans(df, n_clusters=k)

        else:
            raise ValueError(f"Algoritmo não suportado: {algoritmo}")

    def balanced_kmeans(
        self,
        df: pd.DataFrame,
        n_clusters: int,
        max_iter: int = 10,
        tolerance: int = 2
    ) -> pd.DataFrame:

        if df.empty:
            return df

        # 🔥 garantir colunas corretas
        df = df.rename(columns={
            "latitude": "destino_latitude",
            "longitude": "destino_longitude"
        }).copy()

        coords = df[['destino_latitude', 'destino_longitude']].values

        # 1️⃣ KMeans inicial
        kmeans = KMeans(n_clusters=n_clusters, random_state=self.random_state, n_init=10)
        df['cluster'] = kmeans.fit_predict(coords)

        target_size = int(len(df) / n_clusters)

        for _ in range(max_iter):

            cluster_sizes = df['cluster'].value_counts().to_dict()

            clusters_excesso = [c for c, s in cluster_sizes.items() if s > target_size + tolerance]
            clusters_deficit = [c for c, s in cluster_sizes.items() if s < target_size - tolerance]

            if not clusters_excesso or not clusters_deficit:
                break

            for c_excesso in clusters_excesso:
                excesso = cluster_sizes[c_excesso] - target_size

                pontos = df[df['cluster'] == c_excesso].copy()

                # distância ao centro atual
                centro = pontos[['destino_latitude', 'destino_longitude']].mean().values
                pontos['dist_centro'] = np.linalg.norm(
                    pontos[['destino_latitude', 'destino_longitude']].values - centro,
                    axis=1
                )

                # pega os mais distantes (melhor candidatos a mover)
                candidatos = pontos.sort_values('dist_centro', ascending=False).head(excesso)

                for idx, row in candidatos.iterrows():

                    # acha cluster mais próximo com déficit
                    dist_min = float("inf")
                    best_cluster = None

                    for c_def in clusters_deficit:
                        centro_def = df[df['cluster'] == c_def][['destino_latitude', 'destino_longitude']].mean().values
                        dist = np.linalg.norm(
                            row[['destino_latitude', 'destino_longitude']].values - centro_def
                        )

                        if dist < dist_min:
                            dist_min = dist
                            best_cluster = c_def

                    if best_cluster is not None:
                        df.at[idx, 'cluster'] = best_cluster

        # 🔁 recalcular centros
        centers_df = self._recalculate_centers(df)

        for cluster_id, row in centers_df.iterrows():
            df.loc[df['cluster'] == cluster_id, 'centro_lat'] = row['centro_lat']
            df.loc[df['cluster'] == cluster_id, 'centro_lon'] = row['centro_lon']

        return df