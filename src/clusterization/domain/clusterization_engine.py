from typing import Optional, Tuple

import numpy as np
import pandas as pd
from geopy.distance import geodesic
from sklearn.cluster import KMeans


class ClusterizationEngine:
    """
    Motor de clusterizacao geografica usado pelo modulo clusterization.

    Mantem a API historica do ClusteringService, mas aproxima o comportamento do
    motor usado em simulation: k fixo quando solicitado, balanced_kmeans,
    centro pelo ponto mais denso e suporte a clusters especiais do hub central.
    """

    LAT_COL = "destino_latitude"
    LON_COL = "destino_longitude"

    def __init__(
        self,
        uf_bounds: dict,
        random_state: int = 42,
        max_clusters: int = 15,
        reader=None,
        logger=None,
    ):
        self.uf_bounds = uf_bounds
        self.random_state = random_state
        self.max_clusters = max_clusters
        self.reader = reader
        self.logger = logger

    def _log_info(self, message: str):
        if self.logger:
            self.logger.info(message)

    def _log_warning(self, message: str):
        if self.logger:
            self.logger.warning(message)

    def _normalizar_colunas_coordenadas(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        aliases = {
            "latitude": self.LAT_COL,
            "longitude": self.LON_COL,
        }
        for origem, destino in aliases.items():
            if destino not in df.columns and origem in df.columns:
                df[destino] = df[origem]

        for col in [self.LAT_COL, self.LON_COL]:
            if col not in df.columns:
                raise ValueError(f"Coluna obrigatoria ausente: {col}")
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def is_within_uf(self, latitude: float, longitude: float, uf_code: str) -> bool:
        if pd.isna(latitude) or pd.isna(longitude) or not uf_code:
            return False

        bounds = self.uf_bounds.get(str(uf_code).strip().upper())
        if not bounds:
            return False

        return (
            bounds["lat_min"] <= float(latitude) <= bounds["lat_max"]
            and bounds["lon_min"] <= float(longitude) <= bounds["lon_max"]
        )

    def filter_outliers_by_uf(self, data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if data.empty:
            return data.copy(), pd.DataFrame(columns=data.columns)

        df = self._normalizar_colunas_coordenadas(data)
        if "cte_uf" not in df.columns:
            validos = df.dropna(subset=[self.LAT_COL, self.LON_COL]).copy()
            outliers = df.loc[~df.index.isin(validos.index)].copy()
            return validos, outliers

        mascara_valida = df.apply(
            lambda row: self.is_within_uf(
                row[self.LAT_COL],
                row[self.LON_COL],
                row["cte_uf"],
            ),
            axis=1,
        )
        return df.loc[mascara_valida].copy(), df.loc[~mascara_valida].copy()

    def determine_optimal_clusters_elbow(self, data: pd.DataFrame) -> int:
        df = self._normalizar_colunas_coordenadas(data).dropna(
            subset=[self.LAT_COL, self.LON_COL]
        )
        if len(df) <= 1:
            return 1

        k_values = list(range(2, min(len(df), self.max_clusters) + 1))
        if not k_values:
            return 1

        inertia = []
        coords = df[[self.LAT_COL, self.LON_COL]].values
        for k in k_values:
            kmeans = KMeans(n_clusters=k, random_state=self.random_state, n_init=10)
            kmeans.fit(coords)
            inertia.append(kmeans.inertia_)

        return self._find_elbow_point(k_values, inertia)

    def _find_elbow_point(self, k_values, inertia) -> int:
        if len(k_values) == 1:
            return k_values[0]

        line_start = np.array([k_values[0], inertia[0]], dtype=float)
        line_end = np.array([k_values[-1], inertia[-1]], dtype=float)
        denominator = np.linalg.norm(line_end - line_start)
        if denominator == 0:
            return k_values[0]

        distances = [
            abs(np.cross(line_end - line_start, np.array([k, i]) - line_start) / denominator)
            for k, i in zip(k_values, inertia)
        ]
        return k_values[int(np.argmax(distances))]

    def perform_clustering(
        self,
        data: pd.DataFrame,
        n_clusters: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._normalizar_colunas_coordenadas(data).dropna(
            subset=[self.LAT_COL, self.LON_COL]
        ).copy()

        if df.empty:
            return df, pd.DataFrame(columns=["cluster", "centro_lat", "centro_lon", "cluster_cidade"])

        if n_clusters is None:
            n_clusters = self.determine_optimal_clusters_elbow(df)

        n_clusters = max(1, min(int(n_clusters), len(df)))
        modelo = KMeans(n_clusters=n_clusters, random_state=self.random_state, n_init=10)
        df["cluster"] = modelo.fit_predict(df[[self.LAT_COL, self.LON_COL]].values)

        centers_df = self._recalculate_centers(df)
        df = self._aplicar_centros(df, centers_df)
        return df, centers_df

    def perform_capacity_clustering(
        self,
        data: pd.DataFrame,
        n_clusters: int,
        min_entregas_por_cluster: int,
        max_entregas_por_cluster: int,
        max_distancia_fusao_km: Optional[float] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df, _ = self.perform_clustering(data=data, n_clusters=n_clusters)
        if df.empty:
            return df, self._recalculate_centers(df)

        df = self._split_clusters_acima_do_maximo(
            df=df,
            max_entregas=max_entregas_por_cluster,
        )
        df = self._merge_clusters_abaixo_do_minimo(
            df=df,
            min_entregas=min_entregas_por_cluster,
            max_entregas=max_entregas_por_cluster,
            max_distancia_fusao_km=max_distancia_fusao_km,
        )
        df = self._renumerar_clusters(df)
        centers_df = self._recalculate_centers(df)
        df = self._aplicar_centros(df, centers_df)

        tamanhos = df.groupby("cluster").size().sort_index().to_dict()
        self._log_info(f"📦 Clusters balanceados por capacidade: {tamanhos}")
        return df, centers_df

    def _split_clusters_acima_do_maximo(self, df: pd.DataFrame, max_entregas: int) -> pd.DataFrame:
        if df.empty or max_entregas <= 0:
            return df.copy()

        df = self._normalizar_colunas_coordenadas(df).copy()
        partes = []
        proximo_cluster = 0
        fila = [
            df[df["cluster"].astype(str) == str(cluster_id)].copy()
            for cluster_id in sorted(df["cluster"].unique(), key=lambda item: str(item))
        ]

        while fila:
            cluster_df = fila.pop(0)
            quantidade = len(cluster_df)

            if quantidade <= max_entregas:
                cluster_df["cluster"] = proximo_cluster
                proximo_cluster += 1
                partes.append(cluster_df)
                continue

            n_subclusters = int(np.ceil(quantidade / max_entregas))
            n_subclusters = max(1, min(n_subclusters, quantidade))
            coords = cluster_df[[self.LAT_COL, self.LON_COL]].values
            modelo = KMeans(n_clusters=n_subclusters, random_state=self.random_state, n_init=10)
            labels = modelo.fit_predict(coords)

            self._log_info(
                f"Cluster com {quantidade} entregas dividido em "
                f"{n_subclusters} subclusters por maximo={max_entregas}."
            )

            for label in sorted(set(labels)):
                subcluster_df = cluster_df[labels == label].copy()
                fila.append(subcluster_df)

        if not partes:
            return df

        return pd.concat(partes, ignore_index=True)

    def _merge_clusters_abaixo_do_minimo(
        self,
        df: pd.DataFrame,
        min_entregas: int,
        max_entregas: int,
        max_distancia_fusao_km: Optional[float] = None,
    ) -> pd.DataFrame:
        if df.empty or min_entregas <= 1:
            return df.copy()

        df = self._normalizar_colunas_coordenadas(df).copy()

        while True:
            tamanhos = df.groupby("cluster").size().to_dict()
            pequenos = [cluster for cluster, size in tamanhos.items() if size < min_entregas]

            if not pequenos or len(tamanhos) <= 1:
                break

            cluster_origem = min(pequenos, key=lambda cluster: tamanhos[cluster])
            tamanho_origem = tamanhos[cluster_origem]
            centros = df.groupby("cluster")[[self.LAT_COL, self.LON_COL]].mean()
            origem = centros.loc[cluster_origem].values.astype(float)

            candidatos = [
                cluster for cluster, size in tamanhos.items()
                if cluster != cluster_origem and size + tamanho_origem <= max_entregas
            ]
            if max_distancia_fusao_km is not None:
                candidatos = [
                    cluster for cluster in candidatos
                    if geodesic(
                        tuple(origem),
                        tuple(centros.loc[cluster].values.astype(float)),
                    ).km <= max_distancia_fusao_km
                ]

            if not candidatos:
                self._log_warning(
                    f"Cluster {cluster_origem} ficou com {tamanho_origem} entregas; "
                    "nenhum destino proximo com capacidade disponivel para fusao."
                )
                break

            cluster_destino = min(
                candidatos,
                key=lambda destino: np.linalg.norm(
                    origem - centros.loc[destino].values.astype(float)
                ),
            )

            self._log_info(
                f"Cluster {cluster_origem} com {tamanho_origem} entregas fundido ao "
                f"Cluster {cluster_destino} ({tamanhos[cluster_destino]} entregas)."
            )
            df.loc[df["cluster"] == cluster_origem, "cluster"] = cluster_destino

        return df

    def _renumerar_clusters(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "cluster" not in df.columns:
            return df.copy()

        df = df.copy()
        clusters = sorted(df["cluster"].unique(), key=lambda item: str(item))
        mapa = {cluster_id: novo_id for novo_id, cluster_id in enumerate(clusters)}
        df["cluster"] = df["cluster"].map(mapa)
        return df

    def encontrar_centro_mais_denso(self, cluster_data: pd.DataFrame) -> Tuple[float, float]:
        df = self._normalizar_colunas_coordenadas(cluster_data)
        coords = df[[self.LAT_COL, self.LON_COL]].dropna().astype(float).values
        if len(coords) == 0:
            return None, None

        dist_matrix = np.sqrt(((coords[:, None, :] - coords[None, :, :]) ** 2).sum(axis=2))
        idx = int(np.argmin(dist_matrix.sum(axis=1)))
        return float(coords[idx][0]), float(coords[idx][1])

    def _recalculate_centers(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty or "cluster" not in data.columns:
            return pd.DataFrame(columns=["cluster", "centro_lat", "centro_lon", "cluster_cidade"])

        df = self._normalizar_colunas_coordenadas(data)
        centers = []
        for cluster_id in sorted(df["cluster"].unique(), key=lambda item: str(item)):
            cluster_data = df[df["cluster"].astype(str) == str(cluster_id)]
            centro_lat, centro_lon = self.encontrar_centro_mais_denso(cluster_data)
            cidade_mais_comum = (
                cluster_data["cte_cidade"].mode(dropna=True).iloc[0]
                if "cte_cidade" in cluster_data.columns and not cluster_data["cte_cidade"].dropna().empty
                else None
            )
            centers.append((cluster_id, centro_lat, centro_lon, cidade_mais_comum))

        return pd.DataFrame(
            centers,
            columns=["cluster", "centro_lat", "centro_lon", "cluster_cidade"],
        )

    def _aplicar_centros(self, data: pd.DataFrame, centers_df: pd.DataFrame) -> pd.DataFrame:
        df = data.drop(columns=["centro_lat", "centro_lon", "cluster_cidade"], errors="ignore").copy()
        if centers_df.empty:
            return df
        return df.merge(centers_df, on="cluster", how="left")

    def fundir_clusters_pequenos(self, df: pd.DataFrame, min_entregas: int) -> pd.DataFrame:
        if df.empty or "cluster" not in df.columns:
            return df.copy()

        df = self._normalizar_colunas_coordenadas(df)
        centers_df = self._recalculate_centers(df)
        df = self._aplicar_centros(df, centers_df)

        cluster_sizes = df.groupby("cluster").size().to_dict()
        clusters_pequenos = [c for c, size in cluster_sizes.items() if size < min_entregas]
        clusters_validos = [c for c, size in cluster_sizes.items() if size >= min_entregas]

        if not clusters_pequenos:
            self._log_info("Nenhum cluster pequeno encontrado para fusao.")
            return df
        if not clusters_validos:
            self._log_warning("Nenhum cluster com entregas suficientes para receber fusoes.")
            return df

        centros = df.groupby("cluster")[["centro_lat", "centro_lon"]].mean()
        substituicoes = {}
        for cluster_id in clusters_pequenos:
            origem = centros.loc[cluster_id].values.astype(float)
            melhor_cluster = min(
                clusters_validos,
                key=lambda destino: np.linalg.norm(
                    origem - centros.loc[destino].values.astype(float)
                ),
            )
            substituicoes[cluster_id] = melhor_cluster
            self._log_info(
                f"Cluster {cluster_id} fundido com Cluster {melhor_cluster} "
                f"({cluster_sizes[cluster_id]} entregas)."
            )

        df["cluster"] = df["cluster"].map(lambda item: substituicoes.get(item, item))
        return self._aplicar_centros(df, self._recalculate_centers(df))

    def atribuir_cluster_hub_central(
        self,
        df: pd.DataFrame,
        hub_lat: float,
        hub_lon: float,
        raio_km: float,
        cluster_id: int = 9999,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = self._normalizar_colunas_coordenadas(df)
        if df.empty:
            return df.copy(), df.copy()

        distancias = df.apply(
            lambda row: geodesic(
                (row[self.LAT_COL], row[self.LON_COL]),
                (hub_lat, hub_lon),
            ).km,
            axis=1,
        )
        df = df.assign(distancia_ao_hub=distancias)
        df_hub = df[df["distancia_ao_hub"] <= raio_km].copy()
        df_restante = df[df["distancia_ao_hub"] > raio_km].copy()

        if not df_hub.empty:
            df_hub["cluster"] = cluster_id
            df_hub["centro_lat"] = float(hub_lat)
            df_hub["centro_lon"] = float(hub_lon)
            df_hub["cluster_cidade"] = "HUB_CENTRAL"
            df_hub["cluster_endereco"] = "HUB_CENTRAL"

        return df_hub, df_restante

    def clusterizar(
        self,
        df: pd.DataFrame,
        k: int,
        algoritmo: str = "kmeans",
        **kwargs,
    ) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        if algoritmo == "kmeans":
            df_clusterizado, _ = self.perform_clustering(df, n_clusters=k)
            return df_clusterizado

        if algoritmo == "balanced_kmeans":
            return self.balanced_kmeans(df, n_clusters=k, **kwargs)

        raise ValueError(f"Algoritmo nao suportado: {algoritmo}")

    def balanced_kmeans(
        self,
        df: pd.DataFrame,
        n_clusters: int,
        max_iter: int = 10,
        tolerance: int = 2,
    ) -> pd.DataFrame:
        df = self._normalizar_colunas_coordenadas(df).dropna(
            subset=[self.LAT_COL, self.LON_COL]
        ).copy()
        if df.empty:
            return df

        n_clusters = max(1, min(int(n_clusters), len(df)))
        coords = df[[self.LAT_COL, self.LON_COL]].values

        kmeans = KMeans(n_clusters=n_clusters, random_state=self.random_state, n_init=10)
        df["cluster"] = kmeans.fit_predict(coords)

        target_size = int(len(df) / n_clusters)
        for _ in range(max_iter):
            cluster_sizes = df["cluster"].value_counts().to_dict()
            clusters_excesso = [c for c, s in cluster_sizes.items() if s > target_size + tolerance]
            clusters_deficit = [c for c, s in cluster_sizes.items() if s < target_size - tolerance]

            if not clusters_excesso or not clusters_deficit:
                break

            for c_excesso in clusters_excesso:
                excesso = cluster_sizes[c_excesso] - target_size
                pontos = df[df["cluster"] == c_excesso].copy()
                centro = pontos[[self.LAT_COL, self.LON_COL]].mean().values
                pontos["dist_centro"] = np.linalg.norm(
                    pontos[[self.LAT_COL, self.LON_COL]].values - centro,
                    axis=1,
                )

                candidatos = pontos.sort_values("dist_centro", ascending=False).head(excesso)
                for idx, row in candidatos.iterrows():
                    best_cluster = None
                    best_dist = float("inf")
                    for c_deficit in clusters_deficit:
                        centro_deficit = df[df["cluster"] == c_deficit][
                            [self.LAT_COL, self.LON_COL]
                        ].mean().values
                        dist = np.linalg.norm(
                            row[[self.LAT_COL, self.LON_COL]].values - centro_deficit
                        )
                        if dist < best_dist:
                            best_dist = dist
                            best_cluster = c_deficit

                    if best_cluster is not None:
                        df.at[idx, "cluster"] = best_cluster

        return self._aplicar_centros(df, self._recalculate_centers(df))
