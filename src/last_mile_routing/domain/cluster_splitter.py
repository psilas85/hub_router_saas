import pandas as pd
from sklearn.cluster import KMeans
import numpy as np

from last_mile_routing.domain.heuristics import calcular_num_clusters_elbow
from last_mile_routing.logs.logging_factory import LoggerFactory
from last_mile_routing.domain.route_calculator import RotaCalculator
from last_mile_routing.domain.routing_utils import calcular_tempo_total


class ClusterSplitter:
    def __init__(self, tenant_id, cache, parametros, api_key):
        self.logger = LoggerFactory.get_logger("routing")
        self.tenant_id = tenant_id
        self.cache = cache
        self.parametros = parametros
        self.api_key = api_key

    def dividir_subclusters(self, df_cluster, centro_lat, centro_lon):
        n_entregas = len(df_cluster)

        n_clusters = max(1, n_entregas // self.parametros.get('entregas_por_subcluster', 25))
        self.logger.info(f"🚀 Subclusterização inicial: {n_clusters} subclusters para {n_entregas} entregas.")

        df_cluster = self._aplicar_kmeans(df_cluster, n_clusters)

        df_final = pd.DataFrame()
        subcluster_id_global = 0

        rota_calculator = RotaCalculator(self.tenant_id, self.cache, self.api_key)

        grupos = df_cluster.groupby('sub_cluster')

        for _, df_sub in grupos:
            df_resultado, subcluster_id_global = self._subdividir_recursivo(
                df_sub,
                centro_lat,
                centro_lon,
                rota_calculator,
                subcluster_id_global
            )
            df_final = pd.concat([df_final, df_resultado])

        return df_final.reset_index(drop=True)

    def _subdividir_recursivo(self, df_sub, centro_lat, centro_lon, rota_calculator, subcluster_id_global):
        """
        Função recursiva para subdividir até que o tempo de ida esteja dentro do limite.
        """
        pontos = list(zip(df_sub['destino_latitude'], df_sub['destino_longitude']))

        rota_coords, distancia_ida, tempo_transito_ida = rota_calculator.obter_tracado_rota(
            (centro_lat, centro_lon), pontos
        )

        peso_total = df_sub["cte_peso"].sum()
        volumes_total = df_sub["cte_volumes"].sum()

        tempo_parada = (
            self.parametros["tempo_parada_leve"]
            if peso_total <= self.parametros["peso_leve_max"]
            else self.parametros["tempo_parada_pesada"]
        )

        tempo_parcial, _ = calcular_tempo_total(
            distancia_ida,
            volumes_total,
            qtde_paradas=len(df_sub),
            tempo_parada=tempo_parada,
            tempo_descarga_por_volume=self.parametros["tempo_descarga_por_volume"]
        )

        if tempo_parcial <= self.parametros["tempo_maximo_rota"] or len(df_sub) == 1:
            df_sub = df_sub.copy()
            df_sub["sub_cluster"] = subcluster_id_global
            self.logger.info(
                f"✅ Subcluster {subcluster_id_global} criado com {len(df_sub)} entregas, tempo de ida {tempo_parcial:.2f} min."
            )
            subcluster_id_global += 1
            return df_sub, subcluster_id_global

        else:
            self.logger.warning(
                f"⚠️ Subcluster com {len(df_sub)} entregas excede tempo máximo de ida ({tempo_parcial:.2f} > {self.parametros['tempo_maximo_rota']}). Subdividindo..."
            )

            n_subdivide = min(2, len(df_sub))  # No mínimo 2, se possível

            df_sub = self._aplicar_kmeans(df_sub, n_subdivide)

            df_resultado = pd.DataFrame()

            grupos = df_sub.groupby('sub_cluster')

            for _, df_sub_interno in grupos:
                df_temp, subcluster_id_global = self._subdividir_recursivo(
                    df_sub_interno,
                    centro_lat,
                    centro_lon,
                    rota_calculator,
                    subcluster_id_global
                )
                df_resultado = pd.concat([df_resultado, df_temp])

            return df_resultado, subcluster_id_global

    def _aplicar_kmeans(self, df, n_clusters):
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        df = df.copy()
        df['sub_cluster'] = kmeans.fit_predict(
            df[['destino_latitude', 'destino_longitude']]
        )
        return df
