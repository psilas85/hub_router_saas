#last_mile_routing/domain/routing_service.py

import pandas as pd
from geopy.distance import geodesic
import numpy as np
import logging
from sklearn.cluster import KMeans

from last_mile_routing.domain.vehicle_selector import selecionar_veiculo



class LastMileRoutingService:
    def __init__(self, tenant_id, envio_data, cluster, df_cluster, parametros: dict, faixas_veiculo):
        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.cluster = cluster
        self.df = df_cluster
        self.parametros = parametros
        self.faixas_veiculo = faixas_veiculo
        self.logger = logging.getLogger("last_mile_routing")

        self.origem = (self.df["centro_lat"].iloc[0], self.df["centro_lon"].iloc[0])
        self.rota_id_counter = 0

    def calcular_distancia_km(self, origem, destino):
        return geodesic(origem, destino).km

    def sequenciar_ferradura(self, origem, destinos):
        destinos = destinos.copy()
        distancias = [self.calcular_distancia_km(origem, (lat, lon)) for lat, lon in destinos]
        ordenados = [x for _, x in sorted(zip(distancias, destinos), key=lambda pair: pair[0])]
        metade = len(ordenados) // 2
        ida = ordenados[:metade]
        volta = ordenados[metade:]
        volta.reverse()
        return ida + volta

    def calcular_tempo_rota(self, distancia_km, qtde_entregas, volumes, peso_total):
        velocidade = 40
        tempo_transito = (distancia_km / velocidade) * 60

        tempo_parada = self.parametros.get("tempo_parada_leve", 10)
        if peso_total > self.parametros.get("peso_leve_max", 200):
            tempo_parada = self.parametros.get("tempo_parada_pesada", 20)

        tempo_descarga = volumes * self.parametros.get("tempo_descarga_por_volume", 0.4)

        return tempo_transito + (tempo_parada * qtde_entregas) + tempo_descarga

    def gerar_rota(self, df_sub):
        self.rota_id_counter += 1
        rota_id = f"{self.envio_data}_{self.cluster}_R{self.rota_id_counter}"

        destinos = list(zip(df_sub["destino_latitude"], df_sub["destino_longitude"]))
        sequencia = self.sequenciar_ferradura(self.origem, destinos)

        distancia_parcial = sum(
            self.calcular_distancia_km(sequencia[i], sequencia[i + 1]) 
            for i in range(len(sequencia) - 1)
        )

        distancia_volta = self.calcular_distancia_km(sequencia[-1], self.origem)

        distancia_total = distancia_parcial + distancia_volta

        tempo_total = self.calcular_tempo_rota(
            distancia_km=distancia_total,
            qtde_entregas=len(df_sub),
            volumes=df_sub["cte_volumes"].sum(),
            peso_total=df_sub["cte_peso"].sum()
        )

        tipo_veiculo = selecionar_veiculo(
            peso_total=df_sub["cte_peso"].sum(),
            custos_veiculos=self.faixas_veiculo,
            centro_cidade=df_sub["cluster_cidade"].iloc[0],
            cidades=df_sub["cte_cidade"].unique()
        )


        return {
            "rota_id": rota_id,
            "sequencia": sequencia,
            "distancia_parcial": distancia_parcial,
            "distancia_volta": distancia_volta,
            "distancia_total": distancia_total,
            "tempo_total": tempo_total,
            "tipo_veiculo": tipo_veiculo
        }

    def subdividir_recursivamente(self, df_sub):
        tempo_max = self.parametros.get("tempo_maximo_rota", 600)
        resultado = pd.DataFrame()

        fila = [df_sub.copy()]

        while fila:
            atual = fila.pop(0)
            rota = self.gerar_rota(atual)

            if rota["tempo_total"] <= tempo_max or len(atual) == 1:
                atual["rota_id"] = rota["rota_id"]
                resultado = pd.concat([resultado, atual])
                self.logger.info(
                    f"✅ Rota {rota['rota_id']} criada com {len(atual)} entregas, tempo {rota['tempo_total']:.2f} min."
                )
            else:
                self.logger.warning(
                    f"⚠️ Rota excede tempo máximo ({rota['tempo_total']:.2f} > {tempo_max}). Subdividindo..."
                )
                kmeans = KMeans(n_clusters=2, random_state=42, n_init=10)
                atual["subcluster"] = kmeans.fit_predict(atual[["destino_latitude", "destino_longitude"]])

                for sub_id in atual["subcluster"].unique():
                    sub_df = atual[atual["subcluster"] == sub_id].drop(columns="subcluster")
                    fila.append(sub_df)

        return resultado.reset_index(drop=True)

    def rotear_last_mile(self):
        if len(self.df) == 0:
            return pd.DataFrame()

        # 🚀 Subclusterização inicial por proximidade
        n_clusters = max(1, len(self.df) // self.parametros.get('entregas_por_subcluster', 25))
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
        self.df["subcluster"] = kmeans.fit_predict(self.df[["destino_latitude", "destino_longitude"]])

        resultado = pd.DataFrame()

        for subcluster_id in self.df["subcluster"].unique():
            sub_df = self.df[self.df["subcluster"] == subcluster_id].copy()
            resultado = pd.concat([resultado, self.subdividir_recursivamente(sub_df)])

        return resultado.reset_index(drop=True)
