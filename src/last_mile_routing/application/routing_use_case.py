#last_mile_routing/application/routing_use_case.py

import os
from datetime import datetime
import pandas as pd
import uuid
from dotenv import load_dotenv  # 🔹 novo para carregar .env

from last_mile_routing.infrastructure.database_connection import (
    conectar_banco_cluster,
    conectar_banco_routing,
    fechar_conexao
)
from last_mile_routing.infrastructure.database_reader import (
    buscar_entregas_clusterizadas,
    buscar_dados_entregas,
    buscar_custos_veiculos
)
from last_mile_routing.infrastructure.database_writer import (
    salvar_last_mile_rota,
    salvar_detalhe_rota,
    excluir_roteirizacao_anterior
)
from last_mile_routing.infrastructure.cache_route import load_cache
from last_mile_routing.logs.logging_factory import LoggerFactory
from last_mile_routing.domain.cluster_splitter import ClusterSplitter
from last_mile_routing.domain.route_calculator import RotaCalculator
from last_mile_routing.domain.routing_utils import calcular_tempo_total
from last_mile_routing.domain.heuristics import sequenciar_ferradura
from last_mile_routing.domain.vehicle_selector import selecionar_veiculo

# 🔹 Carrega variáveis do .env (inclusive OSRM_URL)
load_dotenv()

class RoutingUseCase:
    def __init__(self, tenant_id, parametros, api_key):
        self.tenant_id = tenant_id
        self.parametros = parametros
        self.api_key = api_key
        self.logger = LoggerFactory.get_logger("routing")
        self.cache = load_cache(tenant_id)

        # 🔹 Lê URL do OSRM (padrão para container interno)
        self.osrm_url = os.getenv("OSRM_URL", "http://osrm:5000")

    def executar(self, envio_data):
        self.logger.info(f"🚀 Iniciando roteirização para {envio_data}...")
        self.logger.info(f"⚙️ Parâmetros operacionais: {self.parametros}")
        self.logger.info(f"🗺 OSRM URL: {self.osrm_url}")

        conn_cluster = conectar_banco_cluster()
        conn_routing = conectar_banco_routing()

        total_rotas = 0
        total_subclusters = 0
        total_ctes_roteirizados = 0

        try:
            excluir_roteirizacao_anterior(conn_routing, self.tenant_id, envio_data)

            df_cluster = buscar_entregas_clusterizadas(self.tenant_id, envio_data)
            df_dados = buscar_dados_entregas(self.tenant_id, envio_data)
            custos_veiculos = buscar_custos_veiculos(self.tenant_id)

            df_dados_filtrado = df_dados[[
                "cte_numero", "tenant_id", "cte_peso", "cte_volumes", "cte_cidade", "cte_valor_frete"
            ]]

            df = pd.merge(
                df_cluster,
                df_dados_filtrado,
                on=["cte_numero", "tenant_id"],
                how="left"
            )

            if df["destino_latitude"].isnull().any() or df["destino_longitude"].isnull().any():
                raise ValueError("❌ Existem entregas sem coordenadas após o merge.")

            agrupado = df.groupby("cluster")

            # 🔹 Agora o RotaCalculator recebe também o OSRM_URL
            rota_calculator = RotaCalculator(
                self.tenant_id, 
                self.cache, 
                self.api_key,
                osrm_url=self.osrm_url
            )

            splitter = ClusterSplitter(
                tenant_id=self.tenant_id,
                cache=self.cache,
                parametros=self.parametros,
                api_key=self.api_key
            )

            for cluster_id, df_clusterado in agrupado:
                centro_lat = df_clusterado["centro_lat"].iloc[0]
                centro_lon = df_clusterado["centro_lon"].iloc[0]

                self.logger.info(f"🚀 Processando Cluster {cluster_id} com {len(df_clusterado)} entregas.")

                df_dividido = splitter.dividir_subclusters(df_clusterado, centro_lat, centro_lon)

                sub_agrupado = df_dividido.groupby("sub_cluster")

                for sub_id, df_sub in sub_agrupado:
                    total_subclusters += 1

                    self.logger.info(f"🚀 Subcluster {sub_id} do cluster {cluster_id} com {len(df_sub)} entregas.")

                    if df_sub["destino_latitude"].isnull().any() or df_sub["destino_longitude"].isnull().any():
                        ctes_sem_coord = df_sub[
                            df_sub["destino_latitude"].isnull() | df_sub["destino_longitude"].isnull()
                        ]["cte_numero"].tolist()

                        self.logger.error(
                            f"❌ Subcluster {sub_id} do cluster {cluster_id} possui entregas sem coordenadas. "
                            f"CTEs: {ctes_sem_coord}"
                        )
                        continue

                    rota_id = str(uuid.uuid4())[:8]

                    pontos = list(zip(df_sub["destino_latitude"], df_sub["destino_longitude"]))
                    sequencia = sequenciar_ferradura(pontos)

                    # 🔹 Ida usando cache → OSRM → Google
                    rota_coords, distancia_parcial, tempo_transito_ida = rota_calculator.obter_tracado_rota(
                        (centro_lat, centro_lon), sequencia
                    )

                    # 🔹 Volta usando cache → OSRM → Google
                    _, distancia_retorno, tempo_retorno = rota_calculator.obter_tracado_rota(
                        sequencia[-1], [(centro_lat, centro_lon)]
                    )

                    distancia_total = distancia_parcial + distancia_retorno

                    peso_total = df_sub["cte_peso"].sum()
                    volumes_total = df_sub["cte_volumes"].sum()

                    tempo_parada = (
                        self.parametros["tempo_parada_leve"]
                        if peso_total <= self.parametros["peso_leve_max"]
                        else self.parametros["tempo_parada_pesada"]
                    )

                    tempo_parcial, _ = calcular_tempo_total(
                        distancia_parcial,
                        volumes_total,
                        qtde_paradas=len(df_sub),
                        tempo_parada=tempo_parada,
                        tempo_descarga_por_volume=self.parametros["tempo_descarga_por_volume"]
                    )

                    tempo_total = tempo_parcial + tempo_retorno

                    veiculo = selecionar_veiculo(
                        peso_total=peso_total,
                        custos_veiculos=custos_veiculos,
                        centro_cidade=df_sub["cluster_cidade"].iloc[0],
                        cidades=df_sub["cte_cidade"].unique(),
                        peso_leve_max=self.parametros["peso_leve_max"],
                        restricao_veiculo_leve_municipio=self.parametros["restricao_veiculo_leve_municipio"]
                    )

                    rota_json = {
                        "tenant_id": self.tenant_id,
                        "envio_data": str(envio_data),
                        "rota_id": rota_id,
                        "cluster": int(cluster_id),
                        "sub_cluster": int(sub_id),
                        "centro_lat": centro_lat,
                        "centro_lon": centro_lon,
                        "distancia_parcial_km": distancia_parcial,
                        "distancia_total_km": distancia_total,
                        "tempo_parcial_min": tempo_parcial,
                        "tempo_total_min": tempo_total,
                        "peso_total_kg": float(peso_total),
                        "volumes_total": int(volumes_total),
                        "veiculo": veiculo,
                        "entregas": [
                            {
                                "cte_numero": row["cte_numero"],
                                "cte_valor_frete_total": float(row["cte_valor_frete"])
                            }
                            for _, row in df_sub.iterrows()
                        ],
                        "rota_coord": rota_coords
                    }

                    salvar_last_mile_rota(conn_routing, rota_json, self.tenant_id)

                    for ordem, (lat, lon) in enumerate(sequencia, start=1):
                        cte_numero = df_sub.iloc[ordem - 1]["cte_numero"]

                        detalhe = {
                            "envio_data": envio_data,
                            "rota_id": rota_id,
                            "rota_codigo": rota_id,
                            "cluster": int(cluster_id),
                            "sub_cluster": int(sub_id),
                            "cte_numero": cte_numero,
                            "ordem_entrega": ordem,
                            "centro_lat": centro_lat,
                            "centro_lon": centro_lon,
                            "destino_latitude": lat,
                            "destino_longitude": lon,
                            "coordenadas_seq": rota_coords,
                            "distancia_km": distancia_total,
                            "tempo_transito_min": tempo_transito_ida,
                            "tempo_total_min": tempo_total,
                            "veiculo": veiculo,
                            "peso_kg": float(df_sub.iloc[ordem - 1]["cte_peso"]),
                            "volumes": int(df_sub.iloc[ordem - 1]["cte_volumes"]),
                            "valor_nf": 0,
                            "valor_frete": float(df_sub.iloc[ordem - 1]["cte_valor_frete"])
                        }

                        salvar_detalhe_rota(conn_routing, detalhe, self.tenant_id)

                    self._salvar_json_local(rota_json, envio_data)

                    self.logger.info(
                        f"✅ Rota {rota_id} criada com {len(df_sub)} entregas. "
                        f"Distância: {distancia_total:.2f} km, Tempo Total: {tempo_total:.2f} min "
                        f"(Tempo Ida: {tempo_parcial:.2f} min), Veículo: {veiculo}. "
                        f"Entregas: {df_sub['cte_numero'].tolist()}"
                    )

                    total_rotas += 1
                    total_ctes_roteirizados += len(df_sub)

            rota_calculator.salvar_cache()

            self.logger.info(
                f"📦 Resumo do processamento de {envio_data}: "
                f"Clusters processados: {agrupado.ngroups}, "
                f"Subclusters gerados: {total_subclusters}, "
                f"Rotas criadas: {total_rotas}, "
                f"Entregas roteirizadas: {total_ctes_roteirizados}."
            )

        finally:
            fechar_conexao(conn_cluster)
            fechar_conexao(conn_routing)

    def _salvar_json_local(self, rota_json, envio_data):
        pasta = f"output/routes_json/{self.tenant_id}/{envio_data}"
        os.makedirs(pasta, exist_ok=True)
        path = f"{pasta}/rota_{rota_json['rota_id']}.json"
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(rota_json))
