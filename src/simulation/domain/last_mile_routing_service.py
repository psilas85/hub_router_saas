# simulation/domain/last_mile_routing_service.py

import pandas as pd
from sklearn.cluster import KMeans
import json

from simulation.utils.route_helpers import (
    calcular_elbow_last_mile,
    sequenciar_ferradura_last_mile, subdividir_subcluster_por_veiculo
)
from simulation.utils.helpers import calcular_tempo_entrega_last_mile
from simulation.infrastructure.simulation_database_reader import (
    carregar_tarifas_last_mile,
    definir_tipo_veiculo_last_mile,
    buscar_latlon_ctes
)
from simulation.infrastructure.cache_routes import obter_rota_last_mile


class LastMileRoutingService:
    def __init__(self, simulation_db, clusterization_db, tenant_id: str, logger,
                 parametros: dict, envio_data: str, permitir_rotas_excedentes: bool = False):
        self.simulation_db = simulation_db
        self.clusterization_db = clusterization_db
        self.tenant_id = tenant_id
        self.logger = logger
        self.parametros = parametros
        self.envio_data = envio_data
        self.permitir_rotas_excedentes = permitir_rotas_excedentes

    def rotear_last_mile(self, df_clusterizado: pd.DataFrame, k_clusters: int, tempo_maximo: int = None) -> pd.DataFrame:
        self.logger.info("📦 Iniciando roteirização de last-mile...")

        df_tarifas = carregar_tarifas_last_mile(self.simulation_db)
        detalhes_totais = []

        for cluster_id, df_cluster in df_clusterizado.groupby('cluster'):
            self.logger.info(f"➔ Processando cluster {cluster_id} com {len(df_cluster)} entregas")

            df_cluster = df_cluster.copy()
            df_cluster['cte_peso'] = df_cluster['cte_peso'].astype(float)
            df_cluster['cte_volumes'] = df_cluster['cte_volumes'].astype(int)
            df_cluster['cte_numero'] = df_cluster['cte_numero'].astype(str)

            df_coords = buscar_latlon_ctes(
                clusterization_db=self.clusterization_db,
                simulation_db=self.simulation_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                lista_ctes=df_cluster['cte_numero'].tolist(),
                k_clusters=k_clusters,
                logger=self.logger
            )

            df_coords = pd.merge(
                df_coords,
                df_cluster[['cte_numero', 'cte_peso', 'cte_volumes']],
                on='cte_numero',
                how='left'
            )
            df_coords.columns = df_coords.columns.str.strip()

            # ✅ Validação de entregas ignoradas por falta de coordenadas
            try:
                cte_esperados = set(df_cluster["cte_numero"].unique())
                cte_com_coord = set(df_coords["cte_numero"].unique())

                cte_sem_coord = cte_esperados - cte_com_coord
                if len(cte_sem_coord) > 0:
                    self.logger.warning(f"⚠️ {len(cte_sem_coord)} entregas ignoradas por falta de coordenadas.")
                    self.logger.debug(f"CTEs sem coordenadas: {list(cte_sem_coord)}")
            except Exception as e:
                self.logger.error(f"❌ Erro ao identificar CTEs sem coordenadas: {e}")

            coordenadas = df_coords[['destino_latitude', 'destino_longitude']].dropna()
            if coordenadas.empty:
                raise ValueError(f"❌ Coordenadas ausentes para cluster {cluster_id}")

            if coordenadas.shape[0] <= 1:
                self.logger.error(f"❌ Cluster {cluster_id} possui {coordenadas.shape[0]} ponto(s). Roteirização cancelada para este cluster.")
                continue

            n_entregas = len(df_coords)
            entregas_por_subcluster = self.parametros.get('entregas_por_subcluster', 25)
            k_sub = max(1, n_entregas // entregas_por_subcluster)

            self.logger.info(
                f"🚀 Subclusterização inicial definida por heurística: {k_sub} subclusters para {n_entregas} entregas "
                f"(parâmetro entregas_por_subcluster = {entregas_por_subcluster})"
            )

            tentativas = 0
            max_tentativas = 15


            while tentativas < max_tentativas:
                tentativas += 1

                if len(coordenadas) < k_sub:
                    self.logger.warning(f"⚠️ Número de pontos ({len(coordenadas)}) menor que k_sub ({k_sub}). Ajustando para {len(coordenadas)}.")
                    k_sub = len(coordenadas)

                df_coords['subcluster'] = KMeans(n_clusters=k_sub, random_state=42).fit_predict(coordenadas.values)

                violou_restricao = False
                detalhes_cluster = []

                for sub_id, df_sub in df_coords.groupby('subcluster'):
                    subclusters = subdividir_subcluster_por_veiculo(
                        df_subcluster=df_sub,
                        df_tarifas=df_tarifas,
                        tempo_maximo=tempo_maximo or self.parametros['tempo_maximo_roteirizacao'],
                        parametros=self.parametros,
                        tenant_id=self.tenant_id,
                        simulation_db=self.simulation_db,
                        logger=self.logger,
                        cluster_cidade=df_cluster['cluster_cidade'].iloc[0],
                        cidades_entregas=df_sub['cte_cidade'].tolist()
                    )


                    if not subclusters:
                        violou_restricao = True
                        break

                    for idx, (df_ordenado, tipo_veiculo, distancia_total_km, _) in enumerate(subclusters):
                        rota_id = f"ROTA_{cluster_id}_{sub_id}_{idx}"
                        origem = (df_ordenado['centro_lat'].iloc[0], df_ordenado['centro_lon'].iloc[0])
                        sequencia_coord = []
                        rota_ida_coords = []
                        anterior = origem

                        distancia_parcial = 0.0
                        tempo_parcial = 0.0

                        for i in range(len(df_ordenado)):
                            atual = (df_ordenado.iloc[i]['destino_latitude'], df_ordenado.iloc[i]['destino_longitude'])

                            if anterior == atual:
                                self.logger.warning(f"⚠️ Ignorando chamada de rota: origem == destino para CTE {df_ordenado.iloc[i]['cte_numero']} no cluster {cluster_id}")
                                rota_ida_coords.append(atual)
                                anterior = atual
                                continue

                            dist, tempo, rota_completa = obter_rota_last_mile(
                                anterior, atual, self.tenant_id, self.simulation_db, self.simulation_db, self.logger
                            )

                            try:
                                if dist is not None:
                                    distancia_parcial += float(dist)
                                if tempo is not None:
                                    tempo_parcial += float(tempo)
                            except Exception as e:
                                self.logger.warning(f"⚠️ Erro ao somar dist/tempo para CTE {df_ordenado.iloc[i]['cte_numero']}: {e}")

                            if rota_completa:
                                rota_ida_coords.extend([(p["lat"], p["lon"]) for p in rota_completa])
                            else:
                                rota_ida_coords.append(atual)

                            anterior = atual

                        tempo_parada = (
                            self.parametros['tempo_parada_pesada']
                            if df_ordenado["cte_peso"].sum() > self.parametros['limite_peso_parada']
                            else self.parametros['tempo_parada_leve']
                        ) * len(df_ordenado)
                        tempo_parcial += tempo_parada

                        tempo_descarga = df_ordenado["cte_volumes"].sum() * self.parametros['tempo_por_volume']
                        tempo_parcial += tempo_descarga

                        rota_volta_coords = []
                        if anterior == origem:
                            self.logger.warning(f"⚠️ Ignorando retorno: origem == último ponto.")
                            rota_volta_coords.append(origem)
                            tempo_back = 0.0
                            dist_back = 0.0
                        else:
                            dist_back, tempo_back, rota_back = obter_rota_last_mile(
                                anterior, origem, self.tenant_id, self.simulation_db, self.simulation_db, self.logger
                            )
                            try:
                                dist_back = float(dist_back) if dist_back is not None else 0.0
                                tempo_back = float(tempo_back) if tempo_back is not None else 0.0
                            except Exception as e:
                                self.logger.warning(f"⚠️ Erro ao processar retorno para rota {rota_id}: {e}")
                                dist_back = 0.0
                                tempo_back = 0.0

                            if rota_back:
                                rota_volta_coords.extend([(p["lat"], p["lon"]) for p in rota_back])
                            else:
                                rota_volta_coords.append(origem)

                        sequencia_coord = []

                        if rota_ida_coords and isinstance(rota_ida_coords, list):
                            sequencia_coord.extend(rota_ida_coords)

                        if rota_volta_coords and isinstance(rota_volta_coords, list):
                            sequencia_coord.extend(rota_volta_coords)

                        if not sequencia_coord:
                            self.logger.warning(f"⚠️ Coordenadas ausentes na ida e volta da rota {rota_id}. Marcando coordenadas_seq como None.")
                            sequencia_coord = None

                        tempo_total_real = tempo_parcial + tempo_back

                        self.logger.info(
                            f"📏 Rota {rota_id} — distância parcial: {distancia_parcial:.2f} km, retorno: {dist_back:.2f} km, total: {distancia_parcial + dist_back:.2f} km"
                        )
                        self.logger.info(
                            f"⏱️ Rota {rota_id} — tempo parcial: {tempo_parcial:.2f} min, retorno: {tempo_back:.2f} min, total: {tempo_total_real:.2f} min"
                        )

                        # 🕐 Validação do tempo total da rota
                        tempo_limite = tempo_maximo or self.parametros['tempo_maximo_roteirizacao']
                        if tempo_total_real > tempo_limite:
                            if self.permitir_rotas_excedentes:
                                self.logger.warning(
                                    f"⚠️ Rota {rota_id} excede o tempo máximo permitido "
                                    f"({tempo_total_real:.2f} > {tempo_limite:.2f}) minutos, "
                                    f"mas foi aceita pois permitir_rotas_excedentes=True."
                                )
                            else:
                                self.logger.warning(
                                    f"⛔ Rota {rota_id} excede o tempo máximo permitido "
                                    f"({tempo_total_real:.2f} > {tempo_limite:.2f}) minutos. "
                                    f"Subdividindo novamente..."
                                )
                                violou_restricao = True
                                break


                        for i, row in df_ordenado.iterrows():
                            origem_igual_destino = (
                                round(row["destino_latitude"], 6) == round(df_ordenado['centro_lat'].iloc[0], 6) and
                                round(row["destino_longitude"], 6) == round(df_ordenado['centro_lon'].iloc[0], 6)
                            )

                            detalhes_cluster.append({
                                "cte_numero": row["cte_numero"],
                                "cluster": cluster_id,
                                "rota_id": rota_id,
                                "ordem_entrega": row["ordem_entrega"],
                                "tipo_veiculo": tipo_veiculo,
                                "distancia_km": None,
                                "tempo_minutos": None,
                                "qtde_entregas": len(df_ordenado),
                                "peso_total": df_ordenado["cte_peso"].sum(),
                                "volumes_total": df_ordenado["cte_volumes"].sum(),
                                "distancia_total_km": round(distancia_parcial + dist_back, 2) if i == 0 else None,
                                "tempo_total_min": round(tempo_total_real, 2) if i == 0 else None,
                                "distancia_parcial_km": round(distancia_parcial, 2) if i == 0 else None,
                                "tempo_parcial_min": round(tempo_parcial, 2) if i == 0 else None,
                                "latitude": row["destino_latitude"],
                                "longitude": row["destino_longitude"],
                                "coordenadas_seq": sequencia_coord if i == 0 else None,
                                "entrega_com_rota": not origem_igual_destino
                            })

                if not violou_restricao:
                    detalhes_totais.extend(detalhes_cluster)
                    break
                else:
                    k_sub += 1
                    self.logger.info(f"🔁 Subindo k_sub para {k_sub} em cluster {cluster_id} devido a restrição violada.")

            else:
                msg = (
                    f"❌ Falha ao encontrar subclusters viáveis no cluster {cluster_id} "
                    f"com {len(df_coords)} entregas após {max_tentativas} tentativas "
                    f"(último k_sub={k_sub}). Verifique se os parâmetros estão adequados."
                )

                self.logger.error(msg)
                raise RuntimeError(msg)

        df_detalhes = pd.DataFrame(detalhes_totais)
        self.logger.info(f"✅ Roteirização last-mile finalizada com {len(df_detalhes)} entregas em {df_detalhes['rota_id'].nunique()} rotas.")
        return df_detalhes




    def salvar_rotas_last_mile_em_db(
        self,
        df_rotas: pd.DataFrame,
        tenant_id: str,
        envio_data: str,
        simulation_id: str,
        k_clusters: int,
        db_conn
    ):
        self.logger.info("💾 Salvando rotas last-mile no banco...")

        cursor = db_conn.cursor()

        # 🔄 Limpar rotas e resumos anteriores completamente
        self.logger.info(f"🧹 Limpando registros anteriores para tenant={tenant_id}, envio_data={envio_data}, k={k_clusters}...")

        cursor.execute("""
            DELETE FROM rotas_last_mile
            WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
        """, (tenant_id, envio_data, k_clusters))

        cursor.execute("""
            DELETE FROM resumo_rotas_last_mile
            WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
        """, (tenant_id, envio_data, k_clusters))
        db_conn.commit()

        # 📝 Inserção das entregas
        insert_sql = """
            INSERT INTO rotas_last_mile (
                tenant_id, envio_data, simulation_id, k_clusters,
                cluster, rota_id, cte_numero, ordem_entrega,
                distancia_km, tempo_minutos, tipo_veiculo,
                created_at, qtde_entregas, peso_total,
                volumes_total, distancia_total_km, tempo_total_min,
                latitude, longitude, coordenadas_seq, entrega_com_rota,
                distancia_parcial_km, tempo_parcial_min
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, CURRENT_TIMESTAMP, %s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s
            )
        """
       
        # 📊 Resumo por rota
        self.logger.info("📝 Gerando resumo por rota para salvar...")

        # Contagem real de entregas por rota
        df_qtdes = df_rotas.groupby("rota_id")["cte_numero"].count().rename("qde_entregas").reset_index()

        # Agregação das métricas da linha i == 0 (resumos)
        df_resumo = (
            df_rotas[df_rotas["distancia_parcial_km"].notnull()]
            .groupby("rota_id")
            .agg({
                "tipo_veiculo": "first",
                "peso_total": "first",
                "volumes_total": "first",
                "distancia_total_km": "first",
                "tempo_total_min": "first",
                "distancia_parcial_km": "first",
                "tempo_parcial_min": "first"
            })
            .reset_index()
            .merge(df_qtdes, on="rota_id", how="left")  # ✅ Junta contagem correta
            .rename(columns={
                "peso_total": "peso_total_kg",
                "volumes_total": "qde_volumes"
            })
        )


        df_resumo["tenant_id"] = tenant_id
        df_resumo["envio_data"] = envio_data
        df_resumo["simulation_id"] = simulation_id
        df_resumo["k_clusters"] = k_clusters

        df_resumo = df_resumo[[  # ordenado
            "tenant_id", "envio_data", "simulation_id", "k_clusters",
            "rota_id", "tipo_veiculo", "peso_total_kg", "qde_volumes",
            "distancia_total_km", "tempo_total_min",
            "distancia_parcial_km", "tempo_parcial_min",
            "qde_entregas"
        ]]

        insert_resumo_sql = """
            INSERT INTO resumo_rotas_last_mile (
                tenant_id, envio_data, simulation_id, k_clusters,
                rota_id, tipo_veiculo, peso_total_kg, qde_volumes,
                distancia_total_km, tempo_total_min,
                distancia_parcial_km, tempo_parcial_min,
                qde_entregas
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s
            )
        """

        valores = [
            (
                row["tenant_id"], row["envio_data"], row["simulation_id"], row["k_clusters"],
                row["rota_id"], row["tipo_veiculo"], row["peso_total_kg"], row["qde_volumes"],
                row["distancia_total_km"], row["tempo_total_min"],
                row["distancia_parcial_km"], row["tempo_parcial_min"],
                row["qde_entregas"]
            )
            for _, row in df_resumo.iterrows()
        ]

        for i, row in df_rotas.iterrows():
            try:
                cte_numero = str(row.get('cte_numero') or f"SIMUL_{simulation_id[:8]}_{i}")
                cluster = int(row.get('cluster') or 0)
                rota_id = str(row.get('rota_id') or f"ROTA_{cluster}")
                ordem_entrega = int(row.get('ordem_entrega') or 0)

                distancia_km = float(row.get('distancia_km')) if pd.notnull(row.get('distancia_km')) else None
                tempo_minutos = float(row.get('tempo_minutos')) if pd.notnull(row.get('tempo_minutos')) else None
                tipo_veiculo = str(row.get('tipo_veiculo') or "Desconhecido")

                qtde_entregas = int(row.get('qtde_entregas') or 0)
                peso_total = float(row.get('peso_total') or 0)
                volumes_total = int(row.get('volumes_total') or 0)

                distancia_total_km = float(row.get('distancia_total_km')) if pd.notnull(row.get('distancia_total_km')) else None
                tempo_total_min = float(row.get('tempo_total_min')) if pd.notnull(row.get('tempo_total_min')) else None

                latitude = float(row.get('latitude')) if pd.notnull(row.get('latitude')) else None
                longitude = float(row.get('longitude')) if pd.notnull(row.get('longitude')) else None

                entrega_com_rota = bool(row.get('entrega_com_rota', True))

                distancia_parcial_km = float(row.get('distancia_parcial_km')) if pd.notnull(row.get('distancia_parcial_km')) else None
                tempo_parcial_min = float(row.get('tempo_parcial_min')) if pd.notnull(row.get('tempo_parcial_min')) else None

                coordenadas_raw = row.get("coordenadas_seq")
                coordenadas_str = None

                try:
                    if isinstance(coordenadas_raw, str):
                        coordenadas_str = coordenadas_raw if coordenadas_raw.strip() else None
                    elif isinstance(coordenadas_raw, list) and len(coordenadas_raw) > 0:
                        coordenadas_str = json.dumps([
                            {"lat": float(lat), "lon": float(lon)}
                            for ponto in coordenadas_raw
                            if isinstance(ponto, (list, tuple)) and len(ponto) == 2
                            for lat, lon in [ponto]
                        ])
                except Exception as e:
                    self.logger.warning(
                        f"⚠️ Erro ao converter coordenadas para JSON na rota {row.get('rota_id')} | CTE {row.get('cte_numero')}: {e}"
                    )
                    coordenadas_str = None


                cursor.execute(insert_sql, (
                    tenant_id,
                    envio_data,
                    simulation_id,
                    k_clusters,
                    cluster,
                    rota_id,
                    cte_numero,
                    ordem_entrega,
                    distancia_km,
                    tempo_minutos,
                    tipo_veiculo,
                    qtde_entregas,
                    peso_total,
                    volumes_total,
                    distancia_total_km,
                    tempo_total_min,
                    latitude,
                    longitude,
                    coordenadas_str,
                    entrega_com_rota,
                    distancia_parcial_km,
                    tempo_parcial_min
                ))

            except Exception as e:
                self.logger.error(f"❌ Erro ao processar rota {i} / {row.get('rota_id')}: {e}")


        cursor.executemany(insert_resumo_sql, valores)
        db_conn.commit()
        cursor.close()
        self.logger.info(f"✅ {len(valores)} resumos de rotas last-mile salvos com sucesso.")
