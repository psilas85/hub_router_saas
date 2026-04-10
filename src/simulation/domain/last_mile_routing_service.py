# simulation/domain/last_mile_routing_service.py

import json
from typing import Optional

import pandas as pd
from sklearn.cluster import KMeans

from simulation.utils.route_helpers import (
    estimar_viabilidade_subcluster,
    ordenar_entregas_subcluster,
    subdividir_subcluster_por_veiculo,
    dividir_subcluster_local,
)
from simulation.infrastructure.simulation_database_reader import (
    carregar_tarifas_last_mile,
    buscar_latlon_ctes
)
from simulation.infrastructure.cache_routes import (
    obter_rota_last_mile_detalhada,
)


class LastMileRoutingService:
    def __init__(self, simulation_db, clusterization_db, tenant_id: str, logger,
                 parametros: dict, envio_data: str, permitir_rotas_excedentes: bool = True):
        self.simulation_db = simulation_db
        self.clusterization_db = clusterization_db
        self.tenant_id = tenant_id
        self.logger = logger
        self.parametros = parametros
        self.envio_data = envio_data
        self.permitir_rotas_excedentes = permitir_rotas_excedentes
        self._route_attempts_by_cluster = {}

    def _registrar_tentativa_rota(
        self,
        cluster_id,
        rota_id: str,
        branch_label: str,
        tipo_veiculo: str,
        qtde_entregas: int,
        tempo_total_min: float,
        tempo_limite_min: float,
        distancia_total_km: float,
        ignoradas_mesmo_ponto: int,
        status: str,
    ):
        registros = self._route_attempts_by_cluster.setdefault(str(cluster_id), [])
        registros.append({
            'rota_id': rota_id,
            'branch_label': branch_label,
            'tipo_veiculo': tipo_veiculo,
            'qtde_entregas': qtde_entregas,
            'tempo_total_min': round(float(tempo_total_min), 2),
            'tempo_limite_min': round(float(tempo_limite_min), 2),
            'distancia_total_km': round(float(distancia_total_km), 2),
            'ignoradas_mesmo_ponto': int(ignoradas_mesmo_ponto),
            'status': status,
        })

    def _emitir_resumo_tentativas_cluster(self, cluster_id, status_final: str):
        registros = self._route_attempts_by_cluster.get(str(cluster_id), [])
        if not registros:
            return

        total_tentativas = len(registros)
        registros_finais_por_branch = {}
        for item in registros:
            registros_finais_por_branch[item['branch_label']] = item

        registros_finais = list(registros_finais_por_branch.values())
        total_branches_finais = len(registros_finais)
        total_viaveis = sum(
            1 for item in registros_finais if item['status'] == 'viavel_sla'
        )
        total_fallback = sum(
            1 for item in registros_finais if item['status'] == 'fallback_excedente'
        )
        total_inviaveis = sum(
            1 for item in registros_finais if item['status'] == 'inviavel'
        )

        self.logger.info(
            f"📋 Resumo de viabilização do cluster {cluster_id}: status_final={status_final} | "
            f"tentativas={total_tentativas} | branches_finais={total_branches_finais} | "
            f"viaveis_sla={total_viaveis} | "
            f"fallback_excedente={total_fallback} | inviaveis={total_inviaveis}"
        )
        for item in registros_finais:
            self.logger.info(
                "📋 Tentativa rota "
                f"{item['rota_id']} | branch={item['branch_label']} | status={item['status']} | "
                f"entregas={item['qtde_entregas']} | veiculo={item['tipo_veiculo']} | "
                f"tempo={item['tempo_total_min']:.2f}/{item['tempo_limite_min']:.2f} min | "
                f"distancia={item['distancia_total_km']:.2f} km | "
                f"origem_igual_destino={item['ignoradas_mesmo_ponto']}"
            )

    def _carregar_coords_cluster(self, df_cluster: pd.DataFrame, k_clusters: int):
        colunas_memoria = {
            'cte_numero',
            'centro_lat',
            'centro_lon',
            'latitude',
            'longitude',
            'cte_cidade',
        }
        if colunas_memoria.issubset(df_cluster.columns):
            df_coords = df_cluster[
                [
                    'cte_numero',
                    'centro_lat',
                    'centro_lon',
                    'latitude',
                    'longitude',
                    'cte_cidade',
                ]
            ].copy()
            df_coords = df_coords.rename(
                columns={
                    'latitude': 'destino_latitude',
                    'longitude': 'destino_longitude',
                }
            )
            df_coords['cte_numero'] = df_coords['cte_numero'].astype(str)
            return df_coords.drop_duplicates(subset=['cte_numero'])

        return buscar_latlon_ctes(
            clusterization_db=self.clusterization_db,
            simulation_db=self.simulation_db,
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            lista_ctes=df_cluster['cte_numero'].tolist(),
            k_clusters=k_clusters,
            logger=self.logger,
        )

    def _obter_velocidade_media_kmh(self):
        return float(
            self.parametros.get(
                'velocidade_media_kmh',
                self.parametros.get('velocidade', 60.0),
            )
            or 60.0
        )

    def _calcular_tempo_servico_entrega(self, row):
        tempo_atendimento = row.get('cte_tempo_atendimento_min')
        if tempo_atendimento is not None and pd.notna(tempo_atendimento):
            return max(float(tempo_atendimento), 0.0)

        tempo_parada = (
            self.parametros['tempo_parada_pesada']
            if float(row.get('cte_peso', 0.0) or 0.0) > self.parametros['limite_peso_parada']
            else self.parametros['tempo_parada_leve']
        )
        tempo_descarga = float(row.get('cte_volumes', 0) or 0) * self.parametros['tempo_por_volume']
        return float(tempo_parada) + float(tempo_descarga)

    def _gerar_subclusters_iniciais(self, df_coords: pd.DataFrame):
        n_entregas = len(df_coords)
        entregas_por_subcluster = self.parametros.get('entregas_por_subcluster', 25)
        k_sub = max(1, n_entregas // entregas_por_subcluster)
        coordenadas = df_coords[['destino_latitude', 'destino_longitude']].dropna()

        self.logger.info(
            f"🚀 Subclusterização inicial definida por heurística: {k_sub} subclusters para {n_entregas} entregas "
            f"(parâmetro entregas_por_subcluster = {entregas_por_subcluster})"
        )

        if coordenadas.empty or k_sub <= 1:
            return [("0", df_coords.copy())]

        if len(coordenadas) < k_sub:
            self.logger.warning(
                f"⚠️ Número de pontos ({len(coordenadas)}) menor que k_sub ({k_sub}). Ajustando para {len(coordenadas)}."
            )
            k_sub = len(coordenadas)

        df_inicial = df_coords.copy()
        df_inicial['subcluster'] = KMeans(
            n_clusters=k_sub,
            random_state=42,
            n_init="auto",
        ).fit_predict(coordenadas.values)
        return [
            (str(sub_id), df_sub.copy())
            for sub_id, df_sub in df_inicial.groupby('subcluster')
        ]

    def _contar_refinamentos_branch(self, branch_label: str) -> int:
        return str(branch_label).count('_r')

    def _obter_limite_refino_subcluster(self) -> int:
        limite = self.parametros.get(
            'max_tentativas_refino_subcluster',
            self.parametros.get('max_refinamentos_subcluster', 4),
        )
        return max(1, int(limite or 4))

    def _obter_limite_refino_global_emergencial(self, total_entregas_cluster: int) -> int:
        limite_parametrizado = self.parametros.get('max_tentativas_refino_cluster')
        try:
            limite_parametrizado = int(limite_parametrizado or 0)
        except (TypeError, ValueError):
            limite_parametrizado = 0

        limite_padrao = max(200, int(total_entregas_cluster) * 8)
        return max(limite_parametrizado, limite_padrao)

    def _resolver_subcluster_localmente(
        self,
        df_sub: pd.DataFrame,
        df_tarifas: pd.DataFrame,
        tempo_maximo: int,
        cluster_cidade,
        branch_label: str,
        depth: int = 0,
    ):
        subclusters = subdividir_subcluster_por_veiculo(
            df_subcluster=df_sub,
            df_tarifas=df_tarifas,
            tempo_maximo=tempo_maximo,
            parametros=self.parametros,
            tenant_id=self.tenant_id,
            simulation_db=self.simulation_db,
            logger=self.logger,
            cluster_cidade=cluster_cidade,
            cidades_entregas=df_sub['cte_cidade'].tolist(),
        )
        if subclusters:
            return [
                {
                    'label': f"{branch_label}_{idx}" if len(subclusters) > 1 else branch_label,
                    'df': df_ordenado,
                    'tipo_veiculo': tipo_veiculo,
                }
                for idx, (df_ordenado, tipo_veiculo, _, _) in enumerate(subclusters)
            ]

        max_depth = int(self.parametros.get('max_refinamentos_subcluster', 4) or 4)
        max_split = int(self.parametros.get('max_particoes_subcluster_local', 4) or 4)

        if depth >= max_depth or len(df_sub) <= 1:
            if self.permitir_rotas_excedentes:
                return self._montar_subcluster_excedente(
                    df_sub=df_sub,
                    df_tarifas=df_tarifas,
                    tempo_maximo=tempo_maximo,
                    cluster_cidade=cluster_cidade,
                    branch_label=branch_label,
                    motivo="limite de refinamento local atingido",
                )
            return None

        for split_size in range(2, min(max_split, len(df_sub)) + 1):
            self.logger.info(
                f"🧩 Refinando localmente subcluster {branch_label} em {split_size} partes (profundidade={depth + 1})."
            )
            filhos = dividir_subcluster_local(df_sub, split_size, logger=self.logger)
            if not filhos:
                continue

            resolvidos = []
            refinamento_ok = True
            for child_idx, (_, df_filho) in enumerate(filhos):
                resultado_filho = self._resolver_subcluster_localmente(
                    df_sub=df_filho,
                    df_tarifas=df_tarifas,
                    tempo_maximo=tempo_maximo,
                    cluster_cidade=cluster_cidade,
                    branch_label=f"{branch_label}_{split_size}_{child_idx}",
                    depth=depth + 1,
                )
                if not resultado_filho:
                    refinamento_ok = False
                    break
                resolvidos.extend(resultado_filho)

            if refinamento_ok:
                return resolvidos

        if self.permitir_rotas_excedentes:
            return self._montar_subcluster_excedente(
                df_sub=df_sub,
                df_tarifas=df_tarifas,
                tempo_maximo=tempo_maximo,
                cluster_cidade=cluster_cidade,
                branch_label=branch_label,
                motivo="subcluster permaneceu inviável após refinamento máximo",
            )

        return None

    def _montar_subcluster_excedente(
        self,
        df_sub: pd.DataFrame,
        df_tarifas: pd.DataFrame,
        tempo_maximo: int,
        cluster_cidade,
        branch_label: str,
        motivo: str,
    ):
        diagnostico = estimar_viabilidade_subcluster(
            df_subcluster=df_sub,
            df_tarifas=df_tarifas,
            tempo_maximo=tempo_maximo,
            parametros=self.parametros,
            logger=self.logger,
            cluster_cidade=cluster_cidade,
            cidades_entregas=df_sub['cte_cidade'].tolist(),
        )
        tempo_estimado = float(diagnostico['tempo_estimado'])
        if tempo_estimado > float(tempo_maximo):
            self.logger.warning(
                f"⚠️ Aceitando subcluster {branch_label} com excedente porque permitir_rotas_excedentes=True. "
                f"motivo={motivo} | tempo_estimado={tempo_estimado:.2f} min | limite={tempo_maximo:.2f} min"
            )
        else:
            self.logger.warning(
                f"⚠️ Aceitando subcluster {branch_label} sem novo refinamento porque permitir_rotas_excedentes=True. "
                f"motivo={motivo} | tempo_estimado={tempo_estimado:.2f} min | limite={tempo_maximo:.2f} min"
            )
        return [{
            'label': branch_label,
            'df': ordenar_entregas_subcluster(df_sub),
            'tipo_veiculo': diagnostico['tipo_veiculo'],
        }]

    def _montar_detalhes_rota(
        self,
        cluster_id,
        rota_label: str,
        df_ordenado: pd.DataFrame,
        tipo_veiculo: str,
        tempo_limite: int,
        velocidade_media_kmh: float,
        aceitar_excedente: bool = False,
    ):
        rota_id = f"ROTA_{cluster_id}_{rota_label}"
        origem = (df_ordenado['centro_lat'].iloc[0], df_ordenado['centro_lon'].iloc[0])
        sequencia_coord = []
        rota_ida_coords = []
        anterior = origem
        ctes_mesma_origem_destino = []

        distancia_parcial = 0.0
        tempo_parcial = 0.0
        fontes_metricas = set()

        for i in range(len(df_ordenado)):
            atual = (df_ordenado.iloc[i]['destino_latitude'], df_ordenado.iloc[i]['destino_longitude'])

            if anterior == atual:
                ctes_mesma_origem_destino.append(str(df_ordenado.iloc[i]['cte_numero']))
                rota_ida_coords.append(atual)
                anterior = atual
                continue

            dist, tempo, rota_completa, fonte_rota = obter_rota_last_mile_detalhada(
                anterior,
                atual,
                self.tenant_id,
                self.simulation_db,
                self.simulation_db,
                self.logger,
                velocidade_media_kmh,
            )

            try:
                if dist is not None:
                    distancia_parcial += float(dist)
                if tempo is not None:
                    tempo_parcial += float(tempo)
                fontes_metricas.add(fonte_rota)
            except Exception as e:
                self.logger.warning(
                    f"⚠️ Erro ao somar dist/tempo para CTE {df_ordenado.iloc[i]['cte_numero']}: {e}"
                )

            if rota_completa:
                rota_ida_coords.extend([(p["lat"], p["lon"]) for p in rota_completa])
            else:
                rota_ida_coords.append(atual)

            anterior = atual

        if ctes_mesma_origem_destino:
            amostra_ctes = ", ".join(ctes_mesma_origem_destino[:5])
            self.logger.warning(
                f"⚠️ Rota {rota_id} ignorou {len(ctes_mesma_origem_destino)} chamadas com origem == destino no cluster {cluster_id}. "
                f"Amostra: {amostra_ctes}"
            )

        tempo_parcial += sum(self._calcular_tempo_servico_entrega(row) for _, row in df_ordenado.iterrows())

        rota_volta_coords = []
        if anterior == origem:
            self.logger.warning("⚠️ Ignorando retorno: origem == último ponto.")
            rota_volta_coords.append(origem)
            tempo_back = 0.0
            dist_back = 0.0
        else:
            dist_back, tempo_back, rota_back, fonte_rota_back = obter_rota_last_mile_detalhada(
                anterior,
                origem,
                self.tenant_id,
                self.simulation_db,
                self.simulation_db,
                self.logger,
                velocidade_media_kmh,
            )
            try:
                dist_back = float(dist_back) if dist_back is not None else 0.0
                tempo_back = float(tempo_back) if tempo_back is not None else 0.0
                fontes_metricas.add(fonte_rota_back)
            except Exception as e:
                self.logger.warning(f"⚠️ Erro ao processar retorno para rota {rota_id}: {e}")
                dist_back = 0.0
                tempo_back = 0.0

            if rota_back:
                rota_volta_coords.extend([(p["lat"], p["lon"]) for p in rota_back])
            else:
                rota_volta_coords.append(origem)

        if rota_ida_coords:
            sequencia_coord.extend(rota_ida_coords)
        if rota_volta_coords:
            sequencia_coord.extend(rota_volta_coords)
        if not sequencia_coord:
            self.logger.warning(
                f"⚠️ Coordenadas ausentes na ida e volta da rota {rota_id}. Marcando coordenadas_seq como None."
            )
            sequencia_coord = None

        tempo_total_real = tempo_parcial + tempo_back
        self.logger.info(
            f"📏 Rota {rota_id} — distância parcial: {distancia_parcial:.2f} km, retorno: {dist_back:.2f} km, total: {distancia_parcial + dist_back:.2f} km"
        )
        self.logger.info(
            f"⏱️ Rota {rota_id} — tempo parcial: {tempo_parcial:.2f} min, retorno: {tempo_back:.2f} min, total: {tempo_total_real:.2f} min"
        )

        if tempo_total_real > tempo_limite and not aceitar_excedente:
            self._registrar_tentativa_rota(
                cluster_id=cluster_id,
                rota_id=rota_id,
                branch_label=rota_label,
                tipo_veiculo=tipo_veiculo,
                qtde_entregas=len(df_ordenado),
                tempo_total_min=tempo_total_real,
                tempo_limite_min=tempo_limite,
                distancia_total_km=distancia_parcial + dist_back,
                ignoradas_mesmo_ponto=len(ctes_mesma_origem_destino),
                status='inviavel',
            )
            self.logger.warning(
                f"⛔ Rota {rota_id} excede o tempo máximo permitido ({tempo_total_real:.2f} > {tempo_limite:.2f}) minutos. Refinando localmente antes de aceitar excedente..."
            )
            return {"excedeu": True, "df": df_ordenado}

        if tempo_total_real > tempo_limite and aceitar_excedente:
            self.logger.warning(
                f"⚠️ Rota {rota_id} excede o tempo máximo permitido ({tempo_total_real:.2f} > {tempo_limite:.2f}) minutos, mas foi aceita como fallback final pois permitir_rotas_excedentes=True."
            )

        self._registrar_tentativa_rota(
            cluster_id=cluster_id,
            rota_id=rota_id,
            branch_label=rota_label,
            tipo_veiculo=tipo_veiculo,
            qtde_entregas=len(df_ordenado),
            tempo_total_min=tempo_total_real,
            tempo_limite_min=tempo_limite,
            distancia_total_km=distancia_parcial + dist_back,
            ignoradas_mesmo_ponto=len(ctes_mesma_origem_destino),
            status='fallback_excedente' if tempo_total_real > tempo_limite else 'viavel_sla',
        )

        detalhes_cluster = []
        for posicao_rota, (_, row) in enumerate(df_ordenado.iterrows()):
            origem_igual_destino = (
                round(row["destino_latitude"], 6) == round(df_ordenado['centro_lat'].iloc[0], 6)
                and round(row["destino_longitude"], 6) == round(df_ordenado['centro_lon'].iloc[0], 6)
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
                "distancia_total_km": round(distancia_parcial + dist_back, 2) if posicao_rota == 0 else None,
                "tempo_total_min": round(tempo_total_real, 2) if posicao_rota == 0 else None,
                "distancia_parcial_km": round(distancia_parcial, 2) if posicao_rota == 0 else None,
                "tempo_parcial_min": round(tempo_parcial, 2) if posicao_rota == 0 else None,
                "fonte_metricas": (
                    "osrm"
                    if fontes_metricas.issubset({"osrm", "cache_osrm", "fallback_minimo"})
                    else "fallback"
                    if fontes_metricas.issubset({
                        "osrm", "cache_osrm", "fallback_minimo", "google", "manual_haversine"
                    })
                    else "nao_osrm"
                ) if posicao_rota == 0 else None,
                "latitude": row["destino_latitude"],
                "longitude": row["destino_longitude"],
                "coordenadas_seq": sequencia_coord if posicao_rota == 0 else None,
                "entrega_com_rota": not origem_igual_destino,
            })

        return {"excedeu": False, "detalhes": detalhes_cluster}

    def rotear_last_mile(
        self,
        df_clusterizado: pd.DataFrame,
        k_clusters: int,
        tempo_maximo: Optional[int] = None,
    ) -> pd.DataFrame:
        self.logger.info("📦 Iniciando roteirização de last-mile...")

        df_tarifas = carregar_tarifas_last_mile(self.simulation_db, self.tenant_id)
        detalhes_totais = []
        self._route_attempts_by_cluster = {}

        for cluster_id, df_cluster in df_clusterizado.groupby('cluster'):
            self.logger.info(f"➔ Processando cluster {cluster_id} com {len(df_cluster)} entregas")

            df_cluster = df_cluster.copy()
            df_cluster['cte_peso'] = pd.to_numeric(
                df_cluster['cte_peso'], errors='coerce'
            ).fillna(0.0)
            df_cluster['cte_volumes'] = pd.to_numeric(
                df_cluster['cte_volumes'], errors='coerce'
            ).fillna(0).astype(int)
            df_cluster['cte_numero'] = df_cluster['cte_numero'].astype(str)
            if 'cte_tempo_atendimento_min' in df_cluster.columns:
                df_cluster['cte_tempo_atendimento_min'] = pd.to_numeric(
                    df_cluster['cte_tempo_atendimento_min'],
                    errors='coerce'
                )

            df_coords = self._carregar_coords_cluster(df_cluster, k_clusters)

            df_coords = pd.merge(
                df_coords,
                df_cluster[
                    [
                        coluna
                        for coluna in [
                            'cte_numero',
                            'cte_peso',
                            'cte_volumes',
                            'cte_tempo_atendimento_min',
                        ]
                        if coluna in df_cluster.columns
                    ]
                ],
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
                self.logger.warning(
                    f"⚠️ Cluster {cluster_id} sem coordenadas válidas para last-mile. Ignorando cluster."
                )
                continue

            if coordenadas.shape[0] == 1:
                self.logger.info(
                    f"📍 Cluster {cluster_id} possui uma única entrega; "
                    "roteirização unitária será mantida."
                )

            velocidade_media_kmh = self._obter_velocidade_media_kmh()
            tempo_limite = tempo_maximo or self.parametros['tempo_maximo_roteirizacao']
            pendentes = self._gerar_subclusters_iniciais(df_coords)
            detalhes_cluster = []
            refinamentos_realizados = 0
            max_tentativas_refino_subcluster = self._obter_limite_refino_subcluster()
            max_tentativas_refino = self._obter_limite_refino_global_emergencial(len(df_coords))
            cluster_cidade = df_cluster['cluster_cidade'].iloc[0]

            self.logger.info(
                f"🧮 Limites de refinamento do cluster {cluster_id}: "
                f"por_subcluster={max_tentativas_refino_subcluster} | guarda_global={max_tentativas_refino}"
            )

            while pendentes:
                if refinamentos_realizados >= max_tentativas_refino:
                    if self.permitir_rotas_excedentes:
                        self.logger.warning(
                            f"⚠️ Guarda global de refinamentos atingida no cluster {cluster_id}. "
                            "Continuando com os subclusters restantes pois permitir_rotas_excedentes=True."
                        )
                        fallback_pendentes = list(pendentes)
                        pendentes = []
                        for branch_pendente, df_pendente in fallback_pendentes:
                            resolvidos = self._montar_subcluster_excedente(
                                df_sub=df_pendente,
                                df_tarifas=df_tarifas,
                                tempo_maximo=tempo_limite,
                                cluster_cidade=cluster_cidade,
                                branch_label=branch_pendente,
                                motivo="limite global de refinamentos do cluster atingido",
                            )
                            for candidato in resolvidos:
                                resultado_rota = self._montar_detalhes_rota(
                                    cluster_id=cluster_id,
                                    rota_label=candidato['label'],
                                    df_ordenado=candidato['df'],
                                    tipo_veiculo=candidato['tipo_veiculo'],
                                    tempo_limite=tempo_limite,
                                    velocidade_media_kmh=velocidade_media_kmh,
                                    aceitar_excedente=True,
                                )
                                detalhes_cluster.extend(resultado_rota['detalhes'])
                        break

                    msg = (
                        f"❌ Falha ao encontrar subclusters viáveis no cluster {cluster_id} "
                        f"com {len(df_coords)} entregas após {refinamentos_realizados} refinamentos locais."
                    )
                    self._emitir_resumo_tentativas_cluster(cluster_id, status_final='falha')
                    self.logger.error(msg)
                    raise RuntimeError(msg)

                branch_label, df_sub = pendentes.pop(0)
                resolvidos = self._resolver_subcluster_localmente(
                    df_sub=df_sub,
                    df_tarifas=df_tarifas,
                    tempo_maximo=tempo_limite,
                    cluster_cidade=cluster_cidade,
                    branch_label=branch_label,
                )
                if not resolvidos:
                    msg = (
                        f"❌ Falha ao encontrar subclusters viáveis no cluster {cluster_id}. "
                        f"Nenhum cluster viável para roteirização."
                    )
                    self._emitir_resumo_tentativas_cluster(cluster_id, status_final='falha')
                    self.logger.error(msg)
                    raise RuntimeError(msg)

                for candidato in resolvidos:
                    resultado_rota = self._montar_detalhes_rota(
                        cluster_id=cluster_id,
                        rota_label=candidato['label'],
                        df_ordenado=candidato['df'],
                        tipo_veiculo=candidato['tipo_veiculo'],
                        tempo_limite=tempo_limite,
                        velocidade_media_kmh=velocidade_media_kmh,
                    )
                    if resultado_rota['excedeu']:
                        refinamentos_branch = self._contar_refinamentos_branch(candidato['label'])
                        if refinamentos_branch >= max_tentativas_refino_subcluster:
                            if self.permitir_rotas_excedentes:
                                self.logger.warning(
                                    f"⚠️ Subcluster {candidato['label']} atingiu o limite fixo de refinamentos "
                                    f"({refinamentos_branch}/{max_tentativas_refino_subcluster}). "
                                    "Aceitando excedente como fallback final."
                                )
                                resultado_excedente = self._montar_detalhes_rota(
                                    cluster_id=cluster_id,
                                    rota_label=candidato['label'],
                                    df_ordenado=candidato['df'],
                                    tipo_veiculo=candidato['tipo_veiculo'],
                                    tempo_limite=tempo_limite,
                                    velocidade_media_kmh=velocidade_media_kmh,
                                    aceitar_excedente=True,
                                )
                                detalhes_cluster.extend(resultado_excedente['detalhes'])
                                continue

                            msg = (
                                f"❌ Falha ao encontrar subclusters viáveis no cluster {cluster_id}. "
                                f"Subcluster {candidato['label']} atingiu o limite fixo de refinamentos "
                                f"({max_tentativas_refino_subcluster})."
                            )
                            self._emitir_resumo_tentativas_cluster(cluster_id, status_final='falha')
                            self.logger.error(msg)
                            raise RuntimeError(msg)

                        refinado = False
                        for split_size in range(2, min(len(candidato['df']), 4) + 1):
                            filhos = dividir_subcluster_local(candidato['df'], split_size, logger=self.logger)
                            if not filhos:
                                continue
                            pendentes = [
                                (f"{candidato['label']}_r{split_size}_{child_idx}", df_filho)
                                for child_idx, (_, df_filho) in enumerate(filhos)
                            ] + pendentes
                            refinamentos_realizados += 1
                            refinado = True
                            break

                        if not refinado:
                            if self.permitir_rotas_excedentes:
                                self.logger.warning(
                                    f"⚠️ Subcluster {candidato['label']} permaneceu acima do limite após tentar splits locais. Aceitando excedente como fallback final."
                                )
                                resultado_excedente = self._montar_detalhes_rota(
                                    cluster_id=cluster_id,
                                    rota_label=candidato['label'],
                                    df_ordenado=candidato['df'],
                                    tipo_veiculo=candidato['tipo_veiculo'],
                                    tempo_limite=tempo_limite,
                                    velocidade_media_kmh=velocidade_media_kmh,
                                    aceitar_excedente=True,
                                )
                                detalhes_cluster.extend(resultado_excedente['detalhes'])
                                continue

                            msg = (
                                f"❌ Falha ao encontrar subclusters viáveis no cluster {cluster_id}. "
                                f"Subcluster excedeu tempo máximo após refinamento local."
                            )
                            self._emitir_resumo_tentativas_cluster(cluster_id, status_final='falha')
                            self.logger.error(msg)
                            raise RuntimeError(msg)
                        continue

                    detalhes_cluster.extend(resultado_rota['detalhes'])

            self._emitir_resumo_tentativas_cluster(cluster_id, status_final='sucesso')

            detalhes_totais.extend(detalhes_cluster)

        if not detalhes_totais:
            self.logger.warning("⚠️ Roteirização last-mile não gerou nenhuma rota válida.")
            return pd.DataFrame()

        df_detalhes = pd.DataFrame(detalhes_totais)
        self.logger.info(
            f"✅ Roteirização last-mile finalizada com {len(df_detalhes)} entregas em "
            f"{df_detalhes['rota_id'].nunique()} rotas."
        )
        return df_detalhes




    def salvar_rotas_last_mile_em_db(
        self,
        df_rotas: pd.DataFrame,
        tenant_id: str,
        envio_data: str,
        simulation_id: str,
        k_clusters: int,
        db_conn,
        auto_commit: bool = True,
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
        if auto_commit:
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

        # Usa explicitamente a linha-resumo da rota, evitando depender da ordem/índice original.
        df_resumo_base = df_rotas[df_rotas["distancia_parcial_km"].notnull()].copy()
        df_resumo = (
            df_resumo_base.sort_values(["rota_id", "ordem_entrega"])
            .drop_duplicates(subset=["rota_id"], keep="first")
            .loc[:, [
                "rota_id",
                "tipo_veiculo",
                "peso_total",
                "volumes_total",
                "distancia_total_km",
                "tempo_total_min",
                "distancia_parcial_km",
                "tempo_parcial_min",
            ]]
            .merge(df_qtdes, on="rota_id", how="left")
            .rename(columns={
                "peso_total": "peso_total_kg",
                "volumes_total": "qde_volumes"
            })
        )

        self.logger.info(
            f"📊 Resumo de persistência last-mile: rotas_unicas={df_rotas['rota_id'].nunique()} | resumos_montados={len(df_resumo)}"
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
                cluster = str(row.get('cluster') or '0')
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
                db_conn.rollback()
                self.logger.error(f"❌ Erro ao processar rota {i} / {row.get('rota_id')}: {e}")
                cursor.close()
                raise


        cursor.executemany(insert_resumo_sql, valores)
        if auto_commit:
            db_conn.commit()
        cursor.close()
        self.logger.info(f"✅ {len(valores)} resumos de rotas last-mile salvos com sucesso.")
