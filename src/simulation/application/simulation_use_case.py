
# hub_router_1.0.1/src/simulation/application/simulation_use_case.py

import os
import uuid
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from simulation.visualization.plot_simulation_cluster import \
    plotar_mapa_clusterizacao_simulation
from simulation.visualization.plot_simulation_transfer import \
    plotar_mapa_transferencias
from simulation.visualization.plot_simulation_last_mile import \
    plotar_mapa_last_mile

from simulation.infrastructure.simulation_database_reader import \
    carregar_cluster_costs, carregar_hub_por_id
from simulation.domain.cost_cluster_service import \
    calcular_custo_clusters_por_scenario
from simulation.domain.simulation_service import SimulationService
from simulation.domain.clusterization_service import ClusterizationService
from simulation.domain.last_mile_routing_service import LastMileRoutingService
from simulation.domain.transfer_routing_service import TransferRoutingService
from simulation.domain.cost_last_mile_service import CostLastMileService
from simulation.domain.cost_transfer_service import CostTransferService
from simulation.domain.simulation_result_service import SimulationResultService
from simulation.utils.validations import \
    validar_integridade_entregas_clusterizadas
from simulation.infrastructure.simulation_database_writer import (
    persistir_resumo_transferencias,
    salvar_detalhes_transferencias,
    salvar_rotas_transferencias,
)
from simulation.infrastructure.simulation_database_connection import (
    conectar_clusterization_db,
    conectar_simulation_db,
)
from simulation.domain.data_cleaner_service import DataCleanerService
from simulation.visualization.gerar_graficos_custos_simulacao import \
    gerar_graficos_custos_por_envio
from simulation.visualization.gerador_relatorio_final import \
    executar_geracao_relatorio_final
from simulation.utils.export_entregas_excel import \
    exportar_entregas_com_rotas_excel

from simulation.domain.entities import SimulationParams
from simulation.domain.strategy_resolver import resolver_estrategia
from simulation.domain.simulation_service import SimulationService


class SimulationUseCase:
    def __init__(
        self,
        tenant_id,
        envio_data,
        simulation_db,
        clusterization_db,
        logger,
        params=None,
        modo_forcar=False,
        simulation_id=None,
        permitir_rotas_excedentes=True,
        hub_id=None,
    ):
        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.simulation_db = simulation_db
        self.clusterization_db = clusterization_db
        self.logger = logger

        # 🔥 FONTE ÚNICA
        if not isinstance(params, SimulationParams):
            self.params = SimulationParams(**(params or {}))
        else:
            self.params = params

        # 🔥 GARANTE CONSISTÊNCIA DO PARAM
        self.params.permitir_rotas_excedentes = permitir_rotas_excedentes

        # 🔥 estratégia derivada
        self.estrategia = resolver_estrategia(self.params)

        # 🔥 LOG CORRETO
        self.logger.info(
            f"[SIMULATION CONFIG] modo={self.params.modo_simulacao} | "
            f"cluster={self.estrategia.algoritmo_clusterizacao} | "
            f"routing={self.estrategia.algoritmo_roteirizacao} | "
            f"velocidade_kmh={self.params.velocidade_kmh}"
        )

        self.logger.info(
            f"[PARAMS] raio_hub_km={self.params.raio_hub_km} | "
            f"tempo_max_k0={getattr(self.params, 'tempo_max_k0', 'NA')} | "
            f"tempo_max_roteirizacao={self.params.tempo_max_roteirizacao}"
        )

        self.modo_forcar = modo_forcar
        self.simulation_id = simulation_id or str(uuid.uuid4())
        self.permitir_rotas_excedentes = permitir_rotas_excedentes
        self.hub_id = hub_id
        self.output_dir = f"exports/simulation/entregas/{self.tenant_id}/{self.envio_data}"

        # cenários inválidos
        self.cenarios_invalidados = []

        # 🔥 SERVICES PADRONIZADOS (SEM ADAPTER)

        self.simulation_service = SimulationService(
            tenant_id,
            envio_data,
            simulation_db,
            logger,
            hub_id=hub_id
        )

        self.cluster_service = ClusterizationService(
            clusterization_db,
            simulation_db,
            logger,
            tenant_id
        )

        self.last_mile_service = LastMileRoutingService(
            simulation_db=simulation_db,
            clusterization_db=clusterization_db,
            tenant_id=tenant_id,
            logger=logger,
            params=self.params,  # 🔥 aqui muda tudo
            envio_data=envio_data,
            permitir_rotas_excedentes=permitir_rotas_excedentes
        )

        self.transfer_service = TransferRoutingService(
            clusterization_db=clusterization_db,
            simulation_db=simulation_db,
            logger=logger,
            tenant_id=tenant_id,
            params=self.params,  # 🔥 aqui também
            hub_id=hub_id
        )

        self.cost_last_mile_service = CostLastMileService(
            simulation_db,
            logger,
            tenant_id,
        )

        self.cost_transfer_service = CostTransferService(
            simulation_db,
            logger,
            tenant_id,
        )

        self.result_service = SimulationResultService(
            simulation_db,
            logger
        )

    def exportar_excel_entregas_rotas(self):
        """
        Gera o Excel de entregas+rotas para o tenant e data configurados.
        """
        try:
            output_dir = self.output_dir
            excel_path = exportar_entregas_com_rotas_excel(
                clusterization_db=self.clusterization_db,
                simulation_db=self.simulation_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                output_dir=output_dir
            )
            self.logger.info(f"✅ Excel de entregas+rotas exportado: {excel_path}")
            return excel_path
        except Exception as e:
            self.logger.warning(f"⚠️ Falha ao exportar Excel de entregas+rotas: {e}")
            return None
        self.cenarios_invalidados = []

    def _contar_especiais_na_rota(self, route, special_flags):
        total = 0
        for idx in route:
            if idx == 0:
                continue  # depot
            if 0 <= idx < len(special_flags) and special_flags[idx]:
                total += 1
        return total

    def _registrar_cenario_invalidado(self, k, motivo, detalhes=None):
        registro = {
            "k_clusters": k,
            "motivo": motivo,
        }
        if detalhes is not None:
            registro["detalhes"] = detalhes
        self.cenarios_invalidados.append(registro)
        sufixo_detalhes = ""
        if isinstance(detalhes, list) and detalhes:
            amostra = detalhes[0]
            if isinstance(amostra, dict):
                cluster_id = amostra.get("cluster_id")
                motivo_detalhe = amostra.get("motivo") or amostra.get("erro")
                sufixo_detalhes = (
                    f" | detalhes={len(detalhes)}"
                    f" | primeiro_cluster={cluster_id}"
                    f" | primeiro_motivo={motivo_detalhe}"
                )
        self.logger.warning(
            f"🚫 Cenário k={k} invalidado: {motivo}{sufixo_detalhes}"
        )

    @staticmethod
    def _todas_rotas_sem_metrica_osrm(fontes_metricas):
        fontes = [fonte for fonte in fontes_metricas if fonte is not None]
        if not fontes:
            return False
        return all(fonte == "nao_osrm" for fonte in fontes)

    # 🔹 Função auxiliar para processar clusters em paralelo
    def _processar_cluster_lastmile(self, cluster_id, df_cluster, k):
        """Processa roteirização e custo last-mile de 1 cluster (thread-safe)."""
        simulation_db = None
        clusterization_db = None
        try:
            simulation_db = conectar_simulation_db()
            clusterization_db = conectar_clusterization_db()

            last_mile_service = LastMileRoutingService(
                simulation_db=simulation_db,
                clusterization_db=clusterization_db,
                tenant_id=self.tenant_id,
                logger=self.logger,
                params=self.params,
                envio_data=self.envio_data,
                permitir_rotas_excedentes=self.permitir_rotas_excedentes,
            )
            cost_last_mile_service = CostLastMileService(
                simulation_db,
                self.logger,
                self.tenant_id,
            )

            tempo_maximo = getattr(self.params, "tempo_max_k0", self.params.tempo_max_roteirizacao)

            rotas = last_mile_service.rotear_last_mile(
                df_cluster,
                k_clusters=k,
                tempo_maximo=tempo_maximo,
            )
            if rotas is None or rotas.empty:
                self.logger.warning(f"⚠️ Nenhuma rota gerada para cluster {cluster_id} (k={k})")
                return {
                    "cluster_id": cluster_id,
                    "rotas": None,
                    "custo_last_mile": 0.0,
                    "erro": f"Nenhuma rota gerada para cluster {cluster_id} (k={k})",
                }

            custo_lm = cost_last_mile_service.calcular_custo(rotas)
            return {
                "cluster_id": cluster_id,
                "rotas": rotas,
                "custo_last_mile": custo_lm,
                "erro": None,
            }

        except Exception as e:
            self.logger.error(f"❌ Erro no cluster {cluster_id} (k={k}): {e}")
            return {
                "cluster_id": cluster_id,
                "rotas": None,
                "custo_last_mile": 0.0,
                "erro": str(e),
            }
        finally:
            try:
                if clusterization_db is not None:
                    clusterization_db.close()
            except Exception:
                pass
            try:
                if simulation_db is not None:
                    simulation_db.close()
            except Exception:
                pass


    def _buscar_resultado_k0(self):
        cursor = self.simulation_db.cursor()
        cursor.execute("""
            SELECT custo_total, custo_transferencia, custo_last_mile, custo_cluster
            FROM resultados_simulacao
            WHERE tenant_id = %s AND envio_data = %s AND simulation_id = %s AND k_clusters = 0
            ORDER BY created_at DESC
            LIMIT 1
        """, (self.tenant_id, self.envio_data, self.simulation_id))
        row = cursor.fetchone()
        cursor.close()

        if not row:
            return None

        custo_total, custo_transf, custo_lm, custo_cluster = row
        return {
            "custo_total": float(custo_total),
            "custo_transferencia": float(custo_transf or 0.0),
            "custo_last_mile": float(custo_lm or 0.0),
            "custo_cluster": float(custo_cluster or 0.0),
        }

    def _limpar_persistencia_cenario(self, k_clusters):
        deletes_por_tabela = {
            "rotas_transferencias": (
                """
                DELETE FROM rotas_transferencias
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, k_clusters),
            ),
            "detalhes_transferencias": (
                """
                DELETE FROM detalhes_transferencias
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND simulation_id = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, self.simulation_id, k_clusters),
            ),
            "resumo_transferencias": (
                """
                DELETE FROM resumo_transferencias
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND simulation_id = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, self.simulation_id, k_clusters),
            ),
            "rotas_last_mile": (
                """
                DELETE FROM rotas_last_mile
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND simulation_id = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, self.simulation_id, k_clusters),
            ),
            "resumo_rotas_last_mile": (
                """
                DELETE FROM resumo_rotas_last_mile
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND simulation_id = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, self.simulation_id, k_clusters),
            ),
            "entregas_clusterizadas": (
                """
                DELETE FROM entregas_clusterizadas
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND simulation_id = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, self.simulation_id, k_clusters),
            ),
            "resumo_clusters": (
                """
                DELETE FROM resumo_clusters
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND simulation_id = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, self.simulation_id, k_clusters),
            ),
            "resultados_simulacao": (
                """
                DELETE FROM resultados_simulacao
                WHERE tenant_id = %s
                  AND envio_data = %s
                  AND simulation_id = %s
                  AND k_clusters = %s
                """,
                (self.tenant_id, self.envio_data, self.simulation_id, k_clusters),
            ),
        }

        cursor = self.simulation_db.cursor()
        try:
            for query, params in deletes_por_tabela.values():
                cursor.execute(query, params)
            self.simulation_db.commit()
            self.logger.info(
                f"🧹 Persistência removida para cenário k={k_clusters} após invalidação."
            )
        except Exception as exc:
            self.simulation_db.rollback()
            self.logger.warning(
                f"⚠️ Falha ao limpar persistência do cenário k={k_clusters}: {exc}"
            )
        finally:
            cursor.close()

    def _executar_last_mile_para_dataframe_clusterizado(
        self,
        identificador_cenario,
        df_clusterizado,
        k_persistencia,
    ):
        resultados_clusters = []
        max_workers = min(8, os.cpu_count() or 4)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    self._processar_cluster_lastmile,
                    cid,
                    df_sub,
                    k_persistencia,
                ): cid
                for cid, df_sub in df_clusterizado.groupby("cluster")
            }
            for future in as_completed(futures):
                resultados_clusters.append(future.result())

        if not resultados_clusters:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "nenhum cluster retornou resultado no last-mile",
            )
            return None, None

        clusters_com_erro = [
            resultado for resultado in resultados_clusters if resultado.get("erro")
        ]
        if clusters_com_erro:
            ids_com_erro = ", ".join(
                str(resultado["cluster_id"]) for resultado in clusters_com_erro
            )
            detalhes = [
                {
                    "cluster_id": resultado["cluster_id"],
                    "erro": resultado.get("erro"),
                }
                for resultado in clusters_com_erro
            ]
            self._registrar_cenario_invalidado(
                identificador_cenario,
                f"falha de roteirização last-mile nos clusters {ids_com_erro}",
                detalhes=detalhes,
            )
            return None, None

        rotas_validas = [
            resultado["rotas"]
            for resultado in resultados_clusters
            if resultado["rotas"] is not None and not resultado["rotas"].empty
        ]
        if not rotas_validas:
            self.logger.warning(
                f"⚠️ Nenhuma rota last-mile válida foi gerada para {identificador_cenario}."
            )
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "last-mile não gerou rotas válidas",
            )
            return None, None

        df_rotas_last_mile = pd.concat(rotas_validas, ignore_index=True)
        df_fontes_last_mile = (
            df_rotas_last_mile[df_rotas_last_mile["fonte_metricas"].notnull()][
                ["rota_id", "fonte_metricas"]
            ]
            .drop_duplicates(subset=["rota_id"])
        )
        fontes_last_mile = df_fontes_last_mile["fonte_metricas"].tolist()
        if self._todas_rotas_sem_metrica_osrm(fontes_last_mile):
            rotas_last_mile_nao_osrm = df_fontes_last_mile[
                df_fontes_last_mile["fonte_metricas"] == "nao_osrm"
            ]["rota_id"].tolist()
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "last-mile sem métricas OSRM em todas as rotas",
                detalhes=[{"rota_id": rota_id} for rota_id in rotas_last_mile_nao_osrm],
            )
            return None, None

        custo_last_mile = sum(
            resultado["custo_last_mile"] for resultado in resultados_clusters
        )
        return df_rotas_last_mile, custo_last_mile

    def _executar_last_mile_time_windows_para_dataframe_clusterizado(
        self,
        identificador_cenario,
        df_clusterizado,
        k_persistencia,
    ):
        import math
        import pandas as pd
        from simulation.utils.ortools_time_windows import solve_time_windows_vrp

        self.logger.info("🚦 Entrou no método Time Windows (algoritmo_roteirizacao=time_windows)")

        if df_clusterizado is None or df_clusterizado.empty:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "base vazia para Time Windows",
            )
            return None, None

        df = df_clusterizado.copy()

        # --------------------------------------------------
        # 🔹 Normalização
        # --------------------------------------------------
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["cte_tempo_atendimento_min"] = pd.to_numeric(
            df.get("cte_tempo_atendimento_min"),
            errors="coerce",
        ).fillna(0)

        df = df.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)

        if df.empty:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "sem coordenadas válidas",
            )
            return None, None

        # --------------------------------------------------
        # 🔹 Regra de especiais
        # --------------------------------------------------
        tempo_especial_min = self.params.tempo_especial_min
        tempo_especial_max = self.params.tempo_especial_max
        max_especiais_por_rota = self.params.max_especiais_por_rota

        df["is_especial"] = df["cte_tempo_atendimento_min"].between(
            tempo_especial_min,
            tempo_especial_max,
            inclusive="both",
        )

        total_especiais = int(df["is_especial"].sum())
        total_entregas = len(df)

        self.logger.info(
            f"⏳ TW | entregas={total_entregas} | especiais={total_especiais} | "
            f"limite_por_rota={max_especiais_por_rota}"
        )

        # --------------------------------------------------
        # 🔹 Depot (hub real)
        # --------------------------------------------------
        hub = self.simulation_service.buscar_hub_central()
        depot_location = (float(hub.latitude), float(hub.longitude))

        # --------------------------------------------------
        # 🔥 BALANCEAMENTO DE ESPECIAIS (SEEDING)
        # --------------------------------------------------
        df_especiais = df[df["is_especial"]].copy()
        df_normais = df[~df["is_especial"]].copy()

        # 🔹 parâmetros
        entregas_por_rota = self.params.entregas_por_rota

        # 🔹 mínimo de rotas por especiais
        min_rotas_por_especiais = (
            max(1, math.ceil(len(df_especiais) / max_especiais_por_rota))
            if len(df_especiais) > 0 else 1
        )

        # 🔹 rotas por volume (CORRIGIDO)
        rotas_por_volume = max(
            1,
            math.ceil(len(df) / entregas_por_rota)
        )

        # 🔹 seed inicial
        num_rotas_seed = max(min_rotas_por_especiais, rotas_por_volume)

        # --------------------------------------------------
        # 🔹 Definição de veículos (ANTES DO SEED FINAL)
        # --------------------------------------------------
        tempo_max = self.params.tempo_max_roteirizacao

        min_veiculos_especial = (
            max(1, math.ceil(total_especiais / max_especiais_por_rota))
            if total_especiais > 0 else 1
        )

        veiculos_por_volume = max(
            1,
            math.ceil(total_entregas / entregas_por_rota)
        )

        num_vehicles = max(min_veiculos_especial, veiculos_por_volume)

        # --------------------------------------------------
        # 🔥 CORREÇÃO CRÍTICA: ALINHAR SEED COM VEÍCULOS
        # --------------------------------------------------
        if num_rotas_seed < num_vehicles:
            self.logger.warning(
                f"⚠️ Ajustando rotas_seed de {num_rotas_seed} para {num_vehicles} "
                f"para respeitar capacidade de veículos"
            )
            num_rotas_seed = num_vehicles

        self.logger.info(
            f"🧠 SEEDING | especiais={len(df_especiais)} | "
            f"rotas_seed={num_rotas_seed} | "
            f"min_rotas_por_especiais={min_rotas_por_especiais} | "
            f"rotas_por_volume={rotas_por_volume}"
        )

        # --------------------------------------------------
        # 🔥 CRIAÇÃO DOS BUCKETS (AGORA CORRETO)
        # --------------------------------------------------
        buckets = [[] for _ in range(num_rotas_seed)]

        # distribui especiais primeiro
        for i, (_, row) in enumerate(df_especiais.iterrows()):
            bucket_idx = i % num_rotas_seed
            buckets[bucket_idx].append(row)

        # embaralha normais
        df_normais = df_normais.sample(frac=1, random_state=42).reset_index(drop=True)

        # distribui normais
        for i, (_, row) in enumerate(df_normais.iterrows()):
            bucket_idx = i % num_rotas_seed
            buckets[bucket_idx].append(row)

        df_rebalanceado = pd.concat(
            [pd.DataFrame(bucket) for bucket in buckets if len(bucket) > 0],
            ignore_index=True
        )

        df = df_rebalanceado.reset_index(drop=True)
        self.logger.info("✅ SEEDING aplicado antes do solver")

        # --------------------------------------------------
        # 🔥 VALIDAÇÃO FINAL DE CAPACIDADE
        # --------------------------------------------------
        capacidade_especiais = num_vehicles * max_especiais_por_rota

        if total_especiais > capacidade_especiais:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                (
                    f"inviável para TW: especiais={total_especiais} "
                    f"capacidade={capacidade_especiais} "
                    f"veiculos={num_vehicles} "
                    f"max_especiais_por_rota={max_especiais_por_rota}"
                ),
            )
            return None, None

        self.logger.info(
            f"🚚 TW veículos={num_vehicles} | "
            f"por_especial={min_veiculos_especial} | "
            f"por_volume={veiculos_por_volume} | "
            f"capacidade_especiais={capacidade_especiais}"
        )

        # --------------------------------------------------
        # 🔥 MONTAGEM DOS INPUTS DO SOLVER
        # --------------------------------------------------

        locations = list(zip(df["latitude"], df["longitude"]))

        service_times = []

        for _, row in df.iterrows():
            tempo_atendimento = row.get("cte_tempo_atendimento_min")

            if pd.notna(tempo_atendimento) and float(tempo_atendimento) > 0:
                service_times.append(float(tempo_atendimento))
            else:
                peso = float(row.get("cte_peso", 0))
                volumes = float(row.get("cte_volumes", 0))

                tempo_parada = (
                    self.params.tempo_parada_pesada
                    if peso > self.params.limite_peso_parada
                    else self.params.tempo_parada_leve
                )

                tempo_descarga = volumes * self.params.tempo_por_volume

                service_times.append(tempo_parada + tempo_descarga)

        permitir_rotas_excedentes = self.permitir_rotas_excedentes

        if permitir_rotas_excedentes:
            time_windows = [(0, 100000)] * len(locations)
        else:
            time_windows = [(0, tempo_max)] * len(locations)

        special_flags = df["is_especial"].tolist()

        self.logger.info(
            f"[TW INPUT] locations={len(locations)} | "
            f"service_times={len(service_times)} | "
            f"vehicles={num_vehicles} | "
            f"velocidade={self.params.velocidade_kmh} | "
            f"permitir_rotas_excedentes={permitir_rotas_excedentes}"
        )

        routes = solve_time_windows_vrp(
            locations=locations,
            special_flags=special_flags,
            time_windows=time_windows,
            service_times=service_times,
            params=self.params,
            depot_location=depot_location,
            num_vehicles=num_vehicles,
        )
        # 🔥 remove rotas vazias (proteção solver)
        routes = [r for r in routes if r and len(r) > 0]

        if not routes:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "solver retornou apenas rotas vazias",
            )
            return None, None

        # --------------------------------------------------
        # 🔹 Validação HARD (crítica)
        # --------------------------------------------------
        for rota_id, route in enumerate(routes, start=1):

            if not route:
                self.logger.warning(f"⚠️ Rota vazia detectada: tw_{rota_id}")
                continue

            qtd_especiais = self._contar_especiais_na_rota(route, special_flags)

            if qtd_especiais > max_especiais_por_rota:
                self._registrar_cenario_invalidado(
                    identificador_cenario,
                    f"rota {rota_id} violou limite de especiais",
                    detalhes=[{
                        "rota_id": rota_id,
                        "qtd_especiais": qtd_especiais,
                        "limite": max_especiais_por_rota
                    }]
                )
                return None, None

        self.logger.info("✅ TW respeitou limite de especiais por rota")

        # --------------------------------------------------
        # 🔎 DEBUG OPERACIONAL POR ROTA
        # --------------------------------------------------
        resumo_rotas = []

        for rota_id, route in enumerate(routes, start=1):

            if not route:
                self.logger.warning(f"⚠️ Rota vazia detectada: tw_{rota_id}")
                continue

            qtd_especiais = self._contar_especiais_na_rota(route, special_flags)
            qtd_entregas = len(route)

            resumo_rotas.append({
                "rota_id": f"tw_{rota_id}",
                "qtd_entregas": qtd_entregas,
                "qtd_especiais": qtd_especiais,
            })

            self.logger.info(
                f"📦 Rota tw_{rota_id} | entregas={qtd_entregas} | especiais={qtd_especiais}"
            )

        # resumo geral
        total_rotas = len(resumo_rotas)
        total_especiais_alocados = sum(r["qtd_especiais"] for r in resumo_rotas)

        self.logger.info(
            f"📊 RESUMO TW | rotas={total_rotas} | especiais_alocados={total_especiais_alocados} | especiais_total={total_especiais}"
        )

        # --------------------------------------------------
        # 🔹 Montagem DataFrame rotas
        # --------------------------------------------------
        rotas_list = []

        for rota_id, route in enumerate(routes, start=1):
            for ordem, idx in enumerate(route, start=1):

                if not (0 <= idx < len(df)):
                    self.logger.warning(
                        f"⚠️ Índice inválido: idx={idx} len(df)={len(df)} rota_id={rota_id}"
                    )
                    continue

                row = df.iloc[idx]

                rotas_list.append({
                    "rota_id": f"tw_{rota_id}",
                    "ordem_entrega": ordem,
                    "cte_numero": row.get("cte_numero"),
                    "latitude": row.get("latitude"),
                    "longitude": row.get("longitude"),
                    "cluster": row.get("cluster"),
                    "is_especial": row.get("is_especial"),
                    "cte_tempo_atendimento_min": row.get("cte_tempo_atendimento_min"),
                    "fonte_metricas": "ortools_time_windows",
                })



        df_rotas = pd.DataFrame(rotas_list)


        if "entrega_com_rota" not in df_rotas.columns:
            df_rotas["entrega_com_rota"] = True

        # 🔥 GARANTE TIPAGEM SEGURA
        df_rotas["latitude"] = pd.to_numeric(df_rotas["latitude"], errors="coerce")
        df_rotas["longitude"] = pd.to_numeric(df_rotas["longitude"], errors="coerce")
        df_rotas["ordem_entrega"] = pd.to_numeric(df_rotas["ordem_entrega"], errors="coerce").fillna(0).astype(int)
        df_rotas["cte_numero"] = df_rotas["cte_numero"].astype(str)

        # 🔥 DEBUG
        self.logger.info(f"DEBUG df_rotas (TW) shape: {df_rotas.shape}")
        self.logger.info(f"DEBUG df_rotas colunas: {df_rotas.columns.tolist()}")



        # === INTEGRAÇÃO OSRM + VEÍCULO + CUSTO REAL ===
        if not df_rotas.empty:
            # Merge informações das entregas originais
            df_rotas = df_rotas.merge(
                df[["cte_numero", "cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete"]],
                on="cte_numero", how="left"
            )

            # Carrega tarifas de veículos
            from simulation.infrastructure.simulation_database_reader import carregar_tarifas_last_mile
            from simulation.infrastructure.cache_routes import obter_rota_last_mile
            from last_mile_routing.domain.routing_utils import alocar_veiculo


            # Padroniza colunas de tarifas para evitar erros de chave
            df_tarifas = carregar_tarifas_last_mile(self.simulation_db, self.tenant_id)
            # Se vier com nomes alternativos, renomeia para padrão esperado
            if "capacidade_kg_min" in df_tarifas.columns:
                df_tarifas = df_tarifas.rename(columns={
                    "capacidade_kg_min": "peso_minimo_kg",
                    "capacidade_kg_max": "peso_maximo_kg"
                })
            if "tipo_veiculo" in df_tarifas.columns:
                df_tarifas = df_tarifas.rename(columns={"tipo_veiculo": "veiculo"})
            # Garante colunas de custo
            if "tarifa_km" in df_tarifas.columns and "custo_km" not in df_tarifas.columns:
                df_tarifas = df_tarifas.rename(columns={"tarifa_km": "custo_km"})
            if "tarifa_entrega" in df_tarifas.columns and "custo_entrega" not in df_tarifas.columns:
                df_tarifas = df_tarifas.rename(columns={"tarifa_entrega": "custo_entrega"})

            # Agrupa por rota_id para calcular resumos e preencher campos reais
            resumo_list = []

            for rota_id, df_rota in df_rotas.groupby("rota_id"):

                hub = self.simulation_service.buscar_hub_central()

                coords = [(float(hub.latitude), float(hub.longitude))]
                coords += list(zip(df_rota["latitude"], df_rota["longitude"]))

                sequencia_coord = []
                distancia_total = 0.0
                tempo_total = 0.0

                # 🔥 NÃO CALCULAR MAIS IDA NO LOOP
                distancia_ida = 0.0
                tempo_ida = 0.0

                for i in range(len(coords) - 1):
                    origem = coords[i]
                    destino = coords[i + 1]

                    try:
                        dist_km, tempo_min, rota_completa = obter_rota_last_mile(
                            origem,
                            destino,
                            self.tenant_id,
                            self.simulation_db,
                            self.simulation_db,
                            self.logger
                        )
                    except Exception as e:
                        self.logger.warning(f"⚠️ Erro OSRM rota {rota_id}: {e}")
                        dist_km, tempo_min, rota_completa = 0.0, 0.0, None

                    dist_km = dist_km or 0.0
                    tempo_min = tempo_min or 0.0

                    distancia_total += dist_km
                    tempo_total += tempo_min

                    # 🔥 ATENDIMENTO
                    if i < len(df_rota):
                        entrega = df_rota.iloc[i]

                        if pd.notnull(entrega.get("cte_tempo_atendimento_min")):
                            tempo_atendimento = float(entrega["cte_tempo_atendimento_min"])
                        else:
                            peso = float(entrega.get("cte_peso", 0))
                            volumes = float(entrega.get("cte_volumes", 0))

                            tempo_parada = (
                                self.params.tempo_parada_pesada
                                if peso > self.params.limite_peso_parada
                                else self.params.tempo_parada_leve
                            )

                            tempo_descarga = volumes * self.params.tempo_por_volume

                            tempo_atendimento = tempo_parada + tempo_descarga

                        tempo_total += tempo_atendimento

                    # 🔥 GEOMETRIA
                    if rota_completa and isinstance(rota_completa, list):
                        try:
                            sequencia_coord.extend([
                                {"lat": float(p["lat"]), "lon": float(p["lon"])}
                                for p in rota_completa
                                if p and "lat" in p and "lon" in p
                            ])
                        except Exception as e:
                            self.logger.warning(f"⚠️ Falha parse rota_completa: {e}")
                            sequencia_coord.append({
                                "lat": float(destino[0]),
                                "lon": float(destino[1])
                            })
                    else:
                        sequencia_coord.append({
                            "lat": float(destino[0]),
                            "lon": float(destino[1])
                        })

                # 🔥 AQUI É A CORREÇÃO PRINCIPAL
                distancia_ida = distancia_total
                tempo_ida = tempo_total

                # 🔥 RETORNO AO HUB
                origem = coords[-1]
                destino = coords[0]

                try:
                    dist_km, tempo_min, _ = obter_rota_last_mile(
                        origem,
                        destino,
                        self.tenant_id,
                        self.simulation_db,
                        self.simulation_db,
                        self.logger
                    )
                except Exception as e:
                    self.logger.warning(f"⚠️ Erro OSRM retorno rota {rota_id}: {e}")
                    dist_km, tempo_min = 0.0, 0.0

                dist_km = dist_km or 0.0
                tempo_min = tempo_min or 0.0

                distancia_total += dist_km
                tempo_total += tempo_min

                # 🔥 fallback geometria
                if not sequencia_coord:
                    self.logger.warning(f"⚠️ Rota {rota_id} sem geometria — fallback")
                    sequencia_coord = [
                        {"lat": float(lat), "lon": float(lon)}
                        for lat, lon in coords
                    ]

                # 🔥 métricas
                peso_total = df_rota["cte_peso"].sum()
                volumes_total = df_rota["cte_volumes"].sum()
                valor_total_nf = df_rota["cte_valor_nf"].sum()
                valor_total_frete = df_rota["cte_valor_frete"].sum()

                veiculo = alocar_veiculo(peso_total, df_tarifas)

                try:
                    tarifa_km = float(
                        df_tarifas[df_tarifas["veiculo"] == veiculo]["custo_km"].values[0]
                    )
                    tarifa_entrega = float(
                        df_tarifas[df_tarifas["veiculo"] == veiculo]["custo_entrega"].values[0]
                    )
                except Exception:
                    tarifa_km = 0.0
                    tarifa_entrega = 0.0

                custo_rota = distancia_total * tarifa_km + len(df_rota) * tarifa_entrega

                resumo_list.append({
                    "rota_id": rota_id,
                    "peso_total": peso_total,
                    "volumes_total": volumes_total,
                    "valor_total_nf": valor_total_nf,
                    "valor_total_frete": valor_total_frete,
                    "tipo_veiculo": veiculo,
                    "distancia_total_km": distancia_total,
                    "tempo_total_min": tempo_total,

                    # 🔥 AGORA CORRETOS
                    "distancia_parcial_km": distancia_ida,
                    "tempo_parcial_min": tempo_ida,

                    "custo_rota": custo_rota,
                    "coordenadas_seq": sequencia_coord
                })

            # 🔥 cria DataFrame resumo
            resumo = pd.DataFrame(resumo_list).set_index("rota_id")

            # 🔥 qtde entregas por rota
            resumo["qtde_entregas"] = df_rotas.groupby("rota_id")["cte_numero"].count()

            # 🔥 join final
            df_rotas = df_rotas.join(resumo, on="rota_id", rsuffix="_resumo")

            # 🔥 DEBUG FINAL
            self.logger.info(f"DEBUG resumo rotas: {resumo.shape}")
            self.logger.info(f"DEBUG df_rotas final: {df_rotas.shape}")


        # Garante colunas obrigatórias mesmo se DataFrame vazio
        for col in ["tipo_veiculo", "peso_total", "volumes_total", "valor_total_nf", "valor_total_frete", "distancia_total_km", "tempo_total_min", "distancia_parcial_km", "tempo_parcial_min"]:
            if col not in df_rotas.columns:
                df_rotas[col] = 0 if col.startswith("distancia") or col.startswith("tempo") else None

        # Gerar detalhes das entregas para persistência e mapas (compatível com fluxo padrão)
        detalhes_list = []
        for _, row in df_rotas.iterrows():
            detalhes_list.append({
                "cte_numero": row.get("cte_numero"),
                "rota_id": row.get("rota_id"),
                "ordem_entrega": row.get("ordem_entrega"),
                "latitude": row.get("latitude"),
                "longitude": row.get("longitude"),
                "cluster": row.get("cluster"),
                "is_especial": row.get("is_especial"),
                "cte_tempo_atendimento_min": row.get("cte_tempo_atendimento_min"),
                "peso": row.get("cte_peso"),
                "volumes": row.get("cte_volumes"),
                "valor_nf": row.get("cte_valor_nf"),
                "valor_frete": row.get("cte_valor_frete"),
                "distancia_parcial_km": row.get("distancia_parcial_km", 0.0),
                "tempo_parcial_min": row.get("tempo_parcial_min", 0.0),
                "distancia_total_km": row.get("distancia_total_km", 0.0),
                "tempo_total_min": row.get("tempo_total_min", 0.0),
                "tipo_veiculo": row.get("tipo_veiculo", "Desconhecido"),
            })
        df_detalhes = pd.DataFrame(detalhes_list)

        # 🔍 SANIDADE FINAL (CORRIGIDO)
        total_input = df["cte_numero"].nunique()
        total_output = df_rotas["cte_numero"].nunique()
        dropped_nodes = total_input - total_output

        if dropped_nodes > 0:
            self.logger.warning(
                f"⚠️ TW com perda de entregas | dropped={dropped_nodes} | input={total_input} | output={total_output}"
            )

            if not self.permitir_rotas_excedentes:
                raise Exception(
                    f"🚨 ERRO CRÍTICO TW: input={total_input} vs output={total_output}"
                )
        # 🔥 IDENTIFICA DROPPED
        ids_input = set(df["cte_numero"].astype(str))
        ids_output = set(df_rotas["cte_numero"].astype(str))

        dropped_ids = ids_input - ids_output

        if dropped_ids:
            df_dropped = df[df["cte_numero"].astype(str).isin(dropped_ids)].copy()

            df_dropped["rota_id"] = None
            df_dropped["ordem_entrega"] = None
            df_dropped["entrega_com_rota"] = False
            df_dropped["motivo"] = "nao_roteirizado_time_windows"

            self.logger.warning(f"⚠️ Entregas sem rota: {len(df_dropped)}")

            df_rotas["entrega_com_rota"] = True
            df_rotas = df_rotas[
                ~df_rotas["cte_numero"].isin(dropped_ids)
            ]

            df_rotas = pd.concat([df_rotas, df_dropped], ignore_index=True)

        if df_rotas.empty:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "sem rotas válidas",
            )
            return None, None

        # --------------------------------------------------
        # 🔹 Custo
        # --------------------------------------------------
        try:
            custo = self.cost_last_mile_service.calcular_custo(df_rotas)
        except Exception as e:
            self.logger.warning(f"⚠️ erro custo TW: {e}")
            custo = 0.0

        self.logger.info(
            f"🔍 SANIDADE TW OK | rotas={len(routes)} | "
            f"entregas_input={len(df)} | entregas_output={len(df_rotas)}"
        )

        return df_rotas, custo



    def _persistir_cenario_concluido(
        self,
        df_clusterizado,
        lista_resumo_transferencias,
        detalhes_transferencia_gerados,
        rotas_transferencia_geradas,
        df_rotas_last_mile,
        resultado,
    ):
        try:
            self.cluster_service.salvar_clusterizacao_em_db(
                df_clusterizado,
                self.simulation_id,
                self.envio_data,
                resultado["k_clusters"],
                auto_commit=False,
            )

            if rotas_transferencia_geradas:
                salvar_rotas_transferencias(
                    rotas_transferencia_geradas,
                    self.simulation_db,
                    auto_commit=False,
                )
            if detalhes_transferencia_gerados:
                salvar_detalhes_transferencias(
                    detalhes_transferencia_gerados,
                    self.simulation_db,
                    auto_commit=False,
                )
            if lista_resumo_transferencias:
                persistir_resumo_transferencias(
                    lista_resumo_transferencias,
                    self.simulation_db,
                    logger=self.logger,
                    tenant_id=self.tenant_id,
                    auto_commit=False,
                )

            # 🔥 DEBUG
            if df_rotas_last_mile is not None:
                self.logger.info(f"DEBUG antes salvar rotas_last_mile: {df_rotas_last_mile.shape}")

            # 🔥 DEBUG + garantia coordenadas_seq
            if df_rotas_last_mile is not None:
                self.logger.info(f"DEBUG rotas_last_mile colunas: {df_rotas_last_mile.columns.tolist()}")

            if df_rotas_last_mile is not None and "coordenadas_seq" not in df_rotas_last_mile.columns:
                self.logger.warning("⚠️ coordenadas_seq ausente — criando fallback")
                df_rotas_last_mile["coordenadas_seq"] = None

            # 🔥 PROTEÇÃO
            # 🔥 PROTEÇÃO + LIMPEZA JSON
            if df_rotas_last_mile is None or df_rotas_last_mile.empty:
                self.logger.warning("⚠️ df_rotas_last_mile vazio — não será persistido")
            else:
                import numpy as np

                # 🔥 1. REMOVE NaN (CRÍTICO PARA POSTGRES JSON)
                df_rotas_last_mile = df_rotas_last_mile.replace({np.nan: None})

                # 🔥 2. GARANTE JSON VÁLIDO EM coordenadas_seq
                if "coordenadas_seq" in df_rotas_last_mile.columns:
                    df_rotas_last_mile["coordenadas_seq"] = df_rotas_last_mile["coordenadas_seq"].apply(
                        lambda x: x if isinstance(x, list) else []
                    )

                # 🔥 DEBUG
                self.logger.info("🧼 Limpeza JSON aplicada antes do insert")

                # 🔥 INSERT
                self.last_mile_service.salvar_rotas_last_mile_em_db(
                    df_rotas_last_mile,
                    self.tenant_id,
                    self.envio_data,
                    self.simulation_id,
                    resultado["k_clusters"],
                    self.simulation_db,
                    auto_commit=False,
                )

            self.result_service.salvar_resultado(
                resultado,
                modo_forcar=self.modo_forcar,
                auto_commit=False,
            )
            self.simulation_db.commit()
        except Exception:
            self.simulation_db.rollback()
            raise

    def _obter_k_algoritmo(self, k_total, df_hub):
        return k_total

    def _finalizar_melhor_resultado(self, melhor_k, menor_custo, melhor_resultado):
        self.logger.info(f"📝 Atualizando flag is_ponto_otimo=True para k={melhor_k}")
        cursor = self.simulation_db.cursor()
        for tabela in ["entregas_clusterizadas", "resumo_transferencias", "detalhes_transferencias"]:
            try:
                cursor.execute(f"""
                    UPDATE {tabela}
                    SET is_ponto_otimo = TRUE
                    WHERE tenant_id = %s AND envio_data = %s AND simulation_id = %s AND k_clusters = %s
                """, (self.tenant_id, self.envio_data, self.simulation_id, melhor_k))
            except Exception as e:
                self.logger.debug(f"ℹ️ UPDATE {tabela} ponto ótimo: {e}")
        self.simulation_db.commit()
        cursor.close()

        self.result_service.salvar_resultado({
            **melhor_resultado,
            "tenant_id": self.tenant_id,
            "envio_data": self.envio_data,
            "simulation_id": self.simulation_id,
            "is_ponto_otimo": True
        }, modo_forcar=self.modo_forcar)

        gerar_graficos_custos_por_envio(
            simulation_db=self.simulation_db,
            tenant_id=self.tenant_id,
            datas_filtradas=[self.envio_data],
            modo_forcar=self.modo_forcar
        )

        executar_geracao_relatorio_final(
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
            simulation_db=self.simulation_db,
            modo_forcar=self.modo_forcar
        )

        # Geração do Excel de entregas + rotas
        try:
            # output_dir padronizado
            output_dir = os.path.join("exports/simulation/entregas", self.tenant_id, str(self.envio_data))
            os.makedirs(output_dir, exist_ok=True)
            excel_path = exportar_entregas_com_rotas_excel(
                clusterization_db=self.clusterization_db,
                simulation_db=self.simulation_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                output_dir=output_dir
            )
            self.logger.info(f"✅ Excel de entregas+rotas exportado: {excel_path}")
        except Exception as e:
            self.logger.warning(f"⚠️ Falha ao exportar Excel de entregas+rotas: {e}")

        if self.cenarios_invalidados:
            resumo = "; ".join(
                f"k={item['k_clusters']}: {item['motivo']}"
                for item in self.cenarios_invalidados
            )
            self.logger.warning(
                f"⚠️ Cenários invalidados durante a execução: {resumo}"
            )

        return {
            "k_clusters": melhor_k,
            "custo_total": menor_custo,
            "cenarios_invalidados": list(self.cenarios_invalidados),
        }


    def executar_simulacao_completa(self):

        if not self.modo_forcar and self.simulation_service.simulacao_ja_existente():
            self.logger.warning(f"🚫 Simulação já existente para {self.envio_data}. Use --modo-forcar para sobrescrever.")
            return None

        if self.modo_forcar:
            cleaner = DataCleanerService(
                db_conn=self.simulation_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                logger=self.logger,
                output_dir=self.output_dir,
                simulation_id=self.simulation_id
            )
            cleaner.limpar_completo()

        self.logger.info("🔁 Iniciando execução completa da simulação.")
        self.logger.info(f"🆔 Simulation ID: {self.simulation_id}")

        # =============================
        # 🔹 CARREGAMENTO
        # =============================
        df_entregas_original = self.cluster_service.carregar_entregas(
            self.tenant_id, self.envio_data
        )

        if df_entregas_original.empty:
            self.logger.warning("⚠️ Nenhuma entrega encontrada.")
            return None

        # =============================
        # 🔹 PRÉ-PROCESSAMENTO
        # =============================
        df_entregas_original, metadata_operacional = (
            self.simulation_service.preparar_dados_operacionais_iniciais(
                df_entregas_original,
                self.params,
            )
        )
        # 🔥 DEBUG CRÍTICO DE COORDENADAS
        self.logger.info(
            f"[DEBUG] antes filtro coords | total={len(df_entregas_original)} | "
            f"lat_valid={df_entregas_original['latitude'].notna().sum()} | "
            f"lon_valid={df_entregas_original['longitude'].notna().sum()}"
        )

        # 🔥 FORÇA SANIDADE (NÃO DEIXA PIPELINE QUEBRAR)
        df_entregas_original = df_entregas_original.copy()

        df_entregas_original["latitude"] = pd.to_numeric(
            df_entregas_original["latitude"], errors="coerce"
        )
        df_entregas_original["longitude"] = pd.to_numeric(
            df_entregas_original["longitude"], errors="coerce"
        )

        df_entregas_original = df_entregas_original.dropna(
            subset=["latitude", "longitude"]
        )

        if df_entregas_original.empty:
            raise Exception(
                "❌ Todas as entregas ficaram sem coordenadas após preparação operacional."
            )

        self.logger.info(
            f"[DEBUG] após filtro coords | total={len(df_entregas_original)}"
        )
        hub_central = carregar_hub_por_id(
            self.simulation_db,
            self.tenant_id,
            self.hub_id
        )

        if not hub_central:
            raise ValueError("❌ Hub central não encontrado")

        # =============================
        # 🔹 CLUSTER HUB
        # =============================
        if not self.params.desativar_cluster_hub:
            df_hub, df_entregas = ClusterizationService.atribuir_entregas_proximas_ao_hub_central(
                df_entregas=df_entregas_original,
                hubs=[hub_central],
                raio_km=self.params.raio_hub_km
            )
        else:
            df_entregas = df_entregas_original.copy()
            df_hub = pd.DataFrame(columns=df_entregas.columns)

        # =============================
        # 🔹 OUTLIERS
        # =============================
        if not self.params.usar_outlier:
            self.logger.info("🚫 Outliers desativados — usando base pós-hub")
            df_entregas_clusterizaveis = df_entregas.copy()
            df_outliers_geograficos = pd.DataFrame(columns=df_entregas.columns)

        else:
            df_entregas_clusterizaveis, df_outliers_geograficos, _ = (
                self.simulation_service.separar_outliers_geograficos(
                    df_entregas,
                    self.params,
                )
            )

        # 🔥 DEBUG DO RAIO DO HUB (COLE AQUI)
        self.logger.info(
            f"📊 HUB={len(df_hub)} | FORA_HUB={len(df_entregas)} | CLUSTERIZAVEIS={len(df_entregas_clusterizaveis)}"
        )

        # 🔥 SANIDADE CRÍTICA ANTES DA CLUSTERIZAÇÃO
        # 🔥 SANIDADE CRÍTICA ANTES DA CLUSTERIZAÇÃO
        df_entregas_clusterizaveis = df_entregas_clusterizaveis.copy()

        if not df_entregas_clusterizaveis.empty:
            df_entregas_clusterizaveis["latitude"] = pd.to_numeric(
                df_entregas_clusterizaveis["latitude"], errors="coerce"
            )
            df_entregas_clusterizaveis["longitude"] = pd.to_numeric(
                df_entregas_clusterizaveis["longitude"], errors="coerce"
            )

            df_entregas_clusterizaveis = df_entregas_clusterizaveis.dropna(
                subset=["latitude", "longitude"]
            )

        self.logger.info(
            f"📍 Clusterizáveis válidas: {len(df_entregas_clusterizaveis)}"
        )

        # 🔥 NOVA REGRA:
        # se tudo caiu no hub, isso NÃO é erro; roda só k=0
        executar_apenas_k0 = False

        if df_entregas_clusterizaveis.empty:
            if df_hub is not None and not df_hub.empty:
                self.logger.info(
                    "🛑 Todas as entregas foram absorvidas pelo hub. "
                    "A simulação seguirá apenas com k=0."
                )
                executar_apenas_k0 = True
            else:
                raise Exception(
                    "❌ Nenhuma entrega com coordenadas válidas disponível para clusterização."
                )

        # =============================
        # 🔴 INICIALIZAÇÃO
        # =============================
        custos_totais = []
        melhor_k = None
        menor_custo = float("inf")
        melhor_resultado = None

        # =============================
        # 🔴 CENÁRIOS (AGORA COM k0)
        # =============================

        total_entregas = df_entregas_original["cte_numero"].nunique()

        k_values = SimulationService.gerar_range_k(
            total_entregas=total_entregas,
            min_cluster=self.params.min_entregas_por_cluster_alvo,
            max_cluster=self.params.max_entregas_por_cluster_alvo,
        )

        self.logger.info(f"🧪 Cenários de K gerados: {k_values}")

        # =============================
        # 🔴 EXECUÇÃO DOS CENÁRIOS
        # =============================

        # 🔴 K0 primeiro
        # 🔴 K0 primeiro
        self.logger.info("🧪 Executando cenário k=0")

        # 🔥 REGRA CORRETA: BASE TOTAL - OUTLIERS (SE ATIVADO)
        df_base_k0 = df_entregas_original.copy()

        if self.params.usar_outlier and df_outliers_geograficos is not None and not df_outliers_geograficos.empty:
            self.logger.info(f"➖ Removendo {len(df_outliers_geograficos)} outliers do k=0")

            df_base_k0 = df_base_k0[
                ~df_base_k0["cte_numero"].isin(df_outliers_geograficos["cte_numero"])
            ]
        else:
            self.logger.info("🚫 Outliers NÃO removidos do k=0 (default OFF)")

        # 🔥 LIMPEZA FINAL
        df_base_k0["latitude"] = pd.to_numeric(df_base_k0["latitude"], errors="coerce")
        df_base_k0["longitude"] = pd.to_numeric(df_base_k0["longitude"], errors="coerce")

        df_base_k0 = df_base_k0.dropna(subset=["latitude", "longitude"])
        df_base_k0 = df_base_k0.drop_duplicates(subset=["cte_numero"])

        resultado_k0 = self._executar_simulacao_k0(df_base_k0)

        if resultado_k0:
            custo_k0 = resultado_k0["custo_total"]
            custos_totais.append(custo_k0)
            melhor_k = 0
            menor_custo = custo_k0
            melhor_resultado = resultado_k0


        if executar_apenas_k0:
            k_values = []

        for k in k_values:

            self.logger.info(f"🧪 Executando cenário k={k}")

            resultado_k = self._executar_simulacao_para_k(
                k,
                df_entregas_clusterizaveis,  # 🔥 base dinâmica
                df_hub,
                None  # 🔥 NÃO deixa passar outlier
            )

            if resultado_k is None:
                continue

            custo_k = resultado_k["custo_total"]
            custos_totais.append(custo_k)

            if custo_k < menor_custo:
                melhor_k = resultado_k["k_clusters"]
                menor_custo = custo_k
                melhor_resultado = {
                    **resultado_k,
                    "simulation_id": self.simulation_id,
                    "tenant_id": self.tenant_id,
                    "envio_data": self.envio_data,
                    "quantidade_entregas": df_entregas_original["cte_numero"].nunique()
                }


        # =============================
        # 🔴 RESULTADO FINAL
        # =============================
        if melhor_k is not None and melhor_resultado is not None:
            return self._finalizar_melhor_resultado(
                melhor_k,
                menor_custo,
                melhor_resultado,
            )

        if self.cenarios_invalidados:
            resumo = "; ".join(
                f"k={item['k_clusters']}: {item['motivo']}"
                for item in self.cenarios_invalidados
            )
            self.logger.warning(f"⚠️ Cenários inválidos: {resumo}")

        return {
            "k_clusters": None,
            "custo_total": None,
            "cenarios_invalidados": list(self.cenarios_invalidados),
        }

    def _executar_simulacao_clusterizada(
        self,
        identificador_cenario,
        k_persistencia,
        df_clusterizado,
        df_hub,
        df_outliers_geograficos=None,
        df_cluster_puro=None  # 🔥 NOVO
    ):
        is_ponto_otimo = False
        df_clusterizado["simulation_id"] = self.simulation_id
        df_clusterizado["k_clusters"] = k_persistencia

        df_clusterizado = self.cluster_service.ajustar_centros_dos_clusters(df_clusterizado)

        # 🔴 BASE FINAL DO CENÁRIO (CORRETA)
        dfs = [df_clusterizado]

        # 🔹 outliers
        if df_outliers_geograficos is not None and not df_outliers_geograficos.empty:
            df_outliers_clusterizados = self.simulation_service.materializar_clusters_outliers(
                df_outliers_geograficos
            )
            dfs.append(df_outliers_clusterizados)

        # 🔹 hub
        if df_hub is not None and not df_hub.empty:
            df_hub = df_hub.copy()
            df_hub["simulation_id"] = self.simulation_id
            df_hub["k_clusters"] = k_persistencia
            df_hub["is_ponto_otimo"] = False
            dfs.append(df_hub)

        # 🔴 monta base final
        df_clusterizado = pd.concat(dfs, ignore_index=True)

        # 🔴 evita duplicidade
        df_clusterizado = df_clusterizado.drop_duplicates(subset=["cte_numero"])


        df_clusterizado = self.simulation_service.repartir_cluster_hub_central(
            df_clusterizado,
            self.params,
        )

        validar_integridade_entregas_clusterizadas(
            db_conn=self.simulation_db,
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
            k_clusters=k_persistencia,
            df_novo=df_clusterizado,
            logger=self.logger
        )

        df_clusterizado["is_ponto_otimo"] = is_ponto_otimo
        (
            lista_resumo_transferencias,
            detalhes_transferencia_gerados,
            rotas_transferencia_geradas,
        ) = self.transfer_service.rotear_transferencias_para_dataframe(
            df_clusterizado=df_clusterizado,
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
            k_clusters=k_persistencia,
            is_ponto_otimo=is_ponto_otimo,
            persistir=False,
        )
        if not detalhes_transferencia_gerados:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "roteirização de transferência não gerou detalhes",
            )
            return None

        fontes_transfer = [
            getattr(resumo, "fonte_metricas", None)
            for resumo in lista_resumo_transferencias
        ]
        if self._todas_rotas_sem_metrica_osrm(fontes_transfer):
            rotas_transfer_nao_osrm = [
                resumo.rota_id
                for resumo in lista_resumo_transferencias
                if getattr(resumo, "fonte_metricas", "nao_osrm") == "nao_osrm"
            ]
            self._registrar_cenario_invalidado(
                identificador_cenario,
                "transferência sem métricas OSRM em todas as rotas",
                detalhes=[{"rota_id": rota_id} for rota_id in rotas_transfer_nao_osrm],
            )
            return None

        routing_alg = self.estrategia.algoritmo_roteirizacao

        if routing_alg == "time_windows":
            df_rotas_last_mile, custo_last_mile = self._executar_last_mile_time_windows_para_dataframe_clusterizado(
                identificador_cenario,
                df_clusterizado,
                k_persistencia,
            )
        else:
            df_rotas_last_mile, custo_last_mile = self._executar_last_mile_para_dataframe_clusterizado(
                identificador_cenario,
                df_clusterizado,
                k_persistencia,
            )
        if df_rotas_last_mile is None:
            return None

        custo_transfer = self.cost_transfer_service.calcular_custo(lista_resumo_transferencias)

        try:
            cluster_cost_cfg = carregar_cluster_costs(self.simulation_db, self.tenant_id)
            df_base_cluster = df_cluster_puro if df_cluster_puro is not None else df_clusterizado

            df_resumo_clusters = (
                df_base_cluster.groupby("cluster").agg(qde_ctes=("cte_numero", "nunique")).reset_index()
            )
            custo_cluster = calcular_custo_clusters_por_scenario(
                df_resumo_clusters=df_resumo_clusters,
                entregas_minimas_por_cluster=cluster_cost_cfg["limite_qtd_entregas"],
                custo_minimo_cluster=cluster_cost_cfg["custo_fixo_diario"],
                custo_variavel_por_entrega=cluster_cost_cfg["custo_variavel_por_entrega"],
                logger=self.logger
            )
        except Exception as e:
            custo_cluster = 0.0
            self.logger.warning(f"⚠️ Falha ao calcular custo cluster: {e}")

        custo_total = custo_transfer + custo_last_mile + custo_cluster
        qtd = int(df_clusterizado["cte_numero"].nunique())

        resultado_cenario = {
            "simulation_id": self.simulation_id,
            "tenant_id": self.tenant_id,
            "envio_data": self.envio_data,
            "k_clusters": k_persistencia,
            "custo_total": custo_total,
            "quantidade_entregas": qtd,
            "custo_transferencia": custo_transfer,
            "custo_last_mile": custo_last_mile,
            "custo_cluster": custo_cluster,
            "is_ponto_otimo": False
        }

        # 🔴 GARANTE SCHEMA DO DF PURO
        df_para_persistir = df_cluster_puro.copy() if df_cluster_puro is not None else df_clusterizado.copy()

        df_para_persistir["simulation_id"] = self.simulation_id
        df_para_persistir["k_clusters"] = k_persistencia
        df_para_persistir["is_ponto_otimo"] = False

        try:
            self._persistir_cenario_concluido(
                df_clusterizado=df_para_persistir,
                lista_resumo_transferencias=lista_resumo_transferencias,
                detalhes_transferencia_gerados=detalhes_transferencia_gerados,
                rotas_transferencia_geradas=rotas_transferencia_geradas,
                df_rotas_last_mile=df_rotas_last_mile,
                resultado=resultado_cenario,
            )
        except Exception as e:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                f"falha ao persistir cenário: {e}",
            )
            self.logger.error(f"❌ Erro ao persistir cenário {identificador_cenario}: {e}")
            return None


        # Padroniza output_dir para todos os artefatos
        output_dir_maps = os.path.join("exports/simulation/maps", self.tenant_id, str(self.envio_data))

        try:
            plotar_mapa_clusterizacao_simulation(
                simulation_db=self.simulation_db,
                clusterization_db=self.clusterization_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                k_clusters=k_persistencia,
                output_dir=output_dir_maps,
                modo_forcar=self.modo_forcar,
                logger=self.logger
            )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro mapa cluster: {e}")

        try:
            plotar_mapa_transferencias(
                simulation_db=self.simulation_db,
                clusterization_db=self.clusterization_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                k_clusters=k_persistencia,
                output_dir=output_dir_maps,
                modo_forcar=self.modo_forcar,
                logger=self.logger
            )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro mapa transfer: {e}")

        try:
            plotar_mapa_last_mile(
            simulation_db=self.simulation_db,
            clusterization_db=self.clusterization_db,
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            k_clusters=k_persistencia,
            output_dir=output_dir_maps,
            modo_forcar=self.modo_forcar,
            logger=self.logger
        )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro mapa last-mile: {e}")

        return {
            "k_clusters": k_persistencia,
            "custo_total": custo_total,
            "custo_transferencia": custo_transfer,
            "custo_last_mile": custo_last_mile,
            "custo_cluster": custo_cluster
        }

    def _executar_simulacao_para_k(self, k, df_entregas, df_hub, df_outliers_geograficos=None):

        # 🔥 PROTEÇÃO CRÍTICA
        df_entregas = df_entregas.copy()

        # 🔥 GARANTE COORDENADAS
        df_entregas["latitude"] = pd.to_numeric(df_entregas["latitude"], errors="coerce")
        df_entregas["longitude"] = pd.to_numeric(df_entregas["longitude"], errors="coerce")

        df_entregas = df_entregas.dropna(subset=["latitude", "longitude"])

        if df_entregas.empty:
            self.logger.warning("⚠️ Sem coordenadas válidas para clusterização")
            return None

        k_algoritmo = k
        if k_algoritmo <= 0:
            self.logger.info(
                f"ℹ️ Cenário k={k} absorvido pelo cenário Hub único (k=0); nenhuma clusterização adicional necessária."
            )
            return None

        self.logger.info(
            f"⚙️ Processando simulação para k={k} | clusters_algoritmo={k_algoritmo}"
        )

        cluster_alg = self.estrategia.algoritmo_clusterizacao

        df_clusterizado = self.cluster_service.clusterizar(
            df_entregas,
            k=k_algoritmo,
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
            algoritmo=cluster_alg,
            entregas_por_rota=self.params.entregas_por_rota
        )

        # 🔴 SNAPSHOT LIMPO
        df_cluster_puro = df_clusterizado.copy()

        return self._executar_simulacao_clusterizada(
            f"k={k}",
            k,
            df_clusterizado,
            df_hub,
            df_outliers_geograficos,
            df_cluster_puro  # 🔥 NOVO
        )

    def _executar_simulacao_k0(self, df_entregas, df_hub=None):
        identificador_cenario = "k=0"
        self.logger.info("🚀 Executando simulação de last-mile do cenário Hub único (k=0)")

        if df_hub is not None and not df_hub.empty:
            self.logger.warning("⚠️ df_hub ignorado em k0 (já consolidado)")

        # 🔥 CORREÇÃO CRÍTICA: NÃO MUTAR DF ORIGINAL
        df_k0 = df_entregas.copy()

        df_k0["cluster"] = 0
        df_k0["k_clusters"] = 0

        hub = self.simulation_service.buscar_hub_central()
        df_k0["centro_lat"] = hub.latitude
        df_k0["centro_lon"] = hub.longitude
        df_k0["cluster_endereco"] = hub.nome
        df_k0["cluster_cidade"] = hub.nome
        df_k0["simulation_id"] = self.simulation_id
        df_k0["is_ponto_otimo"] = False

        routing_alg = self.estrategia.algoritmo_roteirizacao

        self.logger.info(f"🧠 Algoritmo de roteirização (k=0): {routing_alg}")

        if routing_alg == "time_windows":
            self.logger.info("⏳ k=0 usando Time Windows")

            df_rotas_last_mile, custo_last_mile = self._executar_last_mile_time_windows_para_dataframe_clusterizado(
                identificador_cenario,
                df_k0,
                0,
            )
        else:
            self.logger.info("🚚 k=0 usando roteirização padrão")

            df_rotas_last_mile, custo_last_mile = self._executar_last_mile_para_dataframe_clusterizado(
                identificador_cenario,
                df_k0,
                0,
            )

        if df_rotas_last_mile is None:
            return None

        try:
            cluster_cost_cfg = carregar_cluster_costs(self.simulation_db, self.tenant_id)

            df_resumo_clusters = (
                df_k0.groupby("cluster").agg(qde_ctes=("cte_numero", "nunique")).reset_index()
            )

            custo_cluster = calcular_custo_clusters_por_scenario(
                df_resumo_clusters=df_resumo_clusters,
                entregas_minimas_por_cluster=cluster_cost_cfg["limite_qtd_entregas"],
                custo_minimo_cluster=cluster_cost_cfg["custo_fixo_diario"],
                custo_variavel_por_entrega=cluster_cost_cfg["custo_variavel_por_entrega"],
                logger=self.logger
            )
        except Exception as e:
            custo_cluster = 0.0
            self.logger.warning(f"⚠️ Falha ao calcular custo cluster (k=0): {e}")

        custo_total = custo_last_mile + custo_cluster
        qtd = int(df_k0["cte_numero"].nunique())

        resultado_k0 = {
            "simulation_id": self.simulation_id,
            "tenant_id": self.tenant_id,
            "envio_data": self.envio_data,
            "k_clusters": 0,
            "quantidade_entregas": qtd,
            "custo_transferencia": 0.0,
            "custo_last_mile": custo_last_mile,
            "custo_cluster": custo_cluster,
            "custo_total": custo_total,
            "is_ponto_otimo": False
        }

        try:
            df_cluster_puro = df_k0.copy()

            df_cluster_puro["simulation_id"] = self.simulation_id
            df_cluster_puro["k_clusters"] = 0
            df_cluster_puro["is_ponto_otimo"] = False

            self._persistir_cenario_concluido(
                df_clusterizado=df_cluster_puro,
                lista_resumo_transferencias=[],
                detalhes_transferencia_gerados=[],
                rotas_transferencia_geradas=[],
                df_rotas_last_mile=df_rotas_last_mile,
                resultado=resultado_k0,
            )
        except Exception as e:
            self._registrar_cenario_invalidado(
                identificador_cenario,
                f"falha ao persistir cenário: {e}",
            )
            self.logger.error(f"❌ Erro ao persistir cenário {identificador_cenario}: {e}")
            return None

        self.logger.info(f"💾 Resultado k=0 salvo com custo_total={custo_total:.2f}")

        return resultado_k0
