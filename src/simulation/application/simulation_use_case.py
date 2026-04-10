import uuid
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

from simulation.visualization.plot_simulation_cluster import plotar_mapa_clusterizacao_simulation
from simulation.visualization.plot_simulation_transfer import plotar_mapa_transferencias
from simulation.visualization.plot_simulation_last_mile import plotar_mapa_last_mile

from simulation.infrastructure.simulation_database_reader import carregar_cluster_costs, carregar_hub_por_id
from simulation.domain.cost_cluster_service import calcular_custo_clusters_por_scenario
from simulation.domain.simulation_service import SimulationService
from simulation.domain.clusterization_service import ClusterizationService
from simulation.domain.last_mile_routing_service import LastMileRoutingService
from simulation.domain.transfer_routing_service import TransferRoutingService
from simulation.domain.cost_last_mile_service import CostLastMileService
from simulation.domain.cost_transfer_service import CostTransferService
from simulation.domain.simulation_result_service import SimulationResultService
from simulation.utils.validations import validar_integridade_entregas_clusterizadas
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
from simulation.visualization.gerar_graficos_custos_simulacao import gerar_graficos_custos_por_envio
from simulation.visualization.gerador_relatorio_final import executar_geracao_relatorio_final


class SimulationUseCase:

    def __init__(self, tenant_id, envio_data, parametros, hub_id,
                 clusterization_db, simulation_db, logger,
                 modo_forcar=False, simulation_id=None,
                 output_dir="exports/simulation/maps",
                 permitir_rotas_excedentes=True):

        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.hub_id = hub_id
        self.parametros = parametros
        self.clusterization_db = clusterization_db
        self.simulation_db = simulation_db
        self.logger = logger
        self.modo_forcar = modo_forcar
        self.simulation_id = simulation_id or str(uuid.uuid4())
        self.output_dir = output_dir
        self.permitir_rotas_excedentes = permitir_rotas_excedentes

        self.simulation_service = SimulationService(tenant_id, envio_data, simulation_db, logger, hub_id=hub_id)
        self.cluster_service = ClusterizationService(clusterization_db, simulation_db, logger, tenant_id)

        self.last_mile_service = LastMileRoutingService(
            simulation_db=simulation_db,
            clusterization_db=clusterization_db,
            tenant_id=tenant_id,
            logger=logger,
            parametros=parametros,
            envio_data=envio_data,
            permitir_rotas_excedentes=permitir_rotas_excedentes
        )

        self.transfer_service = TransferRoutingService(
            clusterization_db, simulation_db, logger, tenant_id, parametros, hub_id
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
        self.result_service = SimulationResultService(simulation_db, logger)
        self.cenarios_invalidados = []

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
                parametros=self.parametros,
                envio_data=self.envio_data,
                permitir_rotas_excedentes=self.permitir_rotas_excedentes,
            )
            cost_last_mile_service = CostLastMileService(
                simulation_db,
                self.logger,
                self.tenant_id,
            )

            tempo_maximo = (
                self.parametros.get("tempo_maximo_k0")
                if int(k) == 0
                else self.parametros.get("tempo_maximo_roteirizacao")
            )

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
        with ThreadPoolExecutor(max_workers=8) as executor:
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

    def _executar_cenario(self, cenario, df_entregas, df_hub, df_outliers_geograficos=None):
        tipo = cenario.get("tipo")
        identificador = cenario.get("identificador", tipo)
        self.logger.info(f"🧪 Executando cenário explícito {identificador}")

        if tipo == "k_numero":
            return self._executar_simulacao_para_k(
                cenario["k_clusters"],
                df_entregas,
                df_hub,
                df_outliers_geograficos,
            )

        self._registrar_cenario_invalidado(
            identificador,
            f"tipo de cenário não suportado: {tipo}",
        )
        return None

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

            if df_rotas_last_mile is not None and not df_rotas_last_mile.empty:
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
            envio_data=str(self.envio_data),
            simulation_id=self.simulation_id,
            simulation_db=self.simulation_db,
            modo_forcar=self.modo_forcar
        )

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
        self.logger.info(f"📥 Carregando entregas para envio_data = {self.envio_data}, tenant_id = {self.tenant_id}")

        df_entregas_original = self.cluster_service.carregar_entregas(self.tenant_id, self.envio_data)

        if "latitude" in df_entregas_original.columns and "longitude" in df_entregas_original.columns:
            n_coordenadas = df_entregas_original[["latitude", "longitude"]].dropna().shape[0]
            self.logger.info(f"📍 {n_coordenadas} entregas com coordenadas válidas carregadas do clusterization_db.")
        else:
            self.logger.warning("⚠️ Colunas latitude/longitude não encontradas ao carregar entregas.")

        if df_entregas_original.empty:
            self.logger.warning(f"⚠️ Nenhuma entrega encontrada para {self.envio_data}. Simulação será ignorada.")
            return None

        if {"latitude", "longitude"}.issubset(df_entregas_original.columns):
            indices_coordenadas_validas = df_entregas_original[["latitude", "longitude"]].dropna().index
            mascara_coordenadas_validas = df_entregas_original.index.isin(indices_coordenadas_validas)
            qtd_invalidas = int((~mascara_coordenadas_validas).sum())
            if qtd_invalidas:
                self.logger.warning(
                    f"⚠️ {qtd_invalidas} entregas sem coordenadas válidas serão removidas da simulação."
                )
            df_entregas_original = df_entregas_original.loc[mascara_coordenadas_validas].copy()

        if df_entregas_original.empty:
            self.logger.warning(
                f"⚠️ Nenhuma entrega com coordenadas válidas encontrada para {self.envio_data}. Simulação será ignorada."
            )
            return None

        df_entregas_original, metadata_operacional = self.simulation_service.preparar_dados_operacionais_iniciais(
            df_entregas_original,
            self.parametros,
        )
        self.logger.info(
            "📦 Base operacional pronta "
            f"modo={metadata_operacional['modo_avaliacao']} "
            f"entregas={metadata_operacional['total_entregas']}"
        )

        hub_central = carregar_hub_por_id(self.simulation_db, self.tenant_id, self.hub_id)
        if not hub_central:
            raise ValueError(f"❌ Hub central com hub_id={self.hub_id} não encontrado para este tenant.")

        if not self.parametros.get("desativar_cluster_hub", False):
            self.logger.info(f"🧲 Atribuindo entregas próximas ao hub central como cluster especial")
            df_hub, df_entregas = ClusterizationService.atribuir_entregas_proximas_ao_hub_central(
                df_entregas=df_entregas_original,
                hubs=[hub_central],
                raio_km=self.parametros.get("raio_hub_km", 80.0)
            )
        else:
            self.logger.info("⚠️ Cluster especial do hub central desativado por parâmetro.")
            df_entregas = df_entregas_original.copy()
            df_hub = pd.DataFrame(columns=df_entregas.columns)

        if df_entregas.empty:
            if df_hub is not None and not df_hub.empty:
                self.logger.info(
                    "⚠️ Cluster especial do hub central absorveu todas as entregas; "
                    "o cenário Hub único (k=0) será executado sobre a base inteira e os "
                    "cenários numéricos, se existirem, serão avaliados apenas sobre a base "
                    "fora do raio do hub."
                )
            else:
                self.logger.warning(
                    f"⚠️ Nenhuma entrega encontrada para {self.envio_data}. Simulação será ignorada."
                )
                return None

        df_entregas_clusterizaveis, df_outliers_geograficos, metadata_outliers = (
            self.simulation_service.separar_outliers_geograficos(
                df_entregas,
                self.parametros,
            )
        )
        if metadata_outliers["qtd_outliers"]:
            self.logger.info(
                "🧭 Base principal ajustada para clusterização "
                f"entregas_clusterizaveis={len(df_entregas_clusterizaveis)} "
                f"outliers={metadata_outliers['qtd_outliers']}"
            )
        else:
            df_entregas_clusterizaveis = df_entregas
            self.logger.info(
                "🧭 Nenhum outlier geográfico separado antes da clusterização "
                f"entregas_clusterizaveis={len(df_entregas_clusterizaveis)} "
                f"limite_km={metadata_outliers['limite_km']}"
            )

        self._executar_simulacao_k0(df_entregas_original.copy())

        custos_totais: list[float] = []
        melhor_k = None
        menor_custo = float("inf")
        melhor_resultado = None

        k0 = self._buscar_resultado_k0()
        if k0:
            qtd = int(df_entregas_original["cte_numero"].nunique())
            custos_totais.append(k0["custo_total"])
            melhor_k = 0
            menor_custo = k0["custo_total"]
            melhor_resultado = {
                "simulation_id": self.simulation_id,
                "tenant_id": self.tenant_id,
                "envio_data": self.envio_data,
                "k_clusters": 0,
                "custo_total": k0["custo_total"],
                "quantidade_entregas": qtd,
                "custo_transferencia": k0["custo_transferencia"],
                "custo_last_mile": k0["custo_last_mile"],
                "custo_cluster": k0["custo_cluster"],
            }
            self.logger.info(f"🏁 Cenário Hub único (k=0) incluído como candidato inicial: custo_total={k0['custo_total']:.2f}")

        cenarios = self.simulation_service.gerar_cenarios_explicitos(
            df_entregas_original,
            self.parametros,
        )
        self.logger.info(
            "🧪 Total de cenários numéricos elegíveis para teste: "
            f"{len(cenarios)} | base_total={len(df_entregas_original)} | "
            f"base_fora_hub={len(df_entregas_clusterizaveis)}"
        )

        for indice_cenario, cenario in enumerate(cenarios, start=1):
            df_cenario = df_entregas_clusterizaveis
            df_hub_cenario = df_hub
            df_outliers_cenario = df_outliers_geograficos

            self.logger.info(
                "🧪 Tentando cenário numérico "
                f"{indice_cenario}/{len(cenarios)}: {cenario['identificador']} | "
                f"entregas_clusterizaveis={len(df_cenario)} | entregas_hub={len(df_hub_cenario)}"
            )

            if (
                df_cenario is None or df_cenario.empty
            ):
                self.logger.info(
                    "ℹ️ Cenário numérico não executado porque não restaram entregas fora "
                    "do raio do hub central. Isso é comportamento esperado para esta base."
                )
                continue

            resultado_k = self._executar_cenario(
                cenario,
                df_cenario,
                df_hub_cenario,
                df_outliers_cenario,
            )
            if resultado_k is None:
                continue

            custo_k = resultado_k["custo_total"]
            custos_totais.append(custo_k)
            k_resultado = resultado_k["k_clusters"]

            if custo_k < menor_custo:
                melhor_k = k_resultado
                menor_custo = custo_k
                melhor_resultado = {
                    **resultado_k,
                    "simulation_id": self.simulation_id,
                    "tenant_id": self.tenant_id,
                    "envio_data": self.envio_data,
                    "quantidade_entregas": len(df_entregas),
                }

            if self.simulation_service.verificar_ponto_inflexao_com_tendencia(custos_totais):
                self.logger.info("📉 Heurística de inflexão identificou ponto ótimo.")
                break

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
            self.logger.warning(
                f"⚠️ Nenhum cenário válido encontrado. Cenários invalidados: {resumo}"
            )

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
    ):
        is_ponto_otimo = False
        df_clusterizado["simulation_id"] = self.simulation_id
        df_clusterizado["k_clusters"] = k_persistencia

        df_clusterizado = self.cluster_service.ajustar_centros_dos_clusters(df_clusterizado)

        if df_outliers_geograficos is not None and not df_outliers_geograficos.empty:
            df_outliers_clusterizados = self.simulation_service.materializar_clusters_outliers(
                df_outliers_geograficos
            )
            df_clusterizado = pd.concat(
                [df_clusterizado, df_outliers_clusterizados],
                ignore_index=True,
                sort=False,
            )

        if df_hub is not None and not df_hub.empty:
            df_hub["simulation_id"] = self.simulation_id
            df_hub["k_clusters"] = k_persistencia
            df_hub["is_ponto_otimo"] = False
            df_clusterizado = pd.concat([df_clusterizado, df_hub], ignore_index=True)

        df_clusterizado = self.simulation_service.repartir_cluster_hub_central(
            df_clusterizado,
            self.parametros,
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
            df_resumo_clusters = (
                df_clusterizado.groupby("cluster").agg(qde_ctes=("cte_numero", "nunique")).reset_index()
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

        try:
            self._persistir_cenario_concluido(
                df_clusterizado=df_clusterizado,
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

        try:
            plotar_mapa_clusterizacao_simulation(
                simulation_db=self.simulation_db,
                clusterization_db=self.clusterization_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                k_clusters=k_persistencia,
                output_dir=self.output_dir,
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
                output_dir=self.output_dir,
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
                output_dir=self.output_dir,
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
        k_algoritmo = self._obter_k_algoritmo(k, df_hub)
        if k_algoritmo <= 0:
            self.logger.info(
                f"ℹ️ Cenário k={k} absorvido pelo cenário Hub único (k=0); nenhuma clusterização adicional necessária."
            )
            return None

        self.logger.info(
            f"⚙️ Processando simulação para k={k} | clusters_algoritmo={k_algoritmo}"
        )
        df_clusterizado = self.cluster_service.clusterizar(
            df_entregas,
            k=k_algoritmo,
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
        )
        return self._executar_simulacao_clusterizada(
            f"k={k}",
            k,
            df_clusterizado,
            df_hub,
            df_outliers_geograficos,
        )

    def _executar_simulacao_k0(self, df_entregas, df_hub=None):
        identificador_cenario = "k=0"
        self.logger.info("🚀 Executando simulação de last-mile do cenário Hub único (k=0)")
        if df_hub is not None and not df_hub.empty:
            df_entregas = pd.concat([df_entregas, df_hub], ignore_index=True)

        df_entregas["cluster"] = 0
        df_entregas["k_clusters"] = 0

        hub = self.simulation_service.buscar_hub_central()
        df_entregas["centro_lat"] = hub.latitude
        df_entregas["centro_lon"] = hub.longitude
        df_entregas["cluster_endereco"] = hub.nome
        df_entregas["cluster_cidade"] = hub.nome
        df_entregas["simulation_id"] = self.simulation_id
        df_entregas["is_ponto_otimo"] = False

        df_rotas_last_mile, custo_last_mile = self._executar_last_mile_para_dataframe_clusterizado(
            identificador_cenario,
            df_entregas,
            0,
        )
        if df_rotas_last_mile is None:
            return None

        try:
            cluster_cost_cfg = carregar_cluster_costs(self.simulation_db, self.tenant_id)
            df_resumo_clusters = (
                df_entregas.groupby("cluster").agg(qde_ctes=("cte_numero", "nunique")).reset_index()
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
        qtd = int(df_entregas["cte_numero"].nunique())

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
            self._persistir_cenario_concluido(
                df_clusterizado=df_entregas,
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

        try:
            plotar_mapa_last_mile(
                simulation_db=self.simulation_db,
                clusterization_db=self.clusterization_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                k_clusters=0,
                output_dir=self.output_dir,
                modo_forcar=self.modo_forcar,
                logger=self.logger
            )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro mapa last-mile k=0: {e}")

        return resultado_k0
