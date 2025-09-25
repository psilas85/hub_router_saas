#hub_router_1.0.1/src/simulation/application/simulation_use_case.py

import uuid
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from simulation.utils.rate_limiter import RateLimiter

from simulation.visualization.plot_simulation_cluster import plotar_mapa_clusterizacao_simulation
from simulation.visualization.plot_simulation_transfer import plotar_mapa_transferencias
from simulation.visualization.plot_simulation_last_mile import plotar_mapa_last_mile

from simulation.infrastructure.simulation_database_reader import carregar_cluster_costs
from simulation.domain.cost_cluster_service import calcular_cluster_cost
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
    salvar_detalhes_transferencias
)
from simulation.domain.data_cleaner_service import DataCleanerService
from simulation.visualization.gerar_graficos_custos_simulacao import gerar_graficos_custos_por_envio
from simulation.visualization.gerador_relatorio_final import executar_geracao_relatorio_final


class SimulationUseCase:

    def __init__(self, tenant_id, envio_data, parametros, hub_id,
                 clusterization_db, simulation_db, logger,
                 modo_forcar=False, simulation_id=None,
                 output_dir="exports/simulation/maps",
                 fundir_clusters_pequenos=True,
                 permitir_rotas_excedentes=False):

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
        self.fundir_clusters_pequenos = fundir_clusters_pequenos
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
            clusterization_db, simulation_db, logger, tenant_id, parametros
        )
        self.cost_last_mile_service = CostLastMileService(simulation_db, logger)
        self.cost_transfer_service = CostTransferService(simulation_db, logger)
        self.result_service = SimulationResultService(simulation_db, logger)

        motor_rotas = parametros.get("motor_rotas", "osrm")
        if motor_rotas == "google":
            # Exemplo: 10 requests/s
            self.google_limiter = RateLimiter(max_calls_per_sec=10)
        else:
            self.google_limiter = None

    # 🔹 Função auxiliar para processar clusters em paralelo
    def _processar_cluster_lastmile(self, cluster_id, df_cluster, k):
        """Processa roteirização e custo last-mile de 1 cluster (thread-safe)."""
        try:
            # 🚦 Aplica rate limit apenas se for Google
            if self.google_limiter:
                self.google_limiter.wait()

            rotas = self.last_mile_service.rotear_last_mile(df_cluster, k_clusters=k)
            if rotas is None or rotas.empty:
                self.logger.warning(f"⚠️ Nenhuma rota gerada para cluster {cluster_id} (k={k})")
                return {"cluster_id": cluster_id, "rotas": None, "custo_last_mile": 0.0}

            custo_lm = self.cost_last_mile_service.calcular_custo(rotas)
            return {"cluster_id": cluster_id, "rotas": rotas, "custo_last_mile": custo_lm}

        except Exception as e:
            self.logger.error(f"❌ Erro no cluster {cluster_id} (k={k}): {e}")
            return {"cluster_id": cluster_id, "rotas": None, "custo_last_mile": 0.0}


    def _buscar_resultado_k1(self):
        cursor = self.simulation_db.cursor()
        cursor.execute("""
            SELECT custo_total, custo_transferencia, custo_last_mile, custo_cluster
            FROM resultados_simulacao
            WHERE tenant_id = %s AND envio_data = %s AND simulation_id = %s AND k_clusters = 1
            ORDER BY created_at DESC
            LIMIT 1
        """, (self.tenant_id, self.envio_data, self.simulation_id))
        row = cursor.fetchone()
        cursor.close()

        if not row:
            return None

        custo_total_k1, custo_transf_k1, custo_lm_k1, custo_cluster_k1 = row
        return {
            "custo_total": float(custo_total_k1),
            "custo_transferencia": float(custo_transf_k1 or 0.0),
            "custo_last_mile": float(custo_lm_k1 or 0.0),
            "custo_cluster": float(custo_cluster_k1 or 0.0),
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

        from simulation.infrastructure.simulation_database_reader import carregar_hubs
        hubs = carregar_hubs(self.simulation_db, self.tenant_id)

        if not self.parametros.get("desativar_cluster_hub", False):
            self.logger.info(f"🧲 Atribuindo entregas próximas ao hub central como cluster especial")
            df_hub, df_entregas = ClusterizationService.atribuir_entregas_proximas_ao_hub_central(
                df_entregas=df_entregas_original,
                hubs=hubs,
                raio_km=self.parametros.get("raio_hub_km", 80.0)
            )
        else:
            self.logger.info("⚠️ Cluster especial do hub central desativado por parâmetro.")
            df_entregas = df_entregas_original.copy()
            df_hub = pd.DataFrame(columns=df_entregas.columns)

        if df_entregas.empty:
            self.logger.warning(f"⚠️ Nenhuma entrega encontrada para {self.envio_data}. Simulação será ignorada.")
            return None

        self._executar_simulacao_k1(df_entregas, df_hub)

        custos_totais: list[float] = []
        melhor_k = None
        menor_custo = float("inf")
        melhor_resultado = None

        k1 = self._buscar_resultado_k1()
        if k1:
            qtd = int(df_entregas["cte_numero"].nunique())
            custos_totais.append(k1["custo_total"])
            melhor_k = 1
            menor_custo = k1["custo_total"]
            melhor_resultado = {
                "simulation_id": self.simulation_id,
                "tenant_id": self.tenant_id,
                "envio_data": self.envio_data,
                "k_clusters": 1,
                "custo_total": k1["custo_total"],
                "quantidade_entregas": qtd,
                "custo_transferencia": k1["custo_transferencia"],
                "custo_last_mile": k1["custo_last_mile"],
                "custo_cluster": k1["custo_cluster"],
            }
            self.logger.info(f"🏁 k=1 incluído como candidato inicial: custo_total={k1['custo_total']:.2f}")

        k_inicial = self.simulation_service.obter_k_inicial(
            df_entregas, self.parametros['k_min'], self.parametros['k_max']
        )
        lista_k = self.simulation_service.gerar_lista_k(
            k_inicial, self.parametros['k_min'], self.parametros['k_max']
        )

        for k in lista_k:
            resultado_k = self._executar_simulacao_para_k(k, df_entregas, df_hub)
            if resultado_k is None:
                continue

            custo_k = resultado_k["custo_total"]
            custos_totais.append(custo_k)

            if custo_k < menor_custo:
                melhor_k = k
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

            return {"k_clusters": melhor_k, "custo_total": menor_custo}

    def _executar_simulacao_para_k(self, k, df_entregas, df_hub):
        self.logger.info(f"⚙️ Processando simulação para k={k}")
        is_ponto_otimo = False

        df_clusterizado = self.cluster_service.clusterizar(
            df_entregas, k=k, tenant_id=self.tenant_id,
            envio_data=self.envio_data, simulation_id=self.simulation_id
        )
        df_clusterizado["simulation_id"] = self.simulation_id
        df_clusterizado["k_clusters"] = k

        if self.fundir_clusters_pequenos:
            df_clusterizado = self.cluster_service.fundir_clusters_pequenos(
                df_clusterizado, min_entregas=self.parametros['min_entregas_cluster']
            )
            self.logger.info("🔗 Clusters pequenos foram fundidos.")
        else:
            self.logger.info("⛔ Fusão de clusters pequenos desativada.")

        df_clusterizado = self.cluster_service.ajustar_centros_dos_clusters(df_clusterizado)

        if df_hub is not None and not df_hub.empty:
            df_hub["simulation_id"] = self.simulation_id
            df_hub["k_clusters"] = k
            df_hub["is_ponto_otimo"] = False
            df_clusterizado = pd.concat([df_clusterizado, df_hub], ignore_index=True)

        validar_integridade_entregas_clusterizadas(
            db_conn=self.simulation_db,
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
            k_clusters=k,
            df_novo=df_clusterizado,
            logger=self.logger
        )

        df_clusterizado["is_ponto_otimo"] = is_ponto_otimo
        self.cluster_service.salvar_clusterizacao_em_db(df_clusterizado, self.simulation_id, self.envio_data, k)

        lista_resumo_transferencias, detalhes_transferencia_gerados = self.transfer_service.rotear_transferencias(
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
            k_clusters=k,
            is_ponto_otimo=is_ponto_otimo
        )
        if not detalhes_transferencia_gerados:
            return None

        salvar_detalhes_transferencias(detalhes_transferencia_gerados, self.simulation_db)
        persistir_resumo_transferencias(
            lista_resumo_transferencias, self.simulation_db,
            logger=self.logger, tenant_id=self.tenant_id
        )

        # 🔹 NOVO: last-mile em paralelo por cluster
        resultados_clusters = []
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(self._processar_cluster_lastmile, cid, df_sub, k): cid
                for cid, df_sub in df_clusterizado.groupby("cluster")
            }
            for future in as_completed(futures):
                resultados_clusters.append(future.result())

        if not resultados_clusters:
            return None

        df_rotas_last_mile = pd.concat(
            [r["rotas"] for r in resultados_clusters if r["rotas"] is not None],
            ignore_index=True
        )
        custo_last_mile = sum(r["custo_last_mile"] for r in resultados_clusters)

        self.last_mile_service.salvar_rotas_last_mile_em_db(
            df_rotas_last_mile, self.tenant_id, self.envio_data,
            self.simulation_id, k, self.simulation_db
        )

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

        self.result_service.salvar_resultado({
            "simulation_id": self.simulation_id,
            "tenant_id": self.tenant_id,
            "envio_data": self.envio_data,
            "k_clusters": k,
            "custo_total": custo_total,
            "quantidade_entregas": qtd,
            "custo_transferencia": custo_transfer,
            "custo_last_mile": custo_last_mile,
            "custo_cluster": custo_cluster,
            "is_ponto_otimo": False
        }, modo_forcar=self.modo_forcar)

        try:
            plotar_mapa_clusterizacao_simulation(
                simulation_db=self.simulation_db,
                clusterization_db=self.clusterization_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                k_clusters=k,
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
                k_clusters=k,
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
                k_clusters=k,
                output_dir=self.output_dir,
                modo_forcar=self.modo_forcar,
                logger=self.logger
            )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro mapa last-mile: {e}")

        return {
            "k_clusters": k,
            "custo_total": custo_total,
            "custo_transferencia": custo_transfer,
            "custo_last_mile": custo_last_mile,
            "custo_cluster": custo_cluster
        }

    def _executar_simulacao_k1(self, df_entregas, df_hub=None):
        self.logger.info("🚀 Executando simulação especial de last-mile com k=1 (hub central)")
        if df_hub is not None and not df_hub.empty:
            df_entregas = pd.concat([df_entregas, df_hub], ignore_index=True)

        df_entregas["cluster"] = 0
        df_entregas["k_clusters"] = 1

        hub = self.simulation_service.buscar_hub_central()
        df_entregas["centro_lat"] = hub.latitude
        df_entregas["centro_lon"] = hub.longitude
        df_entregas["cluster_endereco"] = hub.nome
        df_entregas["cluster_cidade"] = hub.nome
        df_entregas["simulation_id"] = self.simulation_id
        df_entregas["is_ponto_otimo"] = False

        self.cluster_service.salvar_clusterizacao_em_db(df_entregas, self.simulation_id, self.envio_data, k_clusters=1)

        try:
            df_rotas_last_mile = self.last_mile_service.rotear_last_mile(
                df_entregas,
                k_clusters=1,
                tempo_maximo=self.parametros.get("tempo_maximo_k1", 600)
            )
        except Exception as e:
            self.logger.error(f"❌ Erro na roteirização last-mile k=1: {str(e)}")
            return

        if df_rotas_last_mile is None or df_rotas_last_mile.empty:
            self.logger.warning("⚠️ Roteirização k=1 não gerou rotas.")
            return

        self.last_mile_service.salvar_rotas_last_mile_em_db(
            df_rotas_last_mile, self.tenant_id, self.envio_data,
            self.simulation_id, 1, self.simulation_db
        )

        custo_last_mile = self.cost_last_mile_service.calcular_custo(df_rotas_last_mile)

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
            self.logger.warning(f"⚠️ Falha ao calcular custo cluster (k=1): {e}")

        custo_total = custo_last_mile + custo_cluster
        qtd = int(df_entregas["cte_numero"].nunique())

        self.result_service.salvar_resultado({
            "simulation_id": self.simulation_id,
            "tenant_id": self.tenant_id,
            "envio_data": self.envio_data,
            "k_clusters": 1,
            "quantidade_entregas": qtd,
            "custo_transferencia": 0.0,
            "custo_last_mile": custo_last_mile,
            "custo_cluster": custo_cluster,
            "custo_total": custo_total,
            "is_ponto_otimo": False
        }, modo_forcar=self.modo_forcar)

        try:
            plotar_mapa_last_mile(
                simulation_db=self.simulation_db,
                clusterization_db=self.clusterization_db,
                tenant_id=self.tenant_id,
                envio_data=self.envio_data,
                k_clusters=1,
                output_dir=self.output_dir,
                modo_forcar=self.modo_forcar,
                logger=self.logger
            )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro mapa last-mile k=1: {e}")
