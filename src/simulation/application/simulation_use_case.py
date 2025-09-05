# simulation/application/simulation_use_case.py

import uuid
import pandas as pd

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

class SimulationUseCase:

    def __init__(self, tenant_id, envio_data, parametros,
                 clusterization_db, simulation_db, logger,
                 modo_forcar=False, simulation_id=None,
                 # 🔑 corrigido: antes "output/maps"
                 output_dir="exports/simulation/maps",
                 fundir_clusters_pequenos=True,
                 permitir_rotas_excedentes=False):


        self.tenant_id = tenant_id
        self.envio_data = envio_data
        self.parametros = parametros
        self.clusterization_db = clusterization_db
        self.simulation_db = simulation_db
        self.logger = logger
        self.modo_forcar = modo_forcar
        self.simulation_id = simulation_id or str(uuid.uuid4())
        self.output_dir = output_dir
        self.fundir_clusters_pequenos = fundir_clusters_pequenos
        self.permitir_rotas_excedentes = permitir_rotas_excedentes

        self.simulation_service = SimulationService(tenant_id, envio_data, simulation_db, logger)
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


    def executar_simulacao_completa(self):
        if not self.modo_forcar and self.simulation_service.simulacao_ja_existente():
            self.logger.warning(f"🚫 Simulação já existente para {self.envio_data}. Use --modo-forcar para sobrescrever.")
            return None

        if self.modo_forcar:
            self.logger.info("♻️ Modo forçar ativado. Limpando simulações anteriores...")
            self.simulation_service.limpar_simulacoes_anteriores()

        self.logger.info("🔁 Iniciando execução completa da simulação.")
        self.logger.info(f"🆔 Simulation ID: {self.simulation_id}")
        self.logger.info(f"📥 Carregando entregas para envio_data = {self.envio_data}, tenant_id = {self.tenant_id}")

        # 📥 Carregar entregas do clusterization_db
        df_entregas_original = self.cluster_service.carregar_entregas(self.tenant_id, self.envio_data)

        # ✅ Verificar se as coordenadas já vieram preenchidas corretamente
        if "latitude" in df_entregas_original.columns and "longitude" in df_entregas_original.columns:
            n_coordenadas = df_entregas_original[["latitude", "longitude"]].dropna().shape[0]
            self.logger.info(f"📍 {n_coordenadas} entregas com coordenadas válidas carregadas do clusterization_db.")
        else:
            self.logger.warning("⚠️ Colunas latitude/longitude não encontradas ao carregar entregas.")



        if df_entregas_original.empty:
            self.logger.warning(f"⚠️ Nenhuma entrega encontrada para {self.envio_data}. Simulação será ignorada.")
            return None


        # 🔍 Carregar hubs e aplicar filtro por raio


        from simulation.infrastructure.simulation_database_reader import carregar_hubs

        hubs = carregar_hubs(self.simulation_db, self.tenant_id)

        if not self.parametros.get("desativar_cluster_hub", False):
            self.logger.info(f"🧲 Atribuindo entregas próximas ao hub central como cluster especial (raio: {self.parametros.get('raio_hub_km', 80.0)} km)")
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

        # Simulação inicial com k=1 partindo do hub central
        self._executar_simulacao_k1(df_entregas, df_hub)

        k_inicial = self.simulation_service.obter_k_inicial(df_entregas, self.parametros['k_min'], self.parametros['k_max'])
        lista_k = self.simulation_service.gerar_lista_k(k_inicial, self.parametros['k_min'], self.parametros['k_max'])

        custos_totais = []
        melhor_k = None
        menor_custo = float("inf")
        melhor_resultado = None

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
                    "custo_transferencia": resultado_k["custo_transferencia"],
                    "custo_last_mile": resultado_k["custo_last_mile"],
                    "custo_cluster": resultado_k["custo_cluster"]
                }


            if self.simulation_service.verificar_ponto_inflexao_com_tendencia(custos_totais):
                self.logger.info("📉 Heurística de inflexão identificou ponto ótimo.")
                break

        if melhor_k is not None and melhor_resultado is not None:
            self.logger.info(f"📝 Atualizando flag is_ponto_otimo=True para k={melhor_k}")
            cursor = self.simulation_db.cursor()
            for tabela in ["entregas_clusterizadas", "resumo_transferencias", "detalhes_transferencias"]:
                cursor.execute(f"""
                    UPDATE {tabela}
                    SET is_ponto_otimo = TRUE
                    WHERE tenant_id = %s AND envio_data = %s AND simulation_id = %s AND k_clusters = %s
                """, (self.tenant_id, self.envio_data, self.simulation_id, melhor_k))
            self.simulation_db.commit()
            cursor.close()

            self.result_service.salvar_resultado({
                **melhor_resultado,
                "is_ponto_otimo": True
            }, modo_forcar=self.modo_forcar)


            return {
                "k_clusters": melhor_k,
                "custo_total": menor_custo
            }

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
            self.logger.info("🔗 Clusters pequenos foram fundidos conforme configuração.")
        else:
            self.logger.info("⛔ Fusão de clusters pequenos desativada para esta simulação.")

        df_clusterizado = self.cluster_service.ajustar_centros_dos_clusters(df_clusterizado)
        # ➕ Adicionar entregas do cluster 9999 ao resultado final
        if df_hub is not None and not df_hub.empty:

            df_hub["simulation_id"] = self.simulation_id
            df_hub["k_clusters"] = k
            df_hub["is_ponto_otimo"] = False

            df_clusterizado = pd.concat([df_clusterizado, df_hub], ignore_index=True)
            self.logger.info(f"📦 {len(df_hub)} entregas atribuídas ao cluster 9999 (hub central).")


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

        self.logger.info("🚛 Roteirizando transferências e gerando detalhes...")
        lista_resumo_transferencias, detalhes_transferencia_gerados = self.transfer_service.rotear_transferencias(
            envio_data=self.envio_data,
            simulation_id=self.simulation_id,
            k_clusters=k,
            is_ponto_otimo=is_ponto_otimo
        )
        if not detalhes_transferencia_gerados:
            self.logger.warning(f"⚠️ Nenhuma rota de transferência gerada para k={k}")
            return None

        salvar_detalhes_transferencias(detalhes_transferencia_gerados, self.simulation_db)
        persistir_resumo_transferencias(
            lista_resumo_transferencias, self.simulation_db,
            logger=self.logger, tenant_id=self.tenant_id
        )

        try:
            df_rotas_last_mile = self.last_mile_service.rotear_last_mile(df_clusterizado, k_clusters=k)
        except Exception as e:
            self.logger.error(f"❌ Erro na roteirização last-mile: {str(e)}")
            return None

        if df_rotas_last_mile is None or df_rotas_last_mile.empty:
            self.logger.warning("⚠️ df_rotas_last_mile está vazio ou não foi gerado.")
            return None

        self.last_mile_service.salvar_rotas_last_mile_em_db(
            df_rotas_last_mile, self.tenant_id, self.envio_data,
            self.simulation_id, k, self.simulation_db
        )

        # 💰 Cálculo de custos
        custo_transfer = self.cost_transfer_service.calcular_custo(lista_resumo_transferencias)
        custo_last_mile = self.cost_last_mile_service.calcular_custo(df_rotas_last_mile)

        # 📦 Novo: cálculo alternativo de custo de cluster baseado em clusters individuais
        try:

            cluster_cost_cfg = carregar_cluster_costs(self.simulation_db, self.tenant_id)
            df_resumo_clusters = (
                df_clusterizado
                .groupby("cluster")
                .agg(qde_ctes=("cte_numero", "nunique"))
                .reset_index()
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
            self.logger.warning(f"⚠️ Falha ao calcular custo de cluster (por cluster): {e}")


        # 🔢 Custo total consolidado
        custo_total = custo_transfer + custo_last_mile + custo_cluster
        self.logger.info(f"💰 Custo total para k={k}: R${custo_total:,.2f}")


        self.logger.info(f"💰 Custo total para k={k}: R${custo_total:,.2f}")

        self.result_service.salvar_resultado({
            "simulation_id": self.simulation_id,
            "tenant_id": self.tenant_id,
            "envio_data": self.envio_data,
            "k_clusters": k,
            "custo_total": custo_total,
            "quantidade_entregas": len(df_entregas),
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
                logger=self.logger
            )
        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao gerar mapa de clusterização para k={k}: {e}")

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
            self.logger.warning(f"⚠️ Erro ao gerar mapa de transferências para k={k}: {e}")

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
            self.logger.warning(f"⚠️ Erro ao gerar mapa de last-mile para k={k}: {e}")

        return {
            "k_clusters": k,
            "custo_total": custo_total,
            "custo_transferencia": custo_transfer,
            "custo_last_mile": custo_last_mile,
            "custo_cluster": custo_cluster
        }


    def _executar_simulacao_k1(self, df_entregas, df_hub=None):
        self.logger.info("🚀 Executando simulação especial de last-mile com k=1 (partindo do hub central)")

        # 🔗 Incluir entregas do hub central (cluster 9999)
        if df_hub is not None and not df_hub.empty:
            self.logger.info(f"➕ Incluindo {len(df_hub)} entregas do cluster 9999 (hub central) na simulação k=1")
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

        # 🔍 Validação de coordenadas antes da roteirização
        cte_esperados = set(df_entregas["cte_numero"].astype(str).unique())

        from simulation.infrastructure.simulation_database_reader import buscar_latlon_ctes
        df_coords_k1 = buscar_latlon_ctes(
            clusterization_db=self.clusterization_db,
            simulation_db=self.simulation_db,
            tenant_id=self.tenant_id,
            envio_data=self.envio_data,
            lista_ctes=list(cte_esperados),
            k_clusters=1,
            logger=self.logger
        )

        cte_com_coord = set(df_coords_k1["cte_numero"].astype(str).unique())
        cte_sem_coord = cte_esperados - cte_com_coord

        if len(cte_sem_coord) > 0:
            self.logger.warning(f"⚠️ {len(cte_sem_coord)} entregas ignoradas por falta de coordenadas na simulação k=1.")
            self.logger.debug(f"CTEs sem coordenadas: {sorted(list(cte_sem_coord))}")
        else:
            self.logger.info("✅ Todas as entregas de k=1 possuem coordenadas válidas.")

        # 🚛 Roteirização
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
            self.logger.warning("⚠️ Roteirização k=1 não gerou rotas válidas.")
            return

        self.last_mile_service.salvar_rotas_last_mile_em_db(
            df_rotas_last_mile, self.tenant_id, self.envio_data,
            self.simulation_id, 1, self.simulation_db
        )

                # 💰 Cálculo de custos
        custo_last_mile = self.cost_last_mile_service.calcular_custo(df_rotas_last_mile)

        try:
            cluster_cost_cfg = carregar_cluster_costs(self.simulation_db, self.tenant_id)

            df_resumo_clusters = (
                df_entregas
                .groupby("cluster")
                .agg(qde_ctes=("cte_numero", "nunique"))
                .reset_index()
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
            self.logger.warning(f"⚠️ Falha ao calcular custo de cluster (k=1): {e}")

        custo_total = custo_last_mile + custo_cluster

        self.logger.info(f"💰 Custo total para k=1: R${custo_total:,.2f}")

        self.result_service.salvar_resultado({
            "simulation_id": self.simulation_id,
            "tenant_id": self.tenant_id,
            "envio_data": self.envio_data,
            "k_clusters": 1,
            "quantidade_entregas": len(df_entregas),
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
            self.logger.info("🗺️ Mapa last-mile gerado para k=1")
        except Exception as e:
            self.logger.warning(f"⚠️ Erro ao gerar mapa de last-mile para k=1: {e}")

