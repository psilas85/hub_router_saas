#transfer_routing/domain/transfer_planner.py

from datetime import date
from functools import partial

from transfer_routing.infrastructure.database_reader import (
    buscar_hub_central,
    carregar_entregas_completas
)
from transfer_routing.infrastructure.database_writer import salvar_transferencias
from transfer_routing.infrastructure.geolocation import get_route
from transfer_routing.domain.route_planning import gerar_rotas_transferencias
from transfer_routing.infrastructure.vehicle_selector import obter_tipo_veiculo_por_peso



class TransferPlanner:
    def __init__(self, tenant_id: str, tempo_maximo: float, tempo_parada_leve: float,
                 peso_leve_max: float, tempo_parada_pesada: float, tempo_por_volume: float):
        self.tenant_id = tenant_id
        self.tempo_maximo = tempo_maximo
        self.tempo_parada_leve = tempo_parada_leve
        self.peso_leve_max = peso_leve_max
        self.tempo_parada_pesada = tempo_parada_pesada
        self.tempo_por_volume = tempo_por_volume

    def executar(self, envio_data: date, conn_cluster, conn_routing, logger):
        logger.info(f"Iniciando roteirização de transferências para {envio_data}.")

        logger.info("Buscando hub central.")
        hub = buscar_hub_central(self.tenant_id, conn_routing)
        if not hub:
            raise Exception(f"Nenhum hub cadastrado encontrado para o tenant {self.tenant_id}.")
        logger.info(f"Hub encontrado: {hub['nome']} -> ({hub['latitude']}, {hub['longitude']})")

        origem = (hub["latitude"], hub["longitude"])

        logger.info("Carregando entregas clusterizadas.")
        df_entregas = carregar_entregas_completas(self.tenant_id, envio_data, conn_cluster)
        if df_entregas.empty:
            logger.warning(f"Nenhuma entrega encontrada para {envio_data} e tenant {self.tenant_id}.")
            return
        logger.info(f"Total de entregas carregadas: {len(df_entregas)}")

        # Remover entregas do cluster HUB_CENTRAL (9999) para não processar roteirização
        df_entregas_sem_hub = df_entregas[df_entregas["cluster"] != 9999].copy()
        df_hub = df_entregas[df_entregas["cluster"] == 9999].copy()

        logger.info(f"Total de entregas excluídas para roteirização (HUB_CENTRAL): {len(df_hub)}")
        logger.info(f"Total de entregas para roteirização: {len(df_entregas_sem_hub)}")

        logger.info("Verificando cache e calculando distâncias e tempos.")

        obter_rota = partial(get_route, tenant_id=self.tenant_id, conn=conn_routing, logger=logger)

        logger.info("Iniciando geração de rotas.")
        rotas_resumo, detalhes_transferencias = gerar_rotas_transferencias(
            df_entregas=df_entregas_sem_hub,
            origem=origem,
            tempo_maximo=self.tempo_maximo,
            tempo_parada_leve=self.tempo_parada_leve,
            tempo_parada_pesada=self.tempo_parada_pesada,
            tempo_por_volume=self.tempo_por_volume,
            peso_leve_max=self.peso_leve_max,
            obter_rota=obter_rota,
            conn=conn_routing,
            tenant_id=self.tenant_id,
            logger=logger,
            hub_info=hub
        )

        # Adicionar rota do HUB_CENTRAL manualmente no resumo e detalhes
        if not df_hub.empty:
            logger.info(f"Adicionando rota do HUB Central ({len(df_hub)} entregas)")
            # Obter tipo de veículo para o HUB
            peso_total_hub = round(df_hub["cte_peso"].sum(), 2)
            veiculo_info = obter_tipo_veiculo_por_peso(peso_total_hub, self.tenant_id, conn_routing)
            tipo_veiculo_hub = veiculo_info["tipo_veiculo"] if veiculo_info else "Desconhecido"

            rota_hub = {
                "rota_id": "HUB",
                "quantidade_entregas": len(df_hub),
                "cte_peso": peso_total_hub,
                "cte_valor_nf": round(df_hub["cte_valor_nf"].sum(), 2),
                "cte_valor_frete": round(df_hub["cte_valor_frete"].sum(), 2),
                "clusters_qde": 1,
                "rota_coord": [],
                "hub_central_nome": hub["nome"],
                "hub_central_latitude": hub["latitude"],
                "hub_central_longitude": hub["longitude"],
                "distancia_ida_km": 0.0,
                "distancia_total_km": 0.0,
                "tempo_ida_min": 0.0,
                "tempo_total_min": 0.0,
                "tempo_transito_ida": 0.0,
                "tempo_transito_total": 0.0,
                "tempo_paradas": 0.0,
                "tempo_descarga": 0.0,
                "tipo_veiculo": tipo_veiculo_hub,
                "volumes_total": int(df_hub["cte_volumes"].sum()),
                "peso_total_kg": peso_total_hub
            }


            detalhes_hub = [
                {
                    "cte_numero": row["cte_numero"],
                    "cluster": row["cluster"],
                    "rota_id": "HUB",
                    "hub_central_nome": hub["nome"],
                    "cte_peso": row["cte_peso"],
                    "cte_valor_nf": row["cte_valor_nf"],
                    "cte_valor_frete": row["cte_valor_frete"],
                    "centro_lat": row["centro_lat"],
                    "centro_lon": row["centro_lon"],
                    "cte_volumes": row["cte_volumes"]
                }
                for _, row in df_hub.iterrows()
            ]

            rotas_resumo.append(rota_hub)
            detalhes_transferencias.extend(detalhes_hub)

        logger.info("Rotas geradas com sucesso.")
        logger.info("Geração de rotas concluída.")

        if not rotas_resumo:
            logger.warning(f"Nenhuma rota gerada para {envio_data}.")
            return

        logger.info(f"Total de rotas criadas: {len(rotas_resumo)}")
        logger.info("Resumo das rotas:")
        for rota in rotas_resumo:
            logger.info(
                f"Rota {rota['rota_id']} | "
                f"Entregas: {rota['quantidade_entregas']} | "
                f"Paradas: {rota['clusters_qde']} | "
                f"Peso: {rota['cte_peso']:.2f} kg | "
                f"Distância ida: {rota['distancia_ida_km']:.2f} km | "
                f"Distância total: {rota['distancia_total_km']:.2f} km | "
                f"Tempo ida: {rota['tempo_ida_min']:.2f} min | "
                f"Tempo total: {rota['tempo_total_min']:.2f} min"
            )

        logger.info("Salvando dados no banco.")
        salvar_transferencias(
            rotas_resumo, detalhes_transferencias, conn_routing,
            self.tenant_id, envio_data, logger
        )
        logger.info("Dados salvos no banco.")

        logger.info(f"Roteirização de transferências concluída para {envio_data}.")
