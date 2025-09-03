#domain/transfer_planner_adapter.py

from datetime import date
from typing import List, Dict
import uuid

from simulation.domain.entities import TransferenciaDetalhe, TransferenciaResumo, Hub, ClusterTransferencia
from simulation.domain.transfer_grouping_service import agrupar_clusters_em_rotas


class TransferPlannerAdapter:
    def __init__(
        self,
        tenant_id: str,
        simulation_id: str,
        k_clusters: int,
        is_ponto_otimo: bool,
        logger,
        tempo_max_transferencia: int,
        peso_max_kg: float
    ):
        self.tenant_id = tenant_id
        self.simulation_id = simulation_id
        self.k_clusters = k_clusters
        self.is_ponto_otimo = is_ponto_otimo
        self.logger = logger
        self.tempo_max_transferencia = tempo_max_transferencia
        self.peso_max_kg = peso_max_kg

    def planejar_rotas(self, df_entregas, hub: Hub, envio_data: date) -> Dict:
        self.logger.info("📦 Consolidando entregas por cluster...")

        clusters_consolidados = []
        for cluster_id, grupo in df_entregas.groupby("cluster"):
            clusters_consolidados.append(
                ClusterTransferencia(
                    cluster_id=cluster_id,
                    centro_lat=grupo["centro_lat"].iloc[0],
                    centro_lon=grupo["centro_lon"].iloc[0],
                    peso_total=grupo["cte_peso"].sum(),
                    valor_nf_total=grupo["cte_valor_nf"].sum(),
                    valor_frete_total=grupo["cte_valor_frete"].sum()
                )
            )

        self.logger.info(f"🧮 Total de clusters consolidados: {len(clusters_consolidados)}")

        hub_coord = (hub.latitude, hub.longitude)

        rotas_grupo = agrupar_clusters_em_rotas(
            clusters=clusters_consolidados,
            hub_coord=hub_coord,
            tempo_max=self.tempo_max_transferencia,
            peso_max=self.peso_max_kg,
            tenant_id=self.tenant_id,
            db_conn=hub.db_conn,
            logger=self.logger
        )

        rotas = {}
        for idx, lista_clusters in enumerate(rotas_grupo):
            rota_id = f"R{self.k_clusters}_{idx}_{uuid.uuid4().hex[:4]}"
            clusters_info = []

            for cluster in lista_clusters:
                entregas_do_cluster = df_entregas[df_entregas["cluster"] == cluster.cluster_id]
                for _, row in entregas_do_cluster.iterrows():
                    clusters_info.append({
                        "cte_numero": str(row["cte_numero"]),
                        "cluster": row["cluster"],
                        "lat": row["centro_lat"],
                        "lon": row["centro_lon"],
                        "cte_peso": row["cte_peso"],
                        "cte_volumes": row["cte_volumes"],
                        "cte_valor_nf": row["cte_valor_nf"],
                        "cte_valor_frete": row["cte_valor_frete"],
                        "envio_data": row["envio_data"]
                    })

            rotas[rota_id] = {"clusters": clusters_info}

        self.logger.info(f"✅ Total de rotas geradas: {len(rotas)}")
        return rotas

    def gerar_detalhes(self, rotas: dict, hub: Hub, envio_data: date) -> List[TransferenciaDetalhe]:
        detalhes = []

        for rota_id, rota in rotas.items():
            for entrega in rota["clusters"]:
                detalhe = TransferenciaDetalhe(
                    tenant_id=self.tenant_id,
                    envio_data=envio_data,
                    simulation_id=self.simulation_id,
                    k_clusters=self.k_clusters,
                    is_ponto_otimo=self.is_ponto_otimo,
                    cte_numero=entrega["cte_numero"],
                    cluster=entrega["cluster"],
                    rota_id=rota_id,
                    tipo_veiculo=None,
                    cte_peso=entrega["cte_peso"],
                    cte_volumes=entrega["cte_volumes"],
                    cte_valor_nf=entrega["cte_valor_nf"],
                    cte_valor_frete=entrega["cte_valor_frete"],
                    centro_lat=entrega["lat"],
                    centro_lon=entrega["lon"]
                )
                detalhes.append(detalhe)

        self.logger.info(f"📝 Total de detalhes de transferências gerados: {len(detalhes)}")
        return detalhes

    def gerar_resumo(
        self,
        detalhes: List[TransferenciaDetalhe],
        hub: Hub,
        envio_data: date
    ) -> List[TransferenciaResumo]:
        resumo_por_rota = {}

        for d in detalhes:
            key = d.rota_id
            if key not in resumo_por_rota:
                resumo_por_rota[key] = {
                    "qde_entregas": 0,
                    "peso_total_kg": 0.0,
                    "valor_total_nf": 0.0,
                    "valor_total_frete": 0.0,
                    "clusters": set(),
                    "coords": []
                }

            resumo = resumo_por_rota[key]
            resumo["qde_entregas"] += 1
            resumo["peso_total_kg"] += d.cte_peso
            resumo["valor_total_nf"] += d.cte_valor_nf
            resumo["valor_total_frete"] += d.cte_valor_frete
            resumo["clusters"].add(d.cluster)
            resumo["coords"].append({"lat": d.centro_lat, "lon": d.centro_lon})

        resumos = []
        for rota_id, dados in resumo_por_rota.items():
            coords = (
                [{"lat": hub.latitude, "lon": hub.longitude}]
                + dados["coords"]
                + [{"lat": hub.latitude, "lon": hub.longitude}]
            )
            resumos.append(
                TransferenciaResumo(
                    tenant_id=self.tenant_id,
                    envio_data=envio_data,
                    simulation_id=self.simulation_id,
                    k_clusters=self.k_clusters,
                    is_ponto_otimo=self.is_ponto_otimo,
                    rota_id=rota_id,
                    hub_nome=hub.nome,
                    hub_latitude=hub.latitude,
                    hub_longitude=hub.longitude,
                    tipo_veiculo="Carreta",
                    peso_total_kg=dados["peso_total_kg"],
                    valor_total_nf=dados["valor_total_nf"],
                    valor_total_frete=dados["valor_total_frete"],
                    qde_entregas=dados["qde_entregas"],
                    qde_clusters_rota=len(dados["clusters"]),
                    clusters_utilizados=coords
                )
            )

        return resumos
