#simulation/domain/transfer_routing_service.py

import pandas as pd
import uuid
from datetime import date
from functools import partial
import json

from simulation.utils.format_utils import formatar
from simulation.utils.route_helpers import gerar_rotas_savings_transfer, expandir_pontos_por_capacidade_veiculo
from simulation.infrastructure.cache_routes import obter_rota_real, obter_rota_real_detalhada
from simulation.infrastructure.simulation_database_reader import (
    carregar_hub_por_id,
    definir_tipo_veiculo_transferencia,
)
from simulation.domain.entities import TransferenciaResumo
from simulation.infrastructure.simulation_database_writer import (
    salvar_detalhes_transferencias,
    salvar_rotas_transferencias
)

from simulation.domain.entities import SimulationParams


class TransferRoutingService:
    def __init__(self, clusterization_db, simulation_db, logger, tenant_id, params: SimulationParams, hub_id):
        self.params = params
        self.clusterization_db = clusterization_db
        self.simulation_db = simulation_db
        self.logger = logger
        self.tenant_id = tenant_id
        self.hub_id = hub_id

    @staticmethod
    def _retorno_vazio():
        return [], [], []

    def _normalizar_df_clusterizado(self, df_clusterizado: pd.DataFrame, envio_data, k_clusters):
        df = df_clusterizado.copy()
        if df.empty:
            return df

        colunas_obrigatorias = [
            "cluster", "centro_lat", "centro_lon", "cte_numero",
            "cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete",
        ]
        for coluna in colunas_obrigatorias:
            if coluna not in df.columns:
                raise ValueError(f"❌ Coluna obrigatória ausente para transferências: {coluna}")

        df["cte_peso"] = pd.to_numeric(df["cte_peso"], errors="coerce").fillna(0.0).astype(float)
        df["cte_volumes"] = pd.to_numeric(df["cte_volumes"], errors="coerce").fillna(0).astype(int)
        df["cte_valor_nf"] = pd.to_numeric(df["cte_valor_nf"], errors="coerce").fillna(0.0).astype(float)
        df["cte_valor_frete"] = pd.to_numeric(df["cte_valor_frete"], errors="coerce").fillna(0.0).astype(float)
        df["cte_numero"] = df["cte_numero"].astype(str)

        if "cte_tempo_atendimento_min" in df.columns:
            df["cte_tempo_atendimento_min"] = pd.to_numeric(
                df["cte_tempo_atendimento_min"],
                errors="coerce",
            )

        df["tenant_id"] = self.tenant_id
        df["envio_data"] = envio_data
        df["k_clusters"] = k_clusters
        df = df.drop_duplicates(subset=["cte_numero", "tenant_id", "envio_data", "k_clusters"])
        df["cluster"] = df["cluster"].astype(str)
        return df

    def _obter_velocidade_media_kmh(self):
        return float(self.params.velocidade_kmh)

    def _calcular_tempo_servico_ponto(self, ponto):
        tempo_atendimento = ponto.get("tempo_atendimento_min")
        if tempo_atendimento is not None and pd.notna(tempo_atendimento):
            return max(float(tempo_atendimento), 0.0)

        peso = float(ponto.get("peso") or 0.0)
        volumes = float(ponto.get("volumes") or 0.0)
        tempo_parada = (
            self.params.tempo_parada_pesada
            if peso > self.params.limite_peso_parada
            else self.params.tempo_parada_leve
        )

        tempo_por_volume = self.params.tempo_por_volume
        return tempo_parada + (volumes * tempo_por_volume)

    def rotear_transferencias(self, envio_data, simulation_id, k_clusters, is_ponto_otimo, persistir=True):
        self.logger.info("📦 Iniciando roteirização de transferências com Savings Algorithm...")

        query = """
            SELECT cluster, centro_lat, centro_lon, cte_numero,
                cte_peso, cte_volumes, cte_valor_nf, cte_valor_frete
            FROM entregas_clusterizadas
            WHERE tenant_id = %s
            AND envio_data = %s
            AND k_clusters = %s
        """

        try:
            cursor = self.simulation_db.cursor()
            cursor.execute(query, (self.tenant_id, envio_data, k_clusters))
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            cursor.close()
        except Exception as e:
            self.logger.error(f"❌ Erro ao carregar dados de entregas_clusterizadas: {e}")
            return self._retorno_vazio()

        return self.rotear_transferencias_para_dataframe(
            df_clusterizado=df,
            envio_data=envio_data,
            simulation_id=simulation_id,
            k_clusters=k_clusters,
            is_ponto_otimo=is_ponto_otimo,
            persistir=persistir,
        )

    def rotear_transferencias_para_dataframe(
        self,
        df_clusterizado: pd.DataFrame,
        envio_data,
        simulation_id,
        k_clusters,
        is_ponto_otimo,
        persistir=False,
    ):
        self.logger.info("📦 Iniciando roteirização de transferências com Savings Algorithm...")

        if df_clusterizado is None or df_clusterizado.empty:
            self.logger.warning("⚠️ Nenhum dado encontrado para clusterização de transferências.")
            return self._retorno_vazio()

        df = self._normalizar_df_clusterizado(df_clusterizado, envio_data, k_clusters)


        # ❌ Ignorar entregas do cluster do hub central e seus subgrupos (9999, 9999_1, ...)
        df = df[~df["cluster"].astype(str).str.startswith("9999")]

        if df.empty:
            self.logger.info("ℹ️ Todas as entregas estão no cluster do hub central. Não há roteirização de transferência a executar.")
            return self._retorno_vazio()

        self.logger.info(f"🔍 Total de entregas únicas (CTEs): {df['cte_numero'].nunique()}")

        hub = carregar_hub_por_id(self.simulation_db, self.tenant_id, self.hub_id)
        if not hub:
            raise ValueError(f"❌ Hub central com hub_id={self.hub_id} não encontrado para este tenant.")
        origem = (hub["latitude"], hub["longitude"])

        agregacoes = {
            "cte_peso": "sum",
            "cte_volumes": "sum",
            "cte_valor_nf": "sum",
            "cte_valor_frete": "sum",
            "cte_numero": lambda x: list(x)
        }
        if "cte_tempo_atendimento_min" in df.columns:
            agregacoes["cte_tempo_atendimento_min"] = lambda serie: serie.sum(min_count=1)

        agrupado = df.groupby(["cluster", "centro_lat", "centro_lon"]).agg(agregacoes).reset_index()

        pontos = [
            {
                "cluster_id": row["cluster"],
                "cte_numeros": row["cte_numero"],
                "lat": row["centro_lat"],
                "lon": row["centro_lon"],
                "peso": row["cte_peso"],
                "volumes": row["cte_volumes"],
                "valor_nf": row["cte_valor_nf"],
                "valor_frete": row["cte_valor_frete"],
                "tempo_atendimento_min": row.get("cte_tempo_atendimento_min"),
            }
            for _, row in agrupado.iterrows()
        ]
        velocidade_media_kmh = self._obter_velocidade_media_kmh()
        pontos = expandir_pontos_por_capacidade_veiculo(
            pontos,
            self.simulation_db,
            self.tenant_id,
            self.logger,
        )

        obter_rota = partial(
            obter_rota_real,
            tenant_id=self.tenant_id,
            db_conn=self.simulation_db,
            logger=self.logger,
            velocidade_media_kmh=velocidade_media_kmh,
        )

        rotas = gerar_rotas_savings_transfer(
            pontos=pontos,
            origem=origem,
            tempo_maximo=self.params.tempo_max_transferencia,
            params=self.params,
            obter_rota_func=obter_rota,
            logger=self.logger
        )

        if not rotas:
            self.logger.warning("⚠️ Nenhuma rota gerada com Savings Algorithm.")
            return self._retorno_vazio()

        rotas_resumo = []
        detalhes_transferencias = []
        rotas_transferencia = []
        ctes_ignorados = []

        for idx, rota in enumerate(rotas):
            rota_id = f"R{k_clusters}_{idx+1}_{uuid.uuid4().hex[:4]}"
            self.logger.info(f"\n🚚 Iniciando processamento da {rota_id}...")

            if not rota or sum(len(p.get("cte_numeros", [])) for p in rota) == 0:
                self.logger.warning(f"⚠️ Rota {rota_id} vazia ou sem CT-es. Ignorando.")
                continue

            if all(p["lat"] == origem[0] and p["lon"] == origem[1] for p in rota):
                self.logger.warning(f"⚠️ Rota {rota_id} com todos os pontos iguais ao hub. Ignorando.")
                continue

            if rota[0]["lat"] != origem[0] or rota[0]["lon"] != origem[1]:
                self.logger.warning(f"⚠️ Rota {rota_id} não inicia pelo hub. Inserindo origem.")
                rota = [ {
                    "lat": origem[0],
                    "lon": origem[1],
                    "cte_numeros": [],
                    "peso": 0,
                    "volumes": 0,
                    "valor_nf": 0,
                    "valor_frete": 0
                } ] + rota

            if len(rota) <= 1:
                self.logger.warning(f"⚠️ Rota {rota_id} sem destinos além do hub. Ignorando.")
                cte_debug = [cte for p in rota for cte in p.get("cte_numeros", [])]
                self.logger.warning(f"🧾 CT-es ignorados nesta rota sem destinos: {cte_debug}")
                ctes_ignorados.extend(cte_debug)
                continue

            peso = sum(p["peso"] for p in rota)
            volumes = sum(p["volumes"] for p in rota)
            valor_nf = sum(p["valor_nf"] for p in rota)
            # ✅ Agora com tenant_id e normalização no próprio método
            tipo_veiculo = definir_tipo_veiculo_transferencia(peso, self.simulation_db, self.tenant_id)


            dist_real = 0.0
            tempo_real = 0.0
            sequencia_coord = []
            anterior = origem
            fontes_metricas = set()

            for ponto in rota:
                atual = (ponto["lat"], ponto["lon"])
                dist, tempo, rota_completa, fonte_rota = obter_rota_real_detalhada(
                    anterior,
                    atual,
                    self.tenant_id,
                    self.simulation_db,
                    self.logger,
                    velocidade_media_kmh,
                )
                dist_real += dist or 0.0
                tempo_real += tempo or 0.0
                fontes_metricas.add(fonte_rota)
                anterior = atual
                if rota_completa:
                    sequencia_coord.extend([(p["lat"], p["lon"]) if isinstance(p, dict) else (p[0], p[1]) for p in rota_completa])
                else:
                    sequencia_coord.append(atual)

            dist_back, tempo_back, rota_back, fonte_rota_back = obter_rota_real_detalhada(
                anterior,
                origem,
                self.tenant_id,
                self.simulation_db,
                self.logger,
                velocidade_media_kmh,
            )
            dist_real += dist_back or 0.0
            tempo_real += tempo_back or 0.0
            fontes_metricas.add(fonte_rota_back)

            tempo_paradas = 0.0

            for ponto in rota:
                if ponto.get("cte_numeros"):
                    tempo_paradas += self._calcular_tempo_servico_ponto(ponto)

            tempo_total_completo = tempo_real + tempo_paradas
            tempo_parcial_completo = tempo_total_completo - tempo_back

            if rota_back:
                sequencia_coord.extend([(p["lat"], p["lon"]) if isinstance(p, dict) else (p[0], p[1]) for p in rota_back])
            else:
                sequencia_coord.append(origem)

            # ✅ Limpeza dos NaNs antes de gerar rota_completa_json
            # 🧹 Remove coordenadas com NaN antes de converter para JSON
            sequencia_coord = [
                (lat, lon) for lat, lon in sequencia_coord if pd.notna(lat) and pd.notna(lon)
            ]

            # 🆕 Corrigido: NaN gerava erro de JSON inválido
            rota_completa_json = json.dumps([
                {"lat": float(lat), "lon": float(lon)} for lat, lon in sequencia_coord
            ])


            cte_ids_rota = [cte for p in rota for cte in p.get("cte_numeros", [])]
            clusters_rota = {p["cluster_id"] for p in rota if "cluster_id" in p}

            resumo = TransferenciaResumo(
                rota_id=rota_id,
                tenant_id=self.tenant_id,
                envio_data=envio_data,
                simulation_id=simulation_id,
                k_clusters=k_clusters,
                is_ponto_otimo=is_ponto_otimo,
                tipo_veiculo=tipo_veiculo,
                distancia_total_km=dist_real,
                distancia_parcial_km=dist_real - dist_back,
                tempo_total_min=tempo_total_completo,
                tempo_parcial_min=tempo_parcial_completo,
                peso_total_kg=peso,
                volumes_total=volumes,
                valor_total_nf=valor_nf,
                aproveitamento_percentual=None,
                qde_entregas=len(cte_ids_rota),
                qde_clusters_rota=len(clusters_rota),
                coordenadas_seq=";".join([f"{lat:.6f},{lon:.6f}" for lat, lon in sequencia_coord]),
                fonte_metricas=(
                    "osrm"
                    if fontes_metricas.issubset({"osrm", "cache_osrm", "fallback_minimo"})
                    else "fallback"
                    if fontes_metricas.issubset({
                        "osrm",
                        "cache_osrm",
                        "fallback_minimo",
                        "google",
                        "manual_haversine",
                    })
                    else "nao_osrm"
                ),
            )

            rotas_resumo.append(resumo)

            ctes_rota = df[df["cte_numero"].isin(cte_ids_rota)].copy()
            ctes_rota["rota_id"] = rota_id
            ctes_rota["simulation_id"] = simulation_id
            ctes_rota["is_ponto_otimo"] = is_ponto_otimo
            ctes_rota["k_clusters"] = k_clusters
            ctes_rota["tipo_veiculo"] = tipo_veiculo
            ctes_rota["cluster"] = idx

            colunas = [
                "tenant_id", "envio_data", "simulation_id", "k_clusters", "is_ponto_otimo",
                "cte_numero", "cluster", "rota_id", "tipo_veiculo",
                "cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete",
                "centro_lat", "centro_lon"
            ]
            detalhes_transferencias.extend(ctes_rota[colunas].to_dict(orient="records"))

            rotas_transferencia.append({
                "rota_id": rota_id,
                "tenant_id": self.tenant_id,
                "envio_data": envio_data,
                "tipo_veiculo": tipo_veiculo,
                "cte_peso": float(peso),
                "coordenadas_seq": ";".join([f"{lat:.6f},{lon:.6f}" for lat, lon in sequencia_coord]),
                "distancia_ida_km": dist_real - dist_back,
                "distancia_total_km": dist_real,
                "tempo_total_min": tempo_total_completo,
                "tempo_ida_min": tempo_parcial_completo,
                "rota_completa_json": rota_completa_json,
                "k_clusters": k_clusters,
                "fonte_metricas": resumo.fonte_metricas,
            })


        if persistir:
            salvar_rotas_transferencias(rotas_transferencia, self.simulation_db)
        return rotas_resumo, detalhes_transferencias, rotas_transferencia
