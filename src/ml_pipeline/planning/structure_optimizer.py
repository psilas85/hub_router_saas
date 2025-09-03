#hub_router_1.0.1/src/ml_pipeline/planning/structure_optimizer.py

import itertools
import pandas as pd
from typing import Dict, Any, List, Tuple
from ml_pipeline.infrastructure.geolocation_adapter import GeolocationAdapter

class StructureOptimizer:
    def __init__(self, ml_pipeline, costs_transfer_client, costs_last_mile_client, logger=None, fast: bool = False):
        self.ml = ml_pipeline
        self.ct = costs_transfer_client
        self.lm = costs_last_mile_client
        self.geo = GeolocationAdapter()
        self.logger = logger
        self.fast = fast

    def shortlist_k(self, month_df, tenant_id, kmin=1, kmax=10, top=3):
        if self.fast:
            kmax, top = 2, 1   # 👈 só k=1 e 2, pega top1
        ks = list(range(kmin, kmax + 1))
        total = {
            "total_entregas": int(month_df["entregas"].sum()),
            "total_peso": float(month_df["peso"].sum()),
            "total_volumes": int(month_df["volumes"].sum()),
        }

        if self.logger:
            self.logger.info(f"🔎 Shortlist_k: avaliando ks {ks} com totais {total}")

        scores = []
        for k in ks:
            feats = {"k_clusters": k, **total}
            pred = self.ml.predict(
                features=feats,
                tenant_id=tenant_id,
                dataset_name="simulacoes",
                target_column="is_ponto_otimo",
            )
            scores.append((k, float(pred.get("probability") or 0.0)))

        scores.sort(key=lambda x: x[1], reverse=True)

        if self.logger:
            self.logger.info(f"✅ Shortlist_k: top {top} = {scores[:top]}")

        return [k for k, _ in scores[:top]]

    # ===== Atribuição cidade→hub por menor distância (aproximação p-mediana) =====

    def assign_cities_to_hubs(self, month_df: pd.DataFrame, hubs: List[Tuple[str, str]]) -> pd.DataFrame:
        # 1) agregue por cidade/uf (soma métricas do mês)
        cities = (month_df
                .groupby(["cidade", "uf"], as_index=False)[["entregas","peso","volumes","valor_nf"]]
                .sum())

        # 2) geocodifique cada cidade UMA vez (usa cache/banco do GeolocalizacaoService)
        rows = []
        for c, u, ent, peso, vol, val in cities[["cidade","uf","entregas","peso","volumes","valor_nf"]].itertuples(index=False, name=None):
            lat, lon = self.geo.get_latlon(c, u)
            if lat is None or lon is None:
                # opcional: continue (pula cidade) ou usar fallback por UF
                continue
            rows.append({
                "cidade": c, "uf": u,
                "entregas": ent, "peso": peso, "volumes": vol, "valor_nf": val,
                "lat": lat, "lon": lon
            })
        cities = pd.DataFrame(rows)

        # 3) preparar hubs (coordenadas 1x por hub)
        hub_rows = []
        for (hc, hu) in hubs:
            hlat, hlon = self.geo.get_latlon(hc, hu)
            hub_rows.append({"hub_cidade": hc, "hub_uf": hu, "hub_lat": hlat, "hub_lon": hlon})
        hubs_df = pd.DataFrame(hub_rows)

        # 4) distância haversine e melhor hub
        import numpy as np
        def hav(a, b):
            lat1, lon1, lat2, lon2 = np.radians(a[0]), np.radians(a[1]), np.radians(b[0]), np.radians(b[1])
            dlat, dlon = lat2 - lat1, lon2 - lon1
            R = 6371.0
            h = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
            return 2 * R * np.arcsin(np.sqrt(h))

        assign = []
        for row in cities.itertuples(index=False):
            best = None
            for h in hubs_df.itertuples(index=False):
                d = hav((row.lat, row.lon), (h.hub_lat, h.hub_lon))
                if (best is None) or (d < best[0]):
                    best = (d, h.hub_cidade, h.hub_uf)
            assign.append({
                "cidade": row.cidade, "uf": row.uf,
                "hub_cidade": best[1], "hub_uf": best[2],
                "entregas": row.entregas, "peso": row.peso,
                "volumes": row.volumes, "valor_nf": row.valor_nf
            })

        df = pd.DataFrame(assign)
        if self.logger:
            self.logger.info(f"📌 Assign cities→hubs: {len(df)} cidades atribuídas a {len(hubs)} hubs")
        return df


    def _compute_fleet_breakdown(
        self,
        routes_df: pd.DataFrame,
        viagens_por_veiculo_por_dia: int = 2,   # 👈 veículo faz 2 viagens por dia
        dias_uteis: int = 26                    # 👈 26 dias úteis/mês
    ) -> Dict[str, int]:
        """
        Converte nº de VIAGENS (linhas de routes_df) em nº de VEÍCULOS,
        considerando múltiplas viagens por veículo/mês e faixas de capacidade.
        Espera 'peso_total_subrota' ≈ carga média por VIAGEM.
        """

        import math

        # (nome, capacidade_min_exclusiva, capacidade_max_inclusiva) em kg
        caps = [
            ("Motocicleta", 0, 200),
            ("Fiorino", 200, 600),
            ("HR", 600, 1800),
            ("3/4", 1800, 4000),
            ("Toco", 4000, 6000),
            ("Truck", 6000, 999999),
        ]

        # 1) Classifica CADA VIAGEM pelo peso médio → soma nº de viagens por tipo
        viagens_por_tipo = {k: 0 for k, _, _ in caps}
        for w in routes_df["peso_total_subrota"].astype(float):
            for name, lo, hi in caps:
                if lo < w <= hi:
                    viagens_por_tipo[name] += 1
                    break

        # 2) Converte viagens em veículos, considerando que 1 veículo faz várias viagens no mês
        viagens_por_veiculo_no_mes = max(1, viagens_por_veiculo_por_dia * dias_uteis)
        frota = {}
        for tipo, n_viagens in viagens_por_tipo.items():
            frota[tipo] = int(math.ceil(n_viagens / viagens_por_veiculo_no_mes))

        return frota


    def evaluate_month(self, month_df: pd.DataFrame, hubs: List[Tuple[str, str]], k: int, tenant_id: str) -> Dict[str, Any]:
        assigned = self.assign_cities_to_hubs(month_df, hubs)

        if self.logger:
            self.logger.info(f"🚚 Evaluate_month: {len(assigned)} cidades atribuídas, chamando custos...")

        # === Chamada de custos (simulation_db + microserviços) ===
        custo_transfer = float(self.ct.estimate(assigned, tenant_id=tenant_id))
        lm_result = self.lm.estimate(assigned, tenant_id=tenant_id)  # deve retornar rotas/subrotas
        custo_lastmile = float(lm_result["custo_total"])
        rotas_df = lm_result["rotas_df"]  # DataFrame com 'peso_total_subrota' (ou adapte)

        # 👇 Ajuste aqui, se quiser parametrizar por cenário/tenant
        frota = self._compute_fleet_breakdown(
            rotas_df,
            viagens_por_veiculo_por_dia=2,
            dias_uteis=26
        )


        res = {
            "k_clusters": k,
            "hubs": hubs,
            "frota": frota,
            "custo_transferencia": custo_transfer,
            "custo_last_mile": custo_lastmile,
            "custo_total": custo_transfer + custo_lastmile,
        }

        if self.logger:
            self.logger.info(f"✅ Evaluate_month: k={k}, custo_total={res['custo_total']:.2f}, frota={res['frota']}")

        return res

    def plan(self, forecast_df: pd.DataFrame, tenant_id: str, hub_candidates: List[Tuple[str, str]]) -> pd.DataFrame:
        forecast_df = forecast_df.copy()
        forecast_df["mes"] = pd.to_datetime(forecast_df["data"]).dt.to_period("M").astype(str)

        results = []
        for mes, month_df in forecast_df.groupby("mes"):
            if self.logger:
                self.logger.info(f"📆 Planejando mês {mes}...")

            shortlist = self.shortlist_k(month_df, tenant_id)
            cand = hub_candidates[:8]  # limita busca

            best = None
            for k in shortlist:
                for hubs in itertools.combinations(cand, k):
                    res = self.evaluate_month(month_df, list(hubs), k, tenant_id)
                    if (best is None) or (res["custo_total"] < best["custo_total"]):
                        best = res

            best["mes"] = mes
            results.append(best)

        cols = ["mes", "k_clusters", "hubs", "frota", "custo_transferencia", "custo_last_mile", "custo_total"]
        df = pd.DataFrame(results, columns=cols)

        if self.logger:
            self.logger.info(f"📊 Plano consolidado ({len(df)} meses)")

        return df
