# hub_router_1.0.1/src/machine_learning/k_cluster/utils/feature_builder.py
from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
import numpy as np
import pandas as pd


@dataclass
class FeatureConfig:
    min_k: int = 1
    max_k: int = 20
    col_qtd: str = "quantidade_entregas"
    col_peso: str = "cte_peso"
    col_vols: str = "cte_volumes"


class FeatureBuilder:
    """Constrói features para treino (histórico) e para predição (futuro)."""

    def __init__(self, cfg: FeatureConfig = FeatureConfig()):
        self.cfg = cfg
        self._feature_cols_: Optional[List[str]] = None

    # --------- Treino (histórico) ---------
    def build_training_matrix(
        self,
        resultados_simulacao: pd.DataFrame,
        resumo_clusters: Optional[pd.DataFrame] = None,
        entregas_clusterizadas: Optional[pd.DataFrame] = None,
    ) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
        """
        Retorna:
          X (features sem custos),
          y (binário se k == k_ótimo),
          meta (alinhado a X/y, com colunas: tenant_id, envio_data, k_clusters, k_otimo, custo_total)
        """
        df_res = resultados_simulacao.copy()
        if "envio_data" in df_res.columns:
            df_res["envio_data"] = pd.to_datetime(df_res["envio_data"]).dt.date

        # alvo: k ótimo = menor custo_total onde is_ponto_otimo = true
        target_by_day = (
            df_res[df_res["is_ponto_otimo"].astype(str).str.lower().isin(["t", "true", "1"])]
            .sort_values(["tenant_id", "envio_data", "custo_total"], ascending=[True, True, True])
            .groupby(["tenant_id", "envio_data"], as_index=False)[["k_clusters"]]
            .first()
            .rename(columns={"k_clusters": "k_otimo"})
        )

        # agregados básicos por tenant/data/k (mantém custos apenas para meta/avaliação)
        base = (
            df_res.groupby(["tenant_id", "envio_data", "k_clusters"], as_index=False)
            .agg(
                custo_total=("custo_total", "mean"),
                custo_transferencia=("custo_transferencia", "mean"),
                custo_last_mile=("custo_last_mile", "mean"),
                custo_cluster=("custo_cluster", "mean"),
                qde_ctes=("quantidade_entregas", "mean"),
            )
        )

        # enriquecer com resumo_clusters
        if resumo_clusters is not None and not resumo_clusters.empty:
            rc = resumo_clusters.copy()
            rc["envio_data"] = pd.to_datetime(rc["envio_data"]).dt.date
            rc_agg = (
                rc.groupby(["tenant_id", "envio_data", "k_clusters"], as_index=False)
                .agg(
                    clusters=("cluster", "nunique"),
                    peso_total_kg=("peso_total_kg", "sum"),
                    volumes_total=("volumes_total", "sum"),
                    qde_ctes_sum=("qde_ctes", "sum"),
                    peso_medio_cluster=("peso_total_kg", "mean"),
                    vols_medios_cluster=("volumes_total", "mean"),
                )
            )
            base = base.merge(rc_agg, on=["tenant_id", "envio_data", "k_clusters"], how="left")

        # enriquecer com entregas_clusterizadas
        if entregas_clusterizadas is not None and not entregas_clusterizadas.empty:
            ec = entregas_clusterizadas.copy()
            ec["envio_data"] = pd.to_datetime(ec["envio_data"]).dt.date

            demand_agg = (
                ec.groupby(["tenant_id", "envio_data", "cluster"], as_index=False)
                .agg(
                    cidades_uniq=("cluster_cidade", "nunique"),
                    qtd_sum=(self.cfg.col_qtd, "sum") if self.cfg.col_qtd in ec.columns else ("cluster", "size"),
                    peso_sum=(self.cfg.col_peso, "sum") if self.cfg.col_peso in ec.columns else ("cluster", "size"),
                    vols_sum=(self.cfg.col_vols, "sum") if self.cfg.col_vols in ec.columns else ("cluster", "size"),
                )
            )

            if resumo_clusters is not None and not resumo_clusters.empty:
                bridge = resumo_clusters[["tenant_id", "envio_data", "k_clusters", "cluster"]].copy()
                bridge["envio_data"] = pd.to_datetime(bridge["envio_data"]).dt.date

                demand_k = demand_agg.merge(bridge, on=["tenant_id", "envio_data", "cluster"], how="left")
                demand_k_agg = (
                    demand_k.groupby(["tenant_id", "envio_data", "k_clusters"], as_index=False)
                    .agg(
                        cidades_totais=("cidades_uniq", "sum"),
                        qtd_total=("qtd_sum", "sum"),
                        peso_total=("peso_sum", "sum"),
                        vols_total=("vols_sum", "sum"),
                        clusters_count=("cluster", "nunique"),
                    )
                )
                base = base.merge(demand_k_agg, on=["tenant_id", "envio_data", "k_clusters"], how="left")

        # junta target
        train = base.merge(target_by_day, on=["tenant_id", "envio_data"], how="inner")

        # ======== SANEAR FEATURES (remover custos e razões derivadas de custos) ========
        ban = {
            "custo_total",
            "custo_transferencia",
            "custo_last_mile",
            "custo_cluster",
        }
        feature_cols = [
            c for c in train.columns
            if c not in ban | {"k_otimo", "tenant_id", "envio_data", "k_clusters"}
        ]
        # features úteis derivadas (se existirem)
        if "qde_ctes" in train.columns and "k_clusters" in train.columns and "qde_ctes_cluster_medio" not in train.columns:
            train["qde_ctes_cluster_medio"] = train["qde_ctes"] / train["k_clusters"].replace(0, np.nan)
            feature_cols.append("qde_ctes_cluster_medio")

        X = train[feature_cols].fillna(0.0).astype(np.float32)

        y = (train["k_clusters"].astype(int) == train["k_otimo"].astype(int)).astype(int)

        # meta alinhado (inclui custo_total para avaliação)
        meta = train[["tenant_id", "envio_data", "k_clusters", "k_otimo", "custo_total"]].copy()

        self._feature_cols_ = feature_cols
        return X, y, meta

    # --------- Predição (futuro) ---------
    def build_prediction_rows(
        self,
        demand_forecast: pd.DataFrame,
        tenant_id: str,
        envio_data: str,
        candidate_ks: Optional[List[int]] = None,
    ):
        if candidate_ks is None:
            candidate_ks = list(range(self.cfg.min_k, self.cfg.max_k + 1))

        df = demand_forecast.copy()
        if "envio_data" in df.columns:
            df["envio_data"] = pd.to_datetime(df["envio_data"]).dt.date

        qtd_total = df.get(self.cfg.col_qtd, pd.Series([len(df)])).sum()
        peso_total = df.get(self.cfg.col_peso, pd.Series([0])).sum()
        vols_total = df.get(self.cfg.col_vols, pd.Series([0])).sum()
        cidades_totais = df["cidade"].nunique() if "cidade" in df.columns else np.nan

        rows = []
        for k in candidate_ks:
            rows.append(
                {
                    # ❌ não incluir custos nem razões de custo na inferência
                    "qde_ctes": float(qtd_total),
                    "clusters": int(k),
                    "peso_total_kg": float(peso_total),
                    "volumes_total": float(vols_total),
                    "qde_ctes_sum": float(qtd_total),
                    "peso_medio_cluster": float((peso_total / k) if k else 0.0),
                    "vols_medios_cluster": float((vols_total / k) if k else 0.0),
                    "cidades_totais": float(cidades_totais) if not pd.isna(cidades_totais) else 0.0,
                    "qtd_total": float(qtd_total),
                    "peso_total": float(peso_total),
                    "vols_total": float(vols_total),
                    "clusters_count": int(k),
                    "qde_ctes_cluster_medio": float((qtd_total / k) if k else 0.0),
                }
            )

        # garante mesmas colunas do treino
        feature_cols = self._feature_cols_ or list(rows[0].keys())
        X_pred = pd.DataFrame(rows)[feature_cols].fillna(0.0).astype(np.float32)

        meta_pred = pd.DataFrame(
            {
                "tenant_id": tenant_id,
                "envio_data": pd.to_datetime(envio_data).date(),
                "k_clusters": candidate_ks,
            }
        )
        return X_pred, meta_pred
