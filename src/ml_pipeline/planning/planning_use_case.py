#hub_router_1.0.1/src/ml_pipeline/planning/planning_use_case.py

import os
import pandas as pd
from datetime import datetime

from .demand_forecaster import DemandForecaster
from .scenario_generator import ScenarioGenerator
from .hub_candidates import HubCandidates
from .structure_optimizer import StructureOptimizer


class PlanningUseCase:
    def __init__(self, repository, ml_pipeline, ct_client, lm_client, logger=None):
        """
        repository -> DatasetRepository, que conecta em simulation_db e clusterization_db
        """
        self.repo = repository
        self.ml = ml_pipeline
        self.ct = ct_client
        self.lm = lm_client
        self.logger = logger

    def _export_debug(self, tenant_id: str, scenario: str, forecast_df: pd.DataFrame, plan_df: pd.DataFrame):
        """Salva CSVs em exports/ml_pipeline/{tenant_id}/data_execucao/"""
        base_dir = f"/app/exports/ml_pipeline/{tenant_id}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        os.makedirs(base_dir, exist_ok=True)

        forecast_path = os.path.join(base_dir, f"forecast_{scenario}.csv")
        plan_path = os.path.join(base_dir, f"plan_{scenario}.csv")

        forecast_df.to_csv(forecast_path, index=False)
        plan_df.to_csv(plan_path, index=False)

        if self.logger:
            self.logger.info(f"💾 Debug export: {forecast_path}, {plan_path}")

    def recommend_structure(self, tenant_id: str, start_date: str, months: int,
                        scenarios=("base",), debug=False, fast=False):

        # 🔎 1. histórico
        hist = self.repo.load_city_daily_history(tenant_id)
        if self.logger:
            self.logger.info(f"📦 Histórico carregado de clusterization_db: {hist.shape}")

        # ⚡ Se fast=True, reduz para no máximo 10 cidades
        cidades_amostra = None
        if fast and not hist.empty:
            cidades = hist[["cidade", "uf"]].drop_duplicates()
            cidades_amostra = cidades.sample(n=min(10, len(cidades)), random_state=42)
            hist = hist.merge(cidades_amostra, on=["cidade", "uf"])
            if self.logger:
                self.logger.info(
                    f"⚡ FAST MODE: reduzindo histórico para {hist['cidade'].nunique()} cidades ({hist.shape})"
                )

        # 🔎 2. candidatos a hubs
        cand = HubCandidates()
        hub_cands = cand.select(hist, top_n=(5 if fast else 20))   # 👈 corta hubs no modo fast
        if self.logger:
            self.logger.info(f"🏙️ Hub candidates gerados: {len(hub_cands)}")

        # 🔎 3. inicializa otimizador
        optimizer = StructureOptimizer(
            self.ml,
            self.ct,
            self.lm,
            logger=self.logger,
            fast=fast  # 👈 passa flag
        )

        # 🔎 4. previsões + planos
        planos = {}
        forecaster = DemandForecaster(self.repo, logger=self.logger)
        for c in (["base"] if fast else scenarios):   # 👈 só 1 cenário se fast
            if self.logger:
                self.logger.info(
                    f"🔮 Rodando cenário '{c}' para {months if not fast else 1} meses a partir de {start_date}..."
                )

            fc = forecaster.forecast_city_daily(
                start_date,
                months if not fast else 1,   # 👈 só 1 mês se fast
                tenant_id,
                scenario=c
            )

            # ⚡ Se fast=True, filtra forecast só pras cidades amostradas
            if fast and cidades_amostra is not None:
                fc = fc.merge(cidades_amostra, on=["cidade", "uf"])
                if self.logger:
                    self.logger.info(
                        f"⚡ FAST MODE: forecast reduzido para {fc['cidade'].nunique()} cidades ({fc.shape})"
                    )

            df = optimizer.plan(fc, tenant_id, hub_cands)

            if self.logger:
                self.logger.info(f"✅ Estrutura otimizada para cenário '{c}': {df.shape}")

            if debug:
                self._export_debug(tenant_id, c, fc, df)

            planos[c] = df

        return planos
