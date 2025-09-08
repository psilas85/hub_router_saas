# hub_router_1.0.1/src/ml_pipeline/planning/planning_use_case.py

import os
import pandas as pd
from datetime import datetime

from .demand_forecaster import DemandForecaster
from .scenario_generator import ScenarioGenerator
from .hub_candidates import HubCandidates
from .structure_optimizer import StructureOptimizer


class PlanningUseCase:
    def __init__(self, repository, ml_pipeline, ct_client, lm_client, geo_adapter=None, logger=None):
        """
        repository -> DatasetRepository, que conecta em simulation_db e clusterization_db
        """
        self.repo = repository
        self.ml = ml_pipeline
        self.ct = ct_client
        self.lm = lm_client
        self.geo_adapter = geo_adapter
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
        """
        Consolida os resultados das simulações já realizadas (tabela resultados_simulacao).
        Seleciona pontos ótimos no período e gera planejamento por mês.
        Suporta cenários: base, baixo (otimista), alto (pessimista).
        """

        import pandas as pd
        from datetime import datetime
        import os

        # 🔎 Ajusta intervalo de datas
        start_dt = pd.to_datetime(start_date)
        end_dt = (start_dt + pd.DateOffset(months=months)).strftime("%Y-%m-%d")

        # 🔎 Carrega simulações no período
        df = self.repo.load_simulation_dataset(
            start_date=start_date,
            end_date=end_dt,
            tenant_id=tenant_id
        )

        if df.empty:
            if self.logger:
                self.logger.warning(f"⚠️ Nenhuma simulação encontrada no período {start_date}..{end_dt}")
            return {}

        if self.logger:
            self.logger.info(f"📦 Simulações carregadas: {df.shape}")

        # 🔎 Filtra pontos ótimos
        pontos_otimos = df[df["is_ponto_otimo"] == 1].copy()
        if pontos_otimos.empty:
            if self.logger:
                self.logger.warning("⚠️ Nenhum ponto ótimo marcado nas simulações.")
            return {}

        # 🔎 Adiciona coluna de mês
        pontos_otimos["mes"] = pd.to_datetime(pontos_otimos["envio_data"]).dt.to_period("M").astype(str)

        # 🔎 Agregação base por mês
        plano_base = (
            pontos_otimos
            .groupby("mes")
            .agg({
                "custo_transfer_total": "sum",
                "custo_total": "sum",
                "total_entregas": "sum",
                "total_peso": "sum",
                "total_volumes": "sum"
            })
            .reset_index()
        )
        plano_base["custo_last_mile"] = plano_base["custo_total"] - plano_base["custo_transfer_total"]

        # 🔎 Monta dicionário de cenários
                # 🔎 Monta dicionário de cenários
        planos = {}

        for c in scenarios:
            df_c = plano_base.copy()

            if c.lower() in ["baixo", "otimista"]:
                # cenário otimista = custos –10%
                df_c["custo_transfer_total"] *= 0.9
                df_c["custo_last_mile"] *= 0.9
                df_c["custo_total"] = df_c["custo_transfer_total"] + df_c["custo_last_mile"]

            elif c.lower() in ["alto", "pessimista"]:
                # cenário pessimista = custos +10%
                df_c["custo_transfer_total"] *= 1.1
                df_c["custo_last_mile"] *= 1.1
                df_c["custo_total"] = df_c["custo_transfer_total"] + df_c["custo_last_mile"]

            planos[c] = df_c.to_dict(orient="records")

            if self.logger:
                self.logger.info(f"✅ Cenário '{c}' gerado: {len(df_c)} meses")

            if debug:
                base_dir = f"/app/exports/ml_pipeline/{tenant_id}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
                os.makedirs(base_dir, exist_ok=True)
                df_c.to_csv(os.path.join(base_dir, f"plano_{c}.csv"), index=False)

        # 🔎 Debug consolidado (fora do loop de cenários)
        debug_df = plano_base.copy()
        debug_df["custo_medio_entrega"] = (
            debug_df["custo_total"] / debug_df["total_entregas"].replace(0, pd.NA)
        )

        debug_dir = f"/app/exports/ml_pipeline/{tenant_id}/debug_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
        os.makedirs(debug_dir, exist_ok=True)
        debug_path = os.path.join(debug_dir, "debug_mensal.csv")
        debug_df.to_csv(debug_path, index=False)

        if self.logger:
            self.logger.info(f"💾 Debug mensal exportado em {debug_path}")
            for _, row in debug_df.iterrows():
                self.logger.info(
                    f"📊 {row['mes']} | entregas={row['total_entregas']} | "
                    f"custo_total={row['custo_total']:.2f} | "
                    f"custo_medio_entrega={row['custo_medio_entrega']:.2f}"
                )

        return planos

    def recommend_structure_v2(self, tenant_id: str, start_date: str, months: int,
                                scenarios=("base",), debug=False, fast=False):
        """
        Versão final corrigida:
        - Usa DemandForecaster para prever entregas/peso/volumes por mês.
        - Carrega modelos ML salvos (last-mile e transfer).
        - Alinha as features com o treino dos modelos.
        - Se modelos não existirem, aplica fallback de custo médio histórico.
        - Calcula custo_total = lastmile + transfer.
        - Aplica cenários (baixo, base, alto).
        """
        from ml_pipeline.models.trainer_factory import TrainerFactory

        # 1. Forecast diário
        forecaster = DemandForecaster(self.repo, logger=self.logger)
        forecast_df = forecaster.forecast_city_daily(
            start_date=start_date,
            months=months,
            tenant_id=tenant_id,
            scenario="base"
        )

        if forecast_df.empty:
            if self.logger:
                self.logger.warning(f"⚠️ Forecast vazio para tenant={tenant_id} de {start_date}+{months}m")
            return {}

        # 2. Apenas dias úteis
        forecast_df = forecast_df[forecast_df["data"].dt.weekday < 5]

        # 3. Agregação mensal
        forecast_df["mes"] = pd.to_datetime(forecast_df["data"]).dt.to_period("M").astype(str)
        mensal = (
            forecast_df.groupby("mes")
            .agg({
                "entregas": "sum",
                "peso": "sum",
                "volumes": "sum",
                "valor_nf": "sum"
            })
            .reset_index()
        )

        # 🔑 3b. Renomeia colunas para bater com treino dos modelos
        features = mensal.rename(columns={
            "entregas": "total_entregas",
            "peso": "total_peso",
            "volumes": "total_volumes",
            "valor_nf": "valor_total_nf"
        })[["total_entregas", "total_peso", "total_volumes", "valor_total_nf"]]

        # 4. Predição com modelos salvos
        try:
            model_last = TrainerFactory.load_trained("custo_last_mile", tenant_id)
            model_trans = TrainerFactory.load_trained("custo_transfer_total", tenant_id)

            mensal["custo_last_mile"] = model_last.predict(features)
            mensal["custo_transfer_total"] = model_trans.predict(features)

        except Exception as e:
            if self.logger:
                self.logger.error(f"⚠️ Erro ao carregar modelos salvos: {e}. Usando fallback média histórica.")
            custo_medio = self.repo.load_avg_cost_per_delivery(start_date, start_date, tenant_id) or 60.0
            mensal["custo_last_mile"] = mensal["entregas"] * custo_medio
            mensal["custo_transfer_total"] = 0.0

        mensal["custo_total"] = mensal["custo_last_mile"] + mensal["custo_transfer_total"]

        # 5. Cenários
        planos = {}
        for c in scenarios:
            df_c = mensal.copy()
            if c.lower() in ["baixo", "otimista"]:
                factor = 0.9
            elif c.lower() in ["alto", "pessimista"]:
                factor = 1.1
            else:
                factor = 1.0

            df_c["custo_last_mile"] *= factor
            df_c["custo_transfer_total"] *= factor
            df_c["custo_total"] = df_c["custo_last_mile"] + df_c["custo_transfer_total"]

            planos[c] = df_c.to_dict(orient="records")

            if self.logger:
                self.logger.info(f"✅ Cenário '{c}' gerado: {len(df_c)} meses")

            if debug:
                base_dir = f"/app/exports/ml_pipeline/{tenant_id}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}"
                os.makedirs(base_dir, exist_ok=True)
                df_c.to_csv(os.path.join(base_dir, f"plano_v2_{c}.csv"), index=False)

        return planos
