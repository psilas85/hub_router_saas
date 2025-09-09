# hub_router_1.0.1/src/machine_learning/demand/main_demand.py

import os
import argparse
import pandas as pd
from dotenv import load_dotenv

from machine_learning.demand.infrastructure.demand_repository import DemandRepository
from machine_learning.demand.application.demand_use_case import DemandUseCase
from machine_learning.demand.utils.feature_engineering import build_features
from machine_learning.demand.models.benchmark import run_benchmark
from machine_learning.demand.models.benchmark_multi import run_benchmark_all  # 👈 novo import


if __name__ == "__main__":
    # Carrega variáveis do .env
    load_dotenv("src/machine_learning/.env")

    parser = argparse.ArgumentParser(description="Forecast de demanda por cidade")

    parser.add_argument("--tenant_id", type=str, default=os.getenv("TENANT_ID", None),
                        help="Tenant ID (default: variável de ambiente TENANT_ID)")
    parser.add_argument("--cidade", type=str, required=False,
                        help="Cidade alvo (se não passar, pega a primeira disponível)")
    parser.add_argument("--start_date", type=str, default="2023-01-01",
                        help="Data inicial (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, default="2025-12-31",
                        help="Data final (YYYY-MM-DD)")
    parser.add_argument("--horizon", type=int, default=14,
                        help="Horizonte de previsão (dias)")
    parser.add_argument(
        "--method",
        type=str,
        default="baseline",
        choices=["baseline", "xgboost", "lightgbm"],
        help="Método de previsão (baseline, xgboost ou lightgbm)"
    )
    parser.add_argument("--with_features", action="store_true",
                        help="Se passar, gera CSV com features além do forecast")
    parser.add_argument("--benchmark", action="store_true",
                        help="Se passar, roda benchmark para uma cidade")
    parser.add_argument("--benchmark_all", action="store_true",
                        help="Se passar, roda benchmark para todas as cidades")
    parser.add_argument("--forecast_all", action="store_true",
                        help="Se passar, gera forecast multi-cidades para o horizonte informado")  # 👈 nova flag
    parser.add_argument("--output_csv", type=str,
                        default="/app/exports/machine_learning/forecast_next_90d.csv",
                        help="Caminho de saída do forecast multi-cidades")  # 👈 novo parâmetro

    args = parser.parse_args()

    repo = DemandRepository()
    use_case = DemandUseCase(repo)

    # Busca dados brutos
    df_debug = repo.fetch_daily_city(args.tenant_id, args.start_date, args.end_date)
    print("🔎 Amostra de linhas reais:")
    print(df_debug.head(20))

    if df_debug.empty:
        print("⚠️ Nenhum dado encontrado no período.")
        exit(1)

    # Define cidade alvo
    if args.cidade:
        cidade = args.cidade
    else:
        cidade = df_debug["cidade"].iloc[0]
        print(f"⚠️ Nenhuma cidade passada, usando '{cidade}' como default")

    # ===================================
    # 🚀 Benchmark multi-cidades
    # ===================================
    if args.benchmark_all:
        print("🌍 Rodando benchmark em TODAS as cidades...")
        results_all = run_benchmark_all(df_debug, horizon=args.horizon)
        out_path = "/app/exports/machine_learning/benchmark_all_results.csv"
        results_all.to_csv(out_path, index=False)
        print(f"✅ Benchmark multi-cidades salvo em {out_path}")
        print(results_all.groupby("modelo")[['mae', 'rmse', 'r2']].mean())
        exit(0)

    # ===================================
    # 🚀 Forecast multi-cidades
    # ===================================
    if args.forecast_all:
        print("🌍 Gerando forecast multi-cidades...")
        cidades = df_debug["cidade"].unique().tolist()
        all_forecasts = []

        for cid in cidades:
            try:
                forecast = use_case.forecast_demand(
                    tenant_id=args.tenant_id,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    cidade=cid,
                    horizon=args.horizon,
                    method=args.method,
                )
                if isinstance(forecast, tuple):
                    forecast = forecast[0]  # pega DataFrame principal

                forecast["cidade"] = cid
                # renomeia para colunas esperadas pelo k_cluster
                forecast = forecast.rename(columns={
                    "entregas": "quantidade_entregas",
                    "peso": "cte_peso",
                    "volumes": "cte_volumes"
                })
                all_forecasts.append(forecast)

            except Exception as e:
                print(f"⚠️ Erro ao gerar forecast para {cid}: {e}")

        if all_forecasts:
            df_out = pd.concat(all_forecasts, ignore_index=True)
            df_out.to_csv(args.output_csv, index=False)
            print(f"✅ Forecast multi-cidades salvo em {args.output_csv}")
        else:
            print("⚠️ Nenhum forecast gerado.")
        exit(0)

    # ===================================
    # 🚀 Benchmark de uma cidade
    # ===================================
    if args.benchmark:
        print("🚀 Rodando benchmark entre Baseline, XGBoost e LightGBM...")
        df_results = run_benchmark(df_debug, cidade=cidade, horizon=args.horizon)

        out_path = "/app/exports/machine_learning/benchmark_results.csv"
        df_results.to_csv(out_path, index=False)
        print(f"✅ Benchmark salvo em {out_path}")
        print(df_results)
        exit(0)

    # ===================================
    # Forecast normal (uma cidade)
    # ===================================
    try:
        forecast = use_case.forecast_demand(
            tenant_id=args.tenant_id,
            start_date=args.start_date,
            end_date=args.end_date,
            cidade=cidade,
            horizon=args.horizon,
            method=args.method,
        )

        if isinstance(forecast, tuple):
            forecast, df_features_or_metrics = forecast
            if "mae" in df_features_or_metrics:  # dict de métricas
                metrics = df_features_or_metrics
                metrics_path = "/app/exports/machine_learning/metrics_sample.csv"
                pd.DataFrame([metrics]).to_csv(metrics_path, index=False)
                print(f"✅ Métricas salvas em {metrics_path}")
            else:
                df_features = df_features_or_metrics
                df_features.to_csv("/app/exports/machine_learning/features_sample.csv", index=False)
                print("✅ Features salvas em /exports/machine_learning/features_sample.csv")

        print(forecast)
        forecast.to_csv("/app/exports/machine_learning/forecast_sample.csv", index=False)
        print("✅ Forecast salvo em /app/exports/machine_learning/forecast_sample.csv")

    except ValueError as e:
        print(f"⚠️ Erro no forecast: {e}")

    if args.with_features:
        df_city = df_debug[df_debug["cidade"] == cidade].copy()
        df_feat = build_features(df_city, target_col="entregas")
        df_feat.to_csv("/app/exports/machine_learning/features_sample.csv", index=False)
        print("✅ Features salvas em /app/exports/machine_learning/features_sample.csv")
