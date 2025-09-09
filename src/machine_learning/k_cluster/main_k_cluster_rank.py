# hub_router_1.0.1/src/machine_learning/k_cluster/main_k_cluster_rank.py
from __future__ import annotations
import argparse
import json
import pandas as pd
from datetime import date

from .infrastructure.repositories import PgResultsRepository
from .infrastructure.model_store import LocalKModelStore
from .utils.feature_builder import FeatureBuilder
from .models.k_cluster_ranker import KClusterRanker


def _pretty_metrics_rank(metrics: dict):
    print("\n📊 Métricas (Learning-to-Rank)")
    print("-" * 50)
    print(f"Regret@1 (mean)    : {metrics.get('regret_mean'):.2f}")
    print(f"Regret@1 (p50)     : {metrics.get('regret_p50'):.2f}")
    print(f"Regret@1 (p90)     : {metrics.get('regret_p90'):.2f}")
    print(f"Hit@3              : {metrics.get('hit_at_3'):.3f}")
    print("-" * 50)
    print(f"Train samples      : {metrics.get('n_train')}")
    print(f"Test samples       : {metrics.get('n_test')}")
    print(f"N Features         : {metrics.get('n_features')}")
    print(f"Groups (train/test): {metrics.get('groups_train')} / {metrics.get('groups_test')}")
    print("-" * 50)


def cmd_train(args):
    fb = FeatureBuilder()
    ranker = KClusterRanker(fb)

    repo = PgResultsRepository()
    resultados = repo.load_resultados_simulacao(args.tenant_id, args.start, args.end)
    resumo = repo.load_resumo_clusters(args.tenant_id, args.start, args.end)
    entregas = repo.load_entregas_clusterizadas(args.tenant_id, args.start, args.end)

    metrics = ranker.fit(resultados, resumo, entregas, test_size=args.test_size)

    print(json.dumps({"detail": "Treino concluído (ranker)", "metrics": metrics}, indent=2, ensure_ascii=False))
    _pretty_metrics_rank(metrics)

    payload = {"model": ranker.model, "feature_cols": fb._feature_cols_, "cfg": fb.cfg}
    path = LocalKModelStore().save(args.tenant_id, payload)
    print(f"\n📁 Modelo salvo em {path}")


def cmd_predict(args):
    fb = FeatureBuilder()
    ranker = KClusterRanker(fb)

    payload = LocalKModelStore().load(args.tenant_id)
    ranker.model = payload["model"]
    fb._feature_cols_ = payload.get("feature_cols", None)
    fb.cfg = payload.get("cfg", fb.cfg)

    df = pd.read_csv(args.demand_csv)
    out = ranker.predict_k(
        demand_forecast=df,
        tenant_id=args.tenant_id,
        envio_data=args.envio_data,
        candidate_ks=list(range(args.min_k, args.max_k + 1)),
        top_n=args.top_n,
    )

    print(json.dumps(out, indent=2, ensure_ascii=False))


def cmd_plan_fixed(args):
    """
    Gera um plano de k FIXO para um horizonte (ex.: 3 meses) agregando as predições do Ranker por dia.
    Entrada: CSV com várias linhas de forecast (inclui coluna envio_data, e.g. 'YYYY-MM-DD').
    Saída: JSON com k_fix recomendado + métricas de robustez; CSV opcional com diagnóstico por dia.
    """
    fb = FeatureBuilder()
    ranker = KClusterRanker(fb)

    payload = LocalKModelStore().load(args.tenant_id)
    ranker.model = payload["model"]
    fb._feature_cols_ = payload.get("feature_cols", None)
    fb.cfg = payload.get("cfg", fb.cfg)

    df_all = pd.read_csv(args.forecast_csv)
    if "envio_data" not in df_all.columns:
        raise ValueError("O CSV de forecast precisa ter a coluna 'envio_data' (YYYY-MM-DD).")

    # normaliza data
    df_all["envio_data"] = pd.to_datetime(df_all["envio_data"]).dt.date

    candidate_ks = list(range(args.min_k, args.max_k + 1))

    # agregadores
    from collections import defaultdict
    rank_sums = defaultdict(float)      # soma de ranks por k (quanto menor melhor)
    score_sums = defaultdict(float)     # soma de scores por k (quanto maior melhor)
    top1_counts = defaultdict(int)      # vezes que k foi Top-1 no dia
    top3_counts = defaultdict(int)      # vezes que k ficou no Top-3

    per_day_rows = []  # diagnóstico opcional por dia

    # percorre dias únicos no CSV de forecast
    for envio_d, df_day in df_all.groupby("envio_data"):
        out = ranker.predict_k(
            demand_forecast=df_day,
            tenant_id=args.tenant_id,
            envio_data=str(envio_d),
            candidate_ks=candidate_ks,
            top_n=min(args.top_n, len(candidate_ks)),
        )
        dist = pd.DataFrame(out["distribuicao"])  # colunas: tenant_id, envio_data, k_clusters, score
        # rank 1 = melhor (maior score)
        dist = dist.sort_values("score", ascending=False).reset_index(drop=True)
        dist["rank"] = dist.index + 1

        # acumula métricas por k neste dia
        for _, r in dist.iterrows():
            k = int(r["k_clusters"])
            rank_sums[k] += int(r["rank"])
            score_sums[k] += float(r["score"])

        # top1/top3
        top1_k = int(dist.iloc[0]["k_clusters"])
        top1_counts[top1_k] += 1
        for k in dist.head(3)["k_clusters"].astype(int).tolist():
            top3_counts[k] += 1

        if args.save_daily:
            per_day_rows.append({
                "envio_data": str(envio_d),
                "k_top1": int(dist.iloc[0]["k_clusters"]),
                "k_top2": int(dist.iloc[1]["k_clusters"]) if len(dist) > 1 else None,
                "k_top3": int(dist.iloc[2]["k_clusters"]) if len(dist) > 2 else None,
            })

    n_days = df_all["envio_data"].nunique()

    # consolida por k
    rows = []
    for k in candidate_ks:
        rows.append({
            "k_clusters": k,
            "rank_avg": (rank_sums[k] / n_days) if n_days else float("inf"),
            "score_avg": (score_sums[k] / n_days) if n_days else float("-inf"),
            "top1_share": (top1_counts[k] / n_days) if n_days else 0.0,
            "top3_share": (top3_counts[k] / n_days) if n_days else 0.0,
            "top1_days": top1_counts[k],
            "top3_days": top3_counts[k],
        })
    agg = pd.DataFrame(rows)

    # escolha final: k com menor rank médio (critério principal)
    k_best_rank = int(agg.sort_values(["rank_avg", "score_avg"], ascending=[True, False]).iloc[0]["k_clusters"])
    # apoio: maior score médio
    k_best_score = int(agg.sort_values(["score_avg", "top1_share"], ascending=[False, False]).iloc[0]["k_clusters"])
    # apoio: mais vitórias (top1)
    k_most_top1 = int(agg.sort_values(["top1_days", "top3_days", "score_avg"], ascending=[False, False, False]).iloc[0]["k_clusters"])

    result = {
        "detail": "Plano de k FIXO gerado com Ranker",
        "tenant_id": args.tenant_id,
        "horizon_days": n_days,
        "candidate_ks": candidate_ks,
        "k_fixed_recommended": k_best_rank,
        "supporting_choices": {
            "k_by_avg_rank": k_best_rank,
            "k_by_avg_score": k_best_score,
            "k_by_top1_count": k_most_top1
        },
        "summary": agg.sort_values("rank_avg").to_dict(orient="records")
    }

    print(json.dumps(result, indent=2, ensure_ascii=False))

    # salva CSVs opcionais
    if args.output_agg:
        agg.to_csv(args.output_agg, index=False)
        print(f"\n📄 Resumo por k salvo em {args.output_agg}")
    if args.save_daily and args.output_daily:
        pd.DataFrame(per_day_rows).to_csv(args.output_daily, index=False)
        print(f"📄 Top-3 por dia salvo em {args.output_daily}")


if __name__ == "__main__":
    p = argparse.ArgumentParser("k_cluster_rank")
    sub = p.add_subparsers(required=True)

    p_train = sub.add_parser("train", help="Treinar Ranker (LightGBM LambdaMART)")
    p_train.add_argument("--tenant_id", required=True)
    p_train.add_argument("--start", required=False)
    p_train.add_argument("--end", required=False)
    p_train.add_argument("--test_size", type=float, default=0.2)
    p_train.set_defaults(func=cmd_train)

    p_pred = sub.add_parser("predict", help="Prever k (ranking de ks para a demanda prevista)")
    p_pred.add_argument("--tenant_id", required=True)
    p_pred.add_argument("--envio_data", required=True)
    p_pred.add_argument("--demand_csv", required=True)
    p_pred.add_argument("--min_k", type=int, default=2)
    p_pred.add_argument("--max_k", type=int, default=12)
    p_pred.add_argument("--top_n", type=int, default=3)
    p_pred.set_defaults(func=cmd_predict)

    # ===== novo subcomando: plano FIXO =====
    p_plan = sub.add_parser("plan_fixed", help="Escolher um k FIXO para o horizonte usando agregação do Ranker")
    p_plan.add_argument("--tenant_id", required=True)
    p_plan.add_argument("--forecast_csv", required=True, help="CSV com demanda prevista para vários dias; precisa da coluna envio_data")
    p_plan.add_argument("--min_k", type=int, default=2)
    p_plan.add_argument("--max_k", type=int, default=12)
    p_plan.add_argument("--top_n", type=int, default=3)
    p_plan.add_argument("--output_agg", required=False, default="/app/exports/machine_learning/plan_rank_summary.csv")
    p_plan.add_argument("--save_daily", action="store_true", help="Se setado, salva diagnóstico de Top-3 por dia")
    p_plan.add_argument("--output_daily", required=False, default="/app/exports/machine_learning/plan_rank_daily.csv")
    p_plan.set_defaults(func=cmd_plan_fixed)

    args = p.parse_args()
    args.func(args)
