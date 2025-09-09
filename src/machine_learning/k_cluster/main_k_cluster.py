# hub_router_1.0.1/src/machine_learning/k_cluster/main_k_cluster.py
from __future__ import annotations
import argparse
import json
import pandas as pd

from .infrastructure.repositories import PgResultsRepository
from .infrastructure.model_store import LocalKModelStore
from .application.k_cluster_use_case import KClusterUseCase


def _pretty_metrics(metrics: dict):
    print("\n📊 Métricas do treinamento")
    print("-" * 50)
    print(f"Accuracy           : {metrics.get('accuracy'):.3f}")
    print(f"Balanced Accuracy  : {metrics.get('balanced_accuracy'):.3f}")
    print(f"F1 Score           : {metrics.get('f1'):.3f}")
    print(f"PR-AUC (AvgPrec)   : {metrics.get('pr_ap'):.3f}")
    print("-" * 50)
    print(f"Regret@1 (mean)    : {metrics.get('regret_mean'):.2f}")
    print(f"Regret@1 (p50)     : {metrics.get('regret_p50'):.2f}")
    print(f"Regret@1 (p90)     : {metrics.get('regret_p90'):.2f}")
    print(f"Hit@3              : {metrics.get('hit_at_3'):.3f}")
    print("-" * 50)
    print(f"Train samples      : {metrics.get('n_train')}")
    print(f"Test samples       : {metrics.get('n_test')}")
    print(f"N Features         : {metrics.get('n_features')}")
    print("-" * 50)


def cmd_train(args):
    uc = KClusterUseCase(PgResultsRepository(), LocalKModelStore())
    out = uc.train(
        tenant_id=args.tenant_id,
        start=args.start,
        end=args.end,
        test_size=args.test_size,
    )
    # imprime JSON completo (para log/consumo automático)
    print(json.dumps(out, indent=2, ensure_ascii=False))

    # imprime também em formato legível no terminal
    if "metrics" in out:
        _pretty_metrics(out["metrics"])


def cmd_predict(args):
    uc = KClusterUseCase(PgResultsRepository(), LocalKModelStore())
    df = pd.read_csv(args.demand_csv)
    out = uc.predict(
        tenant_id=args.tenant_id,
        envio_data=args.envio_data,
        demand_forecast=df,
        candidate_ks=list(range(args.min_k, args.max_k + 1)),
        top_n=args.top_n,
    )
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    p = argparse.ArgumentParser("k_cluster")
    sub = p.add_subparsers(required=True)

    p_train = sub.add_parser("train", help="Treinar modelo de k ótimo")
    p_train.add_argument("--tenant_id", required=True)
    p_train.add_argument("--start", required=False)
    p_train.add_argument("--end", required=False)
    p_train.add_argument("--test_size", type=float, default=0.2)
    p_train.set_defaults(func=cmd_train)

    p_pred = sub.add_parser("predict", help="Prever k a partir de uma demanda prevista (CSV)")
    p_pred.add_argument("--tenant_id", required=True)
    p_pred.add_argument("--envio_data", required=True)
    p_pred.add_argument(
        "--demand_csv",
        required=True,
        help="CSV com colunas: cidade, quantidade_entregas, cte_peso, cte_volumes",
    )
    p_pred.add_argument("--min_k", type=int, default=2)
    p_pred.add_argument("--max_k", type=int, default=12)
    p_pred.add_argument("--top_n", type=int, default=3)
    p_pred.set_defaults(func=cmd_predict)

    args = p.parse_args()
    args.func(args)
