# hub_router_1.0.1/src/machine_learning/k_cluster/main_k_cluster_reg.py
from __future__ import annotations
import argparse
import json
import pandas as pd

from .infrastructure.repositories import PgResultsRepository
from .infrastructure.model_store import LocalKModelStore
from .utils.feature_builder import FeatureBuilder
from .models.k_cluster_regressor import KClusterRegressor


def cmd_train(args):
    fb = FeatureBuilder()
    model = KClusterRegressor(fb)

    repo = PgResultsRepository()
    resultados = repo.load_resultados_simulacao(args.tenant_id, args.start, args.end)
    resumo = repo.load_resumo_clusters(args.tenant_id, args.start, args.end)
    entregas = repo.load_entregas_clusterizadas(args.tenant_id, args.start, args.end)

    metrics = model.fit(resultados, resumo, entregas, test_size=args.test_size)

    print(json.dumps({"detail": "Treino concluído (regressor)", "metrics": metrics}, indent=2, ensure_ascii=False))

    # salva o modelo (igual ao classificador)
    payload = {"model": model.model, "feature_cols": fb._feature_cols_, "cfg": fb.cfg}
    path = LocalKModelStore().save(args.tenant_id, payload)
    print(f"\n📁 Modelo salvo em {path}")


def cmd_predict(args):
    fb = FeatureBuilder()
    model = KClusterRegressor(fb)

    # carrega modelo salvo
    payload = LocalKModelStore().load(args.tenant_id)
    model.model = payload["model"]
    fb._feature_cols_ = payload.get("feature_cols", None)
    fb.cfg = payload.get("cfg", fb.cfg)

    df = pd.read_csv(args.demand_csv)
    out = model.predict_k(
        demand_forecast=df,
        tenant_id=args.tenant_id,
        envio_data=args.envio_data,
        candidate_ks=list(range(args.min_k, args.max_k + 1)),
        top_n=args.top_n,
    )

    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    p = argparse.ArgumentParser("k_cluster_reg")
    sub = p.add_subparsers(required=True)

    p_train = sub.add_parser("train", help="Treinar modelo de regressão de custo (k ótimo via argmin)")
    p_train.add_argument("--tenant_id", required=True)
    p_train.add_argument("--start", required=False)
    p_train.add_argument("--end", required=False)
    p_train.add_argument("--test_size", type=float, default=0.2)
    p_train.set_defaults(func=cmd_train)

    p_pred = sub.add_parser("predict", help="Prever k ótimo via regressão de custo (CSV)")
    p_pred.add_argument("--tenant_id", required=True)
    p_pred.add_argument("--envio_data", required=True)
    p_pred.add_argument("--demand_csv", required=True)
    p_pred.add_argument("--min_k", type=int, default=2)
    p_pred.add_argument("--max_k", type=int, default=12)
    p_pred.add_argument("--top_n", type=int, default=3)
    p_pred.set_defaults(func=cmd_predict)

    args = p.parse_args()
    args.func(args)
