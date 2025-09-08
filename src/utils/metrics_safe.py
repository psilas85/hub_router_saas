# utils/metrics_safe.py
import numpy as np
from typing import Optional, Tuple, Dict, Any
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    roc_auc_score,
    precision_recall_curve,
    balanced_accuracy_score,
    average_precision_score,
)

def safe_f1(y_true, y_pred) -> float:
    return float(f1_score(y_true, y_pred, zero_division=0))

def safe_auc(y_true, y_score) -> Optional[float]:
    y_true = np.asarray(y_true)
    if len(np.unique(y_true)) < 2:
        return None
    try:
        return float(roc_auc_score(y_true, y_score))
    except ValueError:
        return None

def stratified_boot_auc_halfwidth(y_true, y_score, n: int = 1000, seed: int = 42) -> Optional[float]:
    """Half-width 95% (≈1.96*sd) via bootstrap estratificado. Retorna None se não der p/ calcular."""
    y_true = np.asarray(y_true)
    y_score = np.asarray(y_score)
    pos = np.where(y_true == 1)[0]
    neg = np.where(y_true == 0)[0]
    if len(pos) == 0 or len(neg) == 0:
        return None
    rng = np.random.default_rng(seed)
    aucs = []
    for _ in range(n):
        bi = np.concatenate([
            rng.choice(pos, size=len(pos), replace=True),
            rng.choice(neg, size=len(neg), replace=True)
        ])
        yy = y_true[bi]
        ss = y_score[bi]
        if len(np.unique(yy)) < 2:
            continue
        try:
            aucs.append(roc_auc_score(yy, ss))
        except ValueError:
            continue
    if not aucs:
        return None
    return float(1.96 * np.std(aucs, ddof=1))

def best_threshold_for_f1(y_true, y_score) -> float:
    """Seleciona limiar que maximiza F1 na validação."""
    ps, rs, ts = precision_recall_curve(y_true, y_score)
    # precision_recall_curve retorna ts com len-1
    f1s = (2 * ps * rs) / (ps + rs + 1e-12)
    if len(ts) == 0:
        return 0.5
    idx = int(np.nanargmax(f1s[:-1]))  # evita último sem threshold
    return float(ts[idx])

def compute_classif_metrics(y_true, y_prob, threshold: Optional[float] = None) -> Dict[str, Any]:
    """Calcula métricas robustas p/ desbalanceamento + intervalos seguros."""
    y_true = np.asarray(y_true)
    y_prob = np.asarray(y_prob)

    if threshold is None:
        threshold = best_threshold_for_f1(y_true, y_prob)
    y_pred = (y_prob >= threshold).astype(int)

    metrics = {
        "threshold": float(threshold),
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "f1": safe_f1(y_true, y_pred),
        "average_precision": float(average_precision_score(y_true, y_prob)),
    }

    auc = safe_auc(y_true, y_prob)
    metrics["roc_auc"] = auc

    # Intervalos (exemplo: só para AUC; outros podem ser adicionados depois)
    hw = stratified_boot_auc_halfwidth(y_true, y_prob, n=1000, seed=42) if auc is not None else None
    metrics["roc_auc_interval"] = (f"± {hw:.4f}" if hw is not None else None)

    # Placeholder interval F1 (opcionalmente, faça bootstrap de F1 também)
    metrics["f1_interval"] = "± 0.0000"

    return metrics
