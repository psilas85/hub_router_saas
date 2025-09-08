# utils/json_sanitize.py
import math
from typing import Any

def clean_for_json(obj: Any):
    """Converte floats inválidos (NaN/Inf) para None, recursivamente."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean_for_json(v) for v in obj]
    return obj
