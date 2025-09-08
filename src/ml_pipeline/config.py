#config.py

import os

# Algoritmos permitidos por target
ALLOWED = {
    "custo_total": ["linear", "random_forest"],
    "is_ponto_otimo": ["logistic", "random_forest"],
}

def get_default_algorithm(target: str) -> str:
    """
    Retorna algoritmo default por target,
    podendo ser sobrescrito via variáveis de ambiente (.env).
    """
    if target == "custo_total":
        return os.getenv("ML_ALGO_COST", "linear")
    elif target == "is_ponto_otimo":
        return os.getenv("ML_ALGO_OPTIMO", "logistic")
    return "linear"  # fallback genérico

def validate_algorithm(target: str, algo: str | None) -> str:
    """
    Normaliza/valida algoritmo informado.
    Se None, pega do default (.env).
    """
    algo = (algo or "").strip() or get_default_algorithm(target)
    allowed = ALLOWED.get(target, [])
    if allowed and algo not in allowed:
        raise ValueError(
            f"Algoritmo '{algo}' inválido para target '{target}'. "
            f"Opções válidas: {allowed}"
        )
    return algo
