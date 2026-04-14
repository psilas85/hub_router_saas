#hub_router_1.0.1/src/simulation/domain/strategy_validator.py

def validar_combinacao(cluster: str, routing: str):

    # ❌ TIME WINDOWS com KMEANS puro (ruim)
    if routing == "time_windows" and cluster == "kmeans":
        raise ValueError(
            "Time Windows requer clusterização balanceada (balanced_kmeans)."
        )

    # ❌ combinações futuras inválidas podem entrar aqui