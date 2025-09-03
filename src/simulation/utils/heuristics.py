# utils/heuristics.py

def avaliar_parada_heuristica(lista_custos: list, tolerancia: float = 0.03) -> bool:
    """
    Para se o custo atual for maior que o anterior, ou se a redução for pequena.
    Avalia a partir do segundo valor.
    """
    if len(lista_custos) < 2:
        return False

    c_anterior = lista_custos[-2]
    c_atual = lista_custos[-1]

    if c_atual > c_anterior:
        return True

    variacao = abs(c_atual - c_anterior) / max(c_anterior, 1e-6)
    return variacao < tolerancia

