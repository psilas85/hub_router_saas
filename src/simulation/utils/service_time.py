#hub_router_1.0.1/src/simulation/utils/service_time.py

import pandas as pd


def calcular_tempo_servico(row, params, peso_referencia=None):
    """
    Calcula tempo de serviço por entrega.
    Ordem de prioridade:
    1. cte_tempo_atendimento_min (se existir)
    2. cálculo baseado em peso de referência da rota + volume
    """

    tempo_atendimento = row.get("cte_tempo_atendimento_min")

    # 🔥 PRIORIDADE 1: tempo explícito
    if tempo_atendimento is not None:
        try:
            if not pd.isna(tempo_atendimento):
                return max(float(tempo_atendimento), 0.0)
        except Exception:
            pass

    # 🔥 FALLBACK: cálculo padrão
    peso_base = peso_referencia
    if peso_base is None:
        peso_base = float(row.get("cte_peso", 0.0) or 0.0)

    volumes = float(row.get("cte_volumes", 0) or 0)

    tempo_parada = (
        params.tempo_parada_pesada
        if float(peso_base) > params.limite_peso_parada
        else params.tempo_parada_leve
    )

    tempo_descarga = volumes * params.tempo_por_volume

    return float(tempo_parada + tempo_descarga)