#hub_router_1.0.1/src/simulation/utils/geo_time.py

import math

def calcular_distancia_e_tempo(
    origem,
    destino,
    params,
    logger=None,
):
    if not origem or not destino:
        if logger:
            logger.warning("⚠️ Origem ou destino inválido no cálculo de distância.")
        return None, None, "erro_input"

    try:
        lat1, lon1 = float(origem[0]), float(origem[1])
        lat2, lon2 = float(destino[0]), float(destino[1])

        R = 6371

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        dist_km = R * c

        fator = getattr(params, "fator_correcao_distancia", 1.3)
        velocidade = max(float(params.velocidade_kmh or 45.0), 1)

        dist_real = dist_km * fator
        tempo_min = (dist_real / velocidade) * 60

        return dist_real, tempo_min, "haversine"

    except Exception as e:
        if logger:
            logger.warning(f"⚠️ Erro no haversine fallback: {e}")
        return None, None, "erro_execucao"