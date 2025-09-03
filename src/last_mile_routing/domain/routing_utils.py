#last_mile_routing/domain/routing_utils.py

def alocar_veiculo(peso_total, custos_veiculos):
    for _, row in custos_veiculos.iterrows():
        if row['peso_minimo_kg'] <= peso_total <= row['peso_maximo_kg']:
            return row['veiculo']
    return "Indefinido"


def calcular_tempo_total(
    distancia_km,
    volumes,
    qtde_paradas,
    tempo_parada,
    tempo_descarga_por_volume,
    velocidade_media_kmh=40
):
    tempo_transito = (distancia_km / velocidade_media_kmh) * 60  # em minutos
    tempo_paradas = qtde_paradas * tempo_parada
    tempo_descarga = volumes * tempo_descarga_por_volume

    tempo_total = tempo_transito + tempo_paradas + tempo_descarga
    return tempo_total, tempo_transito
