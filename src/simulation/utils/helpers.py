#simulation/utils/helpers.py

import numpy as np
import pandas as pd
from sklearn.neighbors import KernelDensity
from geopy.geocoders import Nominatim
from geopy.distance import geodesic

from simulation.config import UF_BOUNDS
from simulation.infrastructure.cache_coordinates import salvar_localizacao_cache

from datetime import date
import pandas as pd

def calcular_tempo_entrega_last_mile(
    peso_total,
    volumes_total,
    qtde_entregas,
    tempo_parada_leve,
    tempo_parada_pesada,
    tempo_por_volume,
    limite_peso_parada
):

    peso_total = float(peso_total)
    volumes_total = int(volumes_total)
    qtde_entregas = int(qtde_entregas)
    tempo_parada_leve = float(tempo_parada_leve)
    tempo_parada_pesada = float(tempo_parada_pesada)
    tempo_por_volume = float(tempo_por_volume)

    tempo_parada = tempo_parada_pesada if peso_total > limite_peso_parada else tempo_parada_leve
    tempo_total = (tempo_parada * qtde_entregas) + (volumes_total * tempo_por_volume)

    return tempo_total



def solicitar_parametros_simulacao():
    print("\n🔧 Parâmetros da Simulação:")
    k_min = int(input("🔢 Valor mínimo de k_clusters: "))
    k_max = int(input("🔢 Valor máximo de k_clusters: "))
    tempo_parada_leve = int(input("⏱️ Tempo de parada por entrega até o limite de peso (min): "))
    tempo_parada_pesada = int(input("⏱️ Tempo de parada por entrega acima do limite de peso (min): "))
    tempo_por_volume = float(input("📦 Tempo de descarregamento por volume (min): "))
    velocidade_media_kmph = float(input("🚗 Velocidade média estimada (km/h): "))
    limite_peso_parada = float(input("⚖️ Limite de peso para classificar parada como leve (kg): "))

    return {
        "k_min": k_min,
        "k_max": k_max,
        "tempo_parada_leve": tempo_parada_leve,
        "tempo_parada_pesada": tempo_parada_pesada,
        "tempo_por_volume": tempo_por_volume,
        "velocidade_media_kmph": velocidade_media_kmph,
        "limite_peso_parada": limite_peso_parada
    }


def encontrar_centro_mais_denso(df_cluster):
    """
    Recebe um DataFrame filtrado com as entregas de um único cluster,
    retorna as coordenadas (lat, lon) do ponto com maior densidade,
    com pequeno deslocamento se coincidir com ponto de entrega.
    """
    if df_cluster.empty:
        return None, None

    coords = df_cluster[['latitude', 'longitude']].dropna().values

    if len(coords) == 0:
        return None, None

    kde = KernelDensity(kernel='gaussian', bandwidth=0.01).fit(coords)
    densidades = kde.score_samples(coords)
    idx_max = np.argmax(densidades)
    centro_lat, centro_lon = coords[idx_max][0], coords[idx_max][1]

    # 🚫 Evita colisão com entrega real
    entregas_set = set(map(tuple, coords))
    if (centro_lat, centro_lon) in entregas_set:
        centro_lat += np.random.uniform(0.0001, 0.0005)
        centro_lon += np.random.uniform(0.0001, 0.0005)

    return centro_lat, centro_lon



def ajustar_para_centro_urbano(lat, lon, db_conn, tenant_id, logger=None):
    """
    Ajusta um ponto denso para o centro urbano mais próximo usando reverse geocoding com cache.
    Se já existir no cache, usa o cache. Se falhar, retorna uma string padrão.
    """

    BRASIL_BOUNDS = {
        "min_lat": -33.7500,
        "max_lat": 5.3000,
        "min_lon": -73.9900,
        "max_lon": -34.7500
    }

    if not (BRASIL_BOUNDS["min_lat"] <= lat <= BRASIL_BOUNDS["max_lat"] and
            BRASIL_BOUNDS["min_lon"] <= lon <= BRASIL_BOUNDS["max_lon"]):
        return f"Coordenada fora do Brasil ({lat:.5f}, {lon:.5f})", "Fora da UF"

    # 🔍 Verificar cache
    cursor = db_conn.cursor()
    query = """
        SELECT endereco_completo, cidade
        FROM cache_localizacoes
        WHERE latitude = %s AND longitude = %s AND tenant_id = %s
    """
    cursor.execute(query, (lat, lon, tenant_id))
    row = cursor.fetchone()

    if row:
        if logger: logger.info(f"📍 Reverse cache HIT: {row[0]}")
        cursor.close()
        return row[0], row[1]

    cursor.close()
    if logger: logger.info(f"🔍 Reverse cache MISS: tentando Nominatim...")

    # 🔁 Tentar Nominatim reverse geocoding
    try:
        geolocator = Nominatim(user_agent="cluster_router_sim")
        location = geolocator.reverse((lat, lon), timeout=10, language="pt")
        if location and location.address:
            address = location.address
            cidade = (
                location.raw["address"].get("city")
                or location.raw["address"].get("town")
                or location.raw["address"].get("village")
                or "Desconhecido"
            )

            salvar_localizacao_cache(db_conn, address, lat, lon, "reverse_nominatim", tenant_id, cidade=cidade)
            return address, cidade
    except Exception as e:
        if logger: logger.warning(f"Nominatim falhou: {e}")

    # Falha total
    if logger: logger.warning(f"⚠️ Reverse geocoding falhou para ({lat:.5f}, {lon:.5f})")
    return f"Centro desconhecido ({lat:.5f}, {lon:.5f})", "Desconhecido"



def calcular_distancia_euclidiana(lat1, lon1, lat2, lon2):
    """
    Calcula a distância euclidiana aproximada entre dois pontos geográficos.
    Converte todos os valores para float para evitar erro com tipos Decimal.
    """
    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    dx = (lon2 - lon1) * 111  # longitude em km (aproximadamente)
    dy = (lat2 - lat1) * 111  # latitude em km (aproximadamente)
    return (dx**2 + dy**2) ** 0.5


def solicitar_datas():
    tenant_id = input("🔐 Informe o tenant_id: ").strip()

    data_inicial_str = input("📅 Data inicial (YYYY-MM-DD): ").strip()
    data_final_str = input("📅 Data final   (YYYY-MM-DD): ").strip()

    data_inicial = datetime.strptime(data_inicial_str, "%Y-%m-%d").date()
    data_final = datetime.strptime(data_final_str, "%Y-%m-%d").date()

    return tenant_id, data_inicial, data_final

import pandas as pd
from datetime import datetime

def gerar_resumo_clusters(df_clusterizado: pd.DataFrame, simulation_id: str) -> pd.DataFrame:
    """
    Gera o DataFrame resumo_clusters com base nas entregas clusterizadas.

    Agrupa por tenant_id, envio_data, k_clusters e cluster para totalizar peso, volumes, valor NF e qde_ctes.
    """
    colunas_agrupamento = ["tenant_id", "envio_data", "k_clusters", "cluster"]
    colunas_agregadas = {
        "cte_peso": "sum",
        "cte_volumes": "sum",
        "cte_valor_nf": "sum",
        "cte_numero": "count",
        "centro_lat": "first",
        "centro_lon": "first"
    }

    df_resumo = df_clusterizado.groupby(colunas_agrupamento).agg(colunas_agregadas).reset_index()
    df_resumo.rename(columns={
        "cte_peso": "peso_total_kg",
        "cte_volumes": "qde_volumes",  # ✅
        "cte_valor_nf": "valor_total_nf",
        "cte_numero": "qde_ctes"
    }, inplace=True)

    df_resumo["simulation_id"] = simulation_id
    df_resumo["created_at"] = datetime.utcnow()

    return df_resumo[[
        "tenant_id", "envio_data", "simulation_id", "k_clusters", "cluster",
        "centro_lat", "centro_lon",
        "peso_total_kg", "qde_volumes", "valor_total_nf", "qde_ctes", "created_at"  # ✅
    ]]

from collections import defaultdict
from simulation.domain.entities import TransferenciaResumo
from simulation.infrastructure.simulation_database_reader import obter_capacidade_veiculo

def consolidar_resumos_por_rota(resumos: list[TransferenciaResumo], simulation_db) -> list[TransferenciaResumo]:
    agrupado = defaultdict(list)

    for r in resumos:
        rota_base = r.rota_id.split("_v")[0]
        agrupado[rota_base].append(r)

    consolidados = []
    for rota_base, subrotas in agrupado.items():
        if len(subrotas) == 1:
            subrotas[0].rota_id = rota_base
            consolidados.append(subrotas[0])
        else:
            primeiro = subrotas[0]
            total_peso = sum(r.peso_total_kg for r in subrotas)
            qde_volumes = sum(r.qde_volumes for r in subrotas)  # ✅ aqui

            total_valor = sum(r.valor_total_nf for r in subrotas)
            total_entregas = sum(r.qde_entregas for r in subrotas)
            total_tempo = sum(r.tempo_total_min for r in subrotas)
            total_distancia = sum(r.distancia_total_km for r in subrotas)
            total_clusters = sum(r.qde_clusters_rota for r in subrotas)

            capacidade_kg = obter_capacidade_veiculo(primeiro.tipo_veiculo, simulation_db)
            aproveitamento = min((total_peso / capacidade_kg) * 100, 100.0) if capacidade_kg else 0

            consolidado = TransferenciaResumo(
                envio_data=primeiro.envio_data,
                simulation_id=primeiro.simulation_id,
                cluster_id=-1,
                hub_id=primeiro.hub_id,
                hub_nome=primeiro.hub_nome,
                hub_latitude=primeiro.hub_latitude,
                hub_longitude=primeiro.hub_longitude,
                destino_latitude=None,
                destino_longitude=None,
                tipo_veiculo=primeiro.tipo_veiculo,
                distancia_total_km=total_distancia,
                distancia_parcial_km=total_distancia,
                tempo_parcial_min=total_tempo,
                tempo_total_min=total_tempo,
                peso_total_kg=total_peso,
                qde_volumes=qde_volumes,  # ✅ corrigido
                valor_total_nf=total_valor,
                aproveitamento_percentual=aproveitamento,
                k_clusters=primeiro.k_clusters,
                is_ponto_otimo=primeiro.is_ponto_otimo,
                qde_clusters_rota=total_clusters,
                rota_id=rota_base,
                qde_entregas=total_entregas,
                clusters_utilizados=[
                    c for r in subrotas for c in r.clusters_utilizados
                ]
            )

            consolidados.append(consolidado)

    return consolidados

# helpers.py
def gerar_nome_arquivo_mapa(envio_data: date, k_clusters: int) -> str:
    data_str = envio_data.strftime("%Y-%m-%d")
    return f"simulacao_{data_str}_k{k_clusters}.html"

def log_coordenadas(logger, cluster_id, lat, lon, prefixo="Cluster"):
    if lat is not None and lon is not None:
        logger.info(f"✅ Coordenadas obtidas para {prefixo} {cluster_id}: lat={lat:.5f}, lon={lon:.5f}")
    else:
        logger.warning(f"⚠️ Coordenadas inválidas para {prefixo} {cluster_id}: (None, None)")

from reportlab.platypus import Image
from reportlab.lib.units import inch
import os

def carregar_imagem_redimensionada(path_img, largura_max=6.5 * inch, altura_max=9 * inch):
    """
    Carrega uma imagem ajustando seu tamanho máximo para caber em uma página A4.
    Ideal para imagens altas, como tabelas exportadas para PNG.
    """
    if not os.path.exists(path_img):
        return None

    try:
        img = Image(path_img)
        img._restrictSize(largura_max, altura_max)
        return img
    except Exception as e:
        return None
