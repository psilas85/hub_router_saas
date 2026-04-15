#hub_router_1.0.1/src/simulation/utils/route_helpers.py

import pandas as pd
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
import itertools
from typing import List, Dict, Tuple
from collections import defaultdict
from itertools import combinations
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import numpy as np

from simulation.utils.helpers import calcular_tempo_entrega_last_mile
from simulation.infrastructure.cache_routes import obter_rota_last_mile
from simulation.infrastructure.cache_routes import obter_rota_real
from simulation.infrastructure.simulation_database_reader import definir_tipo_veiculo_transferencia
from simulation.infrastructure.simulation_database_reader import definir_tipo_veiculo_last_mile
from simulation.domain.entities import SimulationParams

def calcular_distancia_euclidiana(lat1, lon1, lat2, lon2):
    return ((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2) ** 0.5

def obter_cidade_por_coordenada(lat, lon):
    """
    Utiliza o Nominatim para buscar o nome da cidade a partir de coordenadas.
    """
    try:
        geolocator = Nominatim(user_agent="cluster_router_rota")
        location = geolocator.reverse((lat, lon), exactly_one=True, timeout=10)
        if location and location.raw.get('address'):
            address = location.raw['address']
            cidade = address.get('city') or address.get('town') or address.get('village') or address.get('municipality')
            uf = address.get('state_code', '')
            if cidade:
                return f"{cidade} - {uf}"
    except Exception as e:
        print(f"⚠️ Erro ao buscar cidade por coordenada: {e}")
    return "Desconhecido"

def gerar_df_rota_transferencia(
    rota: list[tuple],
    hub_nome: str,
    tenant_id: str,
    simulation_db,
    envio_data,
    simulation_id,
    rota_id: str
) -> pd.DataFrame:
    dados = []

    for i in range(len(rota) - 1):
        origem = rota[i]
        destino = rota[i + 1]

        distancia_km, tempo_min = obter_rota_real(
            origem=origem,
            destino=destino,
            tenant_id=tenant_id,
            db_conn=simulation_db
        )

        cidade_origem = obter_cidade_por_coordenada(origem[0], origem[1])
        cidade_destino = obter_cidade_por_coordenada(destino[0], destino[1])

        dados.append({
            "simulation_id": simulation_id,
            "tenant_id": tenant_id,
            "envio_data": envio_data,
            "rota_id": rota_id,
            "sequencia": i + 1,
            "hub_nome": hub_nome,
            "origem_lat": origem[0],
            "origem_lon": origem[1],
            "destino_lat": destino[0],
            "destino_lon": destino[1],
            "cidade_origem": cidade_origem,
            "cidade_destino": cidade_destino,
            "distancia_km": round(distancia_km, 2),
            "tempo_min": round(tempo_min, 2),
            "tipo_origem": "hub" if i == 0 else "cluster",
            "tipo_destino": "hub" if i == len(rota) - 2 else "cluster",
            "is_retorno_hub": i == len(rota) - 2
        })

    return pd.DataFrame(dados)

def estimar_distancia_km(lat1, lon1, lat2, lon2):
    try:
        return geodesic((lat1, lon1), (lat2, lon2)).km
    except:
        return 20  # fallback

def dividir_rotas_transferencia_por_tempo_maximo(
    df_transferencias: pd.DataFrame,
    df_clusterizado: pd.DataFrame,
    params: SimulationParams,
    tempo_maximo_min: float,
) -> list[pd.DataFrame]:
    """
    Divide clusters em subgrupos com tempo estimado de rota menor que o tempo_maximo_min.
    Usa distância incremental entre clusters para estimativa de trânsito.
    """

    clusters = df_clusterizado.groupby('cluster').agg({
        'cluster_lat': 'first',
        'cluster_lon': 'first',
        'cte_peso': 'sum',
        'cte_volumes': 'sum'
    }).reset_index()

    clusters = clusters.sort_values(by='cluster_lat').reset_index(drop=True)

    grupos = []
    grupo_atual = []
    tempo_acumulado = 0.0
    ponto_anterior = None

    for _, row in clusters.iterrows():
        lat_atual = row['cluster_lat']
        lon_atual = row['cluster_lon']

        if ponto_anterior:
            dist_km = geodesic(ponto_anterior, (lat_atual, lon_atual)).km
        else:
            dist_km = 20.0

        tempo_transito = (dist_km / params.velocidade_kmh) * 60.0
        peso = row['cte_peso']
        volumes = row['cte_volumes']

        parada = (
            params.tempo_parada_pesada
            if peso > params.limite_peso_parada
            else params.tempo_parada_leve
        )

        descarga = volumes * params.tempo_por_volume
        tempo_cluster = tempo_transito + tempo_parada + tempo_descarga

        if tempo_acumulado + tempo_cluster > tempo_maximo_min and grupo_atual:
            grupos.append(grupo_atual)
            grupo_atual = []
            tempo_acumulado = 0.0
            ponto_anterior = None

        grupo_atual.append(row['cluster'])
        tempo_acumulado += tempo_cluster
        ponto_anterior = (lat_atual, lon_atual)

    if grupo_atual:
        grupos.append(grupo_atual)

    return [
        df_clusterizado[df_clusterizado['cluster'].isin(grupo)].copy()
        for grupo in grupos
    ]

def subdividir_rota_se_exceder_tempo(
    rota_principal: dict,
    hub: dict,
    params: SimulationParams,
    tempo_maximo: float,
    permitir_unico_destino_excedente: bool = True
) -> list[dict]:
    """
    Divide a rota em subrotas caso exceda tempo máximo permitido.
    Se tiver apenas um destino e a flag permitir_unico_destino_excedente=True,
    aceita a rota mesmo excedendo o tempo.
    """
    entregas = rota_principal["entregas"]
    destino = rota_principal["destino"]
    rota_id_base = rota_principal["rota_id_base"]

    # Calcular dados consolidados
    peso_total = sum(e["cte_peso"] for e in entregas)
    volumes_total = sum(e["cte_volumes"] for e in entregas)
    valor_total_nf = sum(e["cte_valor_nf"] for e in entregas)
    qde_entregas = len(entregas)
    qde_clusters_rota = 1  # na nova lógica, é sempre um destino único

    # Calcular distância e tempo real entre hub e destino
    origem = (hub["latitude"], hub["longitude"])
    destino_coord = (destino["lat"], destino["lon"])
    distancia_km, tempo_transito_min = obter_rota_real(origem, destino_coord, hub["tenant_id"], None)

    tempo_parada = params.tempo_parada_pesada if peso_total > params.limite_peso_parada else params.tempo_parada_leve
    tempo_descarga = volumes_total * params.tempo_por_volume
    tempo_total = tempo_transito_min + tempo_parada + tempo_descarga

    # Regra: se passar do tempo mas for só 1 destino, aceita
    if tempo_total > tempo_maximo and not permitir_unico_destino_excedente:
        return []

    return [{
        "tipo_veiculo": definir_tipo_veiculo_transferencia(peso_total),
        "distancia_km": distancia_km,
        "tempo_min": tempo_total,
        "peso_total": peso_total,
        "volumes_total": volumes_total,
        "valor_total_nf": valor_total_nf,
        "qde_entregas": qde_entregas,
        "qde_clusters_rota": qde_clusters_rota,
        "clusters_utilizados": [{
            "lat": destino["lat"],
            "lon": destino["lon"],
            "peso": peso_total,
            "cluster": None
        }]
    }]


def savings_algorithm(data: list, max_clusters_per_rota: int, max_peso: float, hub_lat: float, hub_lon: float) -> list:
    """
    Aplica o algoritmo de Savings (Clarke & Wright) com ordenação por ferradura.

    Parâmetros:
        - data: lista de dicionários com 'lat', 'lon', 'peso', 'cluster', etc.
        - max_clusters_per_rota: máximo de clusters por rota
        - max_peso: capacidade máxima de peso por rota
        - hub_lat, hub_lon: coordenadas do hub central real (não fictício)

    Retorna:
        - Lista de rotas com 'route_path' e clusters atribuídos
    """
    # Indexação dos clusters
    clusters = {i: d for i, d in enumerate(data)}
    hub_ficticio = (0.0, 0.0)  # usado só para savings

    for i in clusters:
        clusters[i]['id'] = i

    # Calcular savings
    savings = []
    for i, j in itertools.combinations(clusters.keys(), 2):
        ci, cj = clusters[i], clusters[j]
        dist_hub_i = geodesic(hub_ficticio, (ci["lat"], ci["lon"])).km
        dist_hub_j = geodesic(hub_ficticio, (cj["lat"], cj["lon"])).km
        dist_ij = geodesic((ci["lat"], ci["lon"]), (cj["lat"], cj["lon"])).km
        saving = dist_hub_i + dist_hub_j - dist_ij
        savings.append(((i, j), saving))

    savings.sort(key=lambda x: x[1], reverse=True)

    # Iniciar rotas individuais
    rotas = {i: [i] for i in clusters}

    for (i, j), _ in savings:
        if i not in rotas or j not in rotas or i == j:
            continue
        rota_i = rotas[i]
        rota_j = rotas[j]

        if rota_i is rota_j:
            continue

        peso_total = sum(clusters[c]["peso"] for c in (rota_i + rota_j))
        if len(rota_i + rota_j) <= max_clusters_per_rota and peso_total <= max_peso:
            nova_rota = rota_i + rota_j
            for c in nova_rota:
                rotas[c] = nova_rota

    # Eliminar duplicatas
    rotas_unicas = []
    rotas_vistas = set()
    for rota in rotas.values():
        tupla = tuple(sorted(rota))
        if tupla not in rotas_vistas:
            rotas_unicas.append(rota)
            rotas_vistas.add(tupla)

    # Montar estrutura de retorno com ordenação por ferradura
    resultado = []
    for rota in rotas_unicas:
        rota_path = [(clusters[c]["lat"], clusters[c]["lon"]) for c in rota]
        rota_ordenada = sequenciar_ferradura_pontos(
            lat_origem=hub_lat,
            lon_origem=hub_lon,
            lista_lat=[p[0] for p in rota_path],
            lista_lon=[p[1] for p in rota_path]
        )
        clusters_rota = [clusters[c] for c in rota]
        resultado.append({
            "route_path": rota_ordenada,
            "clusters": clusters_rota
        })

    return resultado

def calcular_tempo_total(distancia_km: float, volumes: int, tempo_parada_min: float, tempo_descarga_por_volume: float, velocidade_media_kmh: float) -> float:
    tempo_transito = (distancia_km / velocidade_media_kmh) * 60
    tempo_descarga = volumes * tempo_descarga_por_volume
    tempo_total = tempo_transito + tempo_parada_min + tempo_descarga
    return round(tempo_total, 2)

def sequenciar_ferradura_pontos(
    lat_origem: float,
    lon_origem: float,
    lista_lat: List[float],
    lista_lon: List[float]
) -> List[Tuple[float, float]]:

    """
    Sequencia os pontos (lat, lon) por proximidade ao ponto de origem
    com ida e volta simulando uma ferradura.
    """
    pontos = list(zip(lista_lat, lista_lon))
    pontos.sort(key=lambda p: geodesic((lat_origem, lon_origem), p).km)

    metade = len(pontos) // 2
    ida = pontos[:metade]
    volta = pontos[metade:]
    volta.reverse()

    return ida + volta

def sequenciar_ferradura(origem: tuple, destinos: list[tuple]) -> list[int]:
    """
    Recebe uma coordenada de origem (lat, lon) e uma lista de destinos [(lat1, lon1), ...].
    Retorna uma lista de índices representando a ordem de entrega (estratégia ferradura).
    """
    if not destinos:
        return []

    lat_origem, lon_origem = origem

    distancias = [
        (i, calcular_distancia_euclidiana(lat_origem, lon_origem, lat, lon))
        for i, (lat, lon) in enumerate(destinos)
    ]

    metade = len(distancias) // 2
    proximos = sorted(distancias, key=lambda x: x[1])[:metade]
    distantes = sorted(distancias, key=lambda x: x[1], reverse=True)[:len(distancias) - metade]

    ordenados = proximos + distantes
    ordem_indices = [i for i, _ in ordenados]

    return ordem_indices

# Função auxiliar a ser incluída no mesmo módulo (se ainda não estiver)
def calcular_tempo_e_distancia_rota(rota_completa, tenant_id, db_conn, logger):
    if len(rota_completa) < 2:
        logger.warning("⚠️ Rota incompleta: rota_completa contém menos de dois pontos. "
                       "Distância e tempo serão assumidos como zero.")
        return 0.0, 0.0

    tempo_total = 0
    distancia_total = 0
    for i in range(len(rota_completa) - 1):
        origem = rota_completa[i]
        destino = rota_completa[i + 1]
        dist, tempo = obter_rota_real(origem, destino, tenant_id, db_conn, logger)
        distancia_total += dist
        tempo_total += tempo

    return round(distancia_total, 2), round(tempo_total, 2)

def gerar_rotas_savings_transfer(
    pontos,
    origem,
    tempo_maximo,
    params: SimulationParams,
    obter_rota_func,
    logger
):
    logger.info("🔧 Iniciando cálculo de savings...")

    # Cálculo de savings
    savings = []
    for i, j in combinations(range(len(pontos)), 2):
        pi, pj = pontos[i], pontos[j]

        dij, _, _ = obter_rota_func((pi["lat"], pi["lon"]), (pj["lat"], pj["lon"]))
        dio, _, _ = obter_rota_func(origem, (pi["lat"], pi["lon"]))
        djo, _, _ = obter_rota_func(origem, (pj["lat"], pj["lon"]))

        # Verificação para evitar erro de NoneType
        if None in (dij, dio, djo):
            logger.warning(
                f"⚠️ Ignorando par ({i}, {j}) no cálculo de savings por coordenada inválida: dij={dij}, dio={dio}, djo={djo}"
            )
            continue

        saving = (dio + djo) - dij
        savings.append(((i, j), saving))

    savings.sort(key=lambda x: x[1], reverse=True)

    # Inicializar rotas individuais
    rotas = {i: [i] for i in range(len(pontos))}
    rotas_invertidas = {i: i for i in range(len(pontos))}

    for (i, j), _ in savings:
        ri = rotas_invertidas[i]
        rj = rotas_invertidas[j]

        if ri == rj:
            continue

        nova_rota = rotas[ri] + rotas[rj]

        # Calcular peso total
        # Calcular peso total
        peso_total = sum(pontos[pid]["peso"] for pid in nova_rota)
        if peso_total > params.peso_max_transferencia:
            logger.info(
                f"🚫 Rejeitada por peso excedente: {peso_total:.1f}kg > {params.peso_max_transferencia}kg"
            )
            continue
        # Calcular tempo total
        tempo_total = _calcular_tempo_total_rota(
            nova_rota,
            pontos,
            origem,
            obter_rota_func,
            params,
            logger
        )
        if tempo_total > tempo_maximo:
            logger.info(f"🚫 Rejeitada por tempo excedente: {tempo_total:.1f} min > {tempo_maximo} min")
            continue

        logger.info(f"💡 Junção aceita {ri} + {rj} → peso: {peso_total:.1f}kg, tempo: {tempo_total:.1f} min")

        # Fundir rotas
        rotas[ri] = nova_rota
        for pid in rotas[rj]:
            rotas_invertidas[pid] = ri
        del rotas[rj]

    # Construir rotas finais
    rotas_finais = []
    for rota_ids in set(tuple(v) for v in rotas.values()):
        rota = [pontos[pid] for pid in rota_ids]
        rotas_finais.append(rota)

    logger.info(f"✅ {len(rotas_finais)} rotas finais geradas com Savings.")
    return rotas_finais



def _calcular_tempo_total_rota(
    rotas_ids,
    pontos,
    origem,
    obter_rota_func,
    params: SimulationParams,
    logger
):
    # 🔒 Conversão segura de parâmetros
    tempo_parada_pesada = float(params.tempo_parada_pesada)
    tempo_parada_leve = float(params.tempo_parada_leve)
    tempo_por_volume = float(params.tempo_por_volume)
    limite_peso = float(params.limite_peso_parada)
    tempo_total = 0.0
    anterior = origem

    for pid in rotas_ids:
        ponto = pontos[pid]

        # ⚠️ Conversão segura dos dados do ponto
        peso = float(ponto.get("peso") or 0.0)
        volumes = int(ponto.get("volumes") or 0)
        atual = (ponto["lat"], ponto["lon"])

        # 🚗 Trânsito até o ponto
        dist, tempo_transito, _ = obter_rota_func(anterior, atual)
        tempo_transito = float(tempo_transito or 0.0)

        parada = tempo_parada_pesada if peso > 200 else tempo_parada_leve
        descarga = volumes * tempo_por_volume

        logger.debug(
            f"🧮 Rota {anterior} → {atual} | trânsito={tempo_transito:.2f} min, parada={parada}, descarga={descarga:.2f}"
        )

        tempo_total += tempo_transito + parada + descarga
        anterior = atual

    # 🔁 Volta ao ponto de origem
    dist_volta, tempo_volta, _ = obter_rota_func(anterior, origem)
    tempo_volta = float(tempo_volta or 0.0)
    tempo_total += tempo_volta

    logger.debug(f"↩️ Volta {anterior} → {origem} | trânsito={tempo_volta:.2f} min")

    peso_total_rota = sum(float(pontos[pid].get("peso") or 0.0) for pid in rotas_ids)
    logger.info(f"⏱️ Rota tentativa: {len(rotas_ids)} clusters | Peso total: {peso_total_rota:.1f}kg | Tempo total: {tempo_total:.1f} min")

    return round(tempo_total, 2)


def calcular_elbow_last_mile(coordenadas, k_min=1, k_max=10):
    """
    Retorna o melhor número de clusters (k) usando método do cotovelo ou silhueta.
    """
    if len(coordenadas) <= k_min:
        return k_min

    distortions = []
    k_values = list(range(k_min, min(k_max + 1, len(coordenadas))))

    for k in k_values:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(coordenadas)
        distortions.append(kmeans.inertia_)

    # Cotovelo: identifica o ponto com maior queda relativa
    deltas = np.diff(distortions)
    if len(deltas) == 0:
        return k_min
    melhor_k = k_values[np.argmin(deltas) + 1]  # +1 por causa do diff
    return max(melhor_k, k_min)

from geopy.distance import geodesic

def sequenciar_ferradura_last_mile(origem: tuple, destinos: list[tuple]) -> list[int]:
    """
    Retorna lista de índices dos destinos ordenados pela estratégia de ferradura:
    - Metade mais próxima
    - Metade mais distante (invertida)
    """
    if not destinos:
        return []

    distancias = [
        (i, geodesic(origem, ponto).km)
        for i, ponto in enumerate(destinos)
    ]

    distancias.sort(key=lambda x: x[1])

    metade = len(distancias) // 2
    proximos = [i for i, _ in distancias[:metade]]
    distantes = [i for i, _ in reversed(distancias[metade:])]

    return proximos + distantes


def ordenar_entregas_subcluster(df_subcluster):
    if df_subcluster.empty:
        return df_subcluster.copy()

    centro_lat = df_subcluster['centro_lat'].iloc[0]
    centro_lon = df_subcluster['centro_lon'].iloc[0]
    df_ordenado = df_subcluster.copy()
    df_ordenado['dist_to_centro'] = (
        (df_ordenado['destino_latitude'] - centro_lat) ** 2
        + (df_ordenado['destino_longitude'] - centro_lon) ** 2
    ) ** 0.5
    df_ordenado = df_ordenado.sort_values(by='dist_to_centro').reset_index(drop=True)
    df_ordenado['ordem_entrega'] = range(len(df_ordenado))
    return df_ordenado


def _dividir_dataframe_em_blocos(df_subcluster, n_partes, label_col):
    df_ordenado = ordenar_entregas_subcluster(df_subcluster)
    n_blocos = min(int(n_partes), len(df_ordenado))
    if n_blocos <= 1:
        df_ordenado[label_col] = 0
        return df_ordenado

    blocos_indices = np.array_split(df_ordenado.index.to_numpy(), n_blocos)
    for bloco_id, indices in enumerate(blocos_indices):
        df_ordenado.loc[indices, label_col] = bloco_id

    df_ordenado[label_col] = df_ordenado[label_col].astype(int)
    return df_ordenado


def estimar_viabilidade_subcluster(
    df_subcluster,
    df_tarifas,
    tempo_maximo,
    params: SimulationParams,
    logger,
    cluster_cidade=None,
    cidades_entregas=None,
):
    peso_total = df_subcluster['cte_peso'].sum()
    volumes_total = df_subcluster['cte_volumes'].sum()
    qtde_entregas = len(df_subcluster)
    centro_lat = df_subcluster['centro_lat'].iloc[0]
    centro_lon = df_subcluster['centro_lon'].iloc[0]

    cidades_referencia = cidades_entregas or df_subcluster['cte_cidade'].tolist()
    todas_na_mesma_cidade = all(
        str(cidade).strip().lower() == str(cluster_cidade).strip().lower()
        for cidade in cidades_referencia
    )

    tipo_veiculo, _ = definir_tipo_veiculo_last_mile(
        peso_total=peso_total,
        df_tarifas=df_tarifas,
        cluster_cidade=cluster_cidade,
        cidades_entregas=cidades_referencia,
        logger=logger,
    )

    if not getattr(params, "permitir_veiculo_leve_intermunicipal", False):
        if not todas_na_mesma_cidade:
            logger.info("🚫 Forçando veículo não leve por regra intermunicipal")
            limite_leve = getattr(params, "limite_peso_veiculo", 50.0)

            df_tarifas_filtrado = df_tarifas.copy()
            if "capacidade_kg_min" in df_tarifas_filtrado.columns:
                df_tarifas_filtrado = df_tarifas_filtrado[
                    df_tarifas_filtrado["capacidade_kg_min"] > limite_leve
                ]

            if not df_tarifas_filtrado.empty:
                tipo_veiculo, _ = definir_tipo_veiculo_last_mile(
                    peso_total=peso_total,
                    df_tarifas=df_tarifas_filtrado,
                    cluster_cidade=None,
                    cidades_entregas=cidades_referencia,
                    logger=logger,
                )

    tempo_servico_estimado = 0.0

    for _, row in df_subcluster.iterrows():
        tempo_atendimento = row.get("cte_tempo_atendimento_min")

        if tempo_atendimento is not None and pd.notna(tempo_atendimento):
            tempo_servico_estimado += float(tempo_atendimento)
        else:
            peso = float(row.get("cte_peso", 0))
            volumes = float(row.get("cte_volumes", 0))

            tempo_parada = (
                params.tempo_parada_pesada
                if peso > params.limite_peso_parada
                else params.tempo_parada_leve
            )

            tempo_descarga = volumes * params.tempo_por_volume
            tempo_servico_estimado += tempo_parada + tempo_descarga

    velocidade_media_kmh = params.velocidade_kmh
    tempo_transito_minimo = 0.0

    if velocidade_media_kmh > 0:
        distancias_ao_centro = [
            geodesic(
                (centro_lat, centro_lon),
                (row['destino_latitude'], row['destino_longitude'])
            ).km
            for _, row in df_subcluster.iterrows()
            if pd.notna(row['destino_latitude']) and pd.notna(row['destino_longitude'])
        ]
        if distancias_ao_centro:
            tempo_transito_minimo = (2 * max(distancias_ao_centro) / velocidade_media_kmh) * 60

    tempo_estimado = tempo_servico_estimado + tempo_transito_minimo
    viavel = tempo_estimado <= tempo_maximo

    return {
        "viavel": viavel,
        "tipo_veiculo": tipo_veiculo,
        "tempo_estimado": tempo_estimado,
        "tempo_servico_estimado": tempo_servico_estimado,
        "tempo_transito_minimo": tempo_transito_minimo,
    }

def dividir_subcluster_local(df_subcluster, n_partes, logger=None):
    coordenadas_df = df_subcluster[['destino_latitude', 'destino_longitude']].dropna()
    coordenadas = coordenadas_df.values
    if len(coordenadas) <= 1 or n_partes <= 1:
        return [("0", df_subcluster.copy())]

    n_clusters = min(int(n_partes), len(coordenadas))
    if n_clusters <= 1:
        return [("0", df_subcluster.copy())]

    if len(coordenadas_df.drop_duplicates()) < n_clusters:
        if logger:
            logger.info(
                f"🧱 Split local degenerado por coordenadas repetidas; aplicando divisão por blocos em {n_clusters} partes."
            )
        df_local = _dividir_dataframe_em_blocos(
            df_subcluster,
            n_clusters,
            'subcluster_local',
        )
        return [
            (str(sub_id), df_sub.copy())
            for sub_id, df_sub in df_local.groupby('subcluster_local')
        ]

    try:
        df_local = df_subcluster.copy()
        df_local['subcluster_local'] = KMeans(
            n_clusters=n_clusters,
            random_state=42,
            n_init="auto",
        ).fit_predict(coordenadas)
    except Exception as exc:
        if logger:
            logger.error(f"❌ Erro no split local com k={n_clusters}: {exc}")
        return []

    if df_local['subcluster_local'].nunique() < n_clusters:
        if logger:
            logger.info(
                f"🧱 KMeans local retornou apenas {df_local['subcluster_local'].nunique()} grupos; aplicando fallback por blocos em {n_clusters} partes."
            )
        df_local = _dividir_dataframe_em_blocos(
            df_subcluster,
            n_clusters,
            'subcluster_local',
        )

    return [
        (str(sub_id), df_sub.copy())
        for sub_id, df_sub in df_local.groupby('subcluster_local')
    ]

def subdividir_subcluster_por_veiculo(
    df_subcluster,
    df_tarifas,
    tempo_maximo,
    params: SimulationParams,
    tenant_id,
    simulation_db,
    logger,
    cluster_cidade=None,
    cidades_entregas=None
):
    subclusters_validos = []
    k_subveic = 1

    coordenadas = df_subcluster[['destino_latitude', 'destino_longitude']].dropna().values

    if len(coordenadas) <= 1:
        logger.warning("⚠️ Subcluster com uma ou nenhuma entrega. Aceitando como está.")
        df_subcluster = df_subcluster.copy()
        df_subcluster['ordem_entrega'] = 0
        tipo_veiculo, _ = definir_tipo_veiculo_last_mile(
            peso_total=df_subcluster["cte_peso"].sum(),
            df_tarifas=df_tarifas,
            cluster_cidade=cluster_cidade,
            cidades_entregas=cidades_entregas or df_subcluster['cte_cidade'].tolist(),
            logger=logger,
        )
        return [(df_subcluster, tipo_veiculo, 0.0, 0.0)]
    k_subveic = 1
    MAX_K_SUBVEIC = 6

    while k_subveic <= min(len(coordenadas), MAX_K_SUBVEIC):
        try:
            df_trabalho = df_subcluster.copy()

            if len(pd.DataFrame(coordenadas).drop_duplicates()) < k_subveic:
                df_trabalho = _dividir_dataframe_em_blocos(
                    df_trabalho,
                    k_subveic,
                    'subveic',
                )
            else:
                df_trabalho['subveic'] = KMeans(
                    n_clusters=k_subveic,
                    random_state=42,
                    n_init="auto",
                ).fit_predict(coordenadas)
        except Exception as e:
            logger.error(f"❌ Erro no KMeans com k={k_subveic}: {e}")
            break

        if df_trabalho['subveic'].nunique() < k_subveic:
            logger.info(
                f"🧱 KMeans por veículo retornou apenas {df_trabalho['subveic'].nunique()} grupos; aplicando fallback por blocos em {k_subveic} partes."
            )
            df_trabalho = _dividir_dataframe_em_blocos(
                df_trabalho,
                k_subveic,
                'subveic',
            )

        violou_restricao = False
        rotas_do_subcluster = []

        for _, df_sub in df_trabalho.groupby('subveic'):
            diagnostico = estimar_viabilidade_subcluster(
                df_subcluster=df_sub,
                df_tarifas=df_tarifas,
                tempo_maximo=tempo_maximo,
                params=params,
                logger=logger,
                cluster_cidade=cluster_cidade,
                cidades_entregas=df_sub["cte_cidade"].tolist(),
            )

            if not diagnostico['viavel']:
                violou_restricao = True
                logger.info(
                    f"⛔ Tempo estimado {diagnostico['tempo_estimado']:.2f} min excede limite ({tempo_maximo} min). "
                    f"servico={diagnostico['tempo_servico_estimado']:.2f} min, "
                    f"transito_min={diagnostico['tempo_transito_minimo']:.2f} min."
                )
                break

            df_sub = ordenar_entregas_subcluster(df_sub)
            rotas_do_subcluster.append(
                (df_sub, diagnostico['tipo_veiculo'], 0.0, diagnostico['tempo_estimado'])
            )

        if not violou_restricao:
            subclusters_validos = rotas_do_subcluster
            break

        k_subveic += 1
        logger.info(f"🔁 Subindo k_subveic para {k_subveic} devido à violação de tempo.")

    if not subclusters_validos:
        logger.error(
            f"❌ k_subveic={k_subveic} maior que número de pontos ({len(coordenadas)}). Interrompendo tentativa."
        )
        return []

    return subclusters_validos


def expandir_pontos_por_capacidade_veiculo(
    pontos: list[dict],
    db_conn,
    tenant_id: str,
    logger=None,
) -> list[dict]:
    """
    Divide pontos com peso maior que a capacidade máxima de veículos disponíveis.
    Retorna uma nova lista de pontos com divisões aplicadas.
    """
    # Carrega tabela de veículos
    df_veiculos = pd.read_sql(
        "SELECT * FROM veiculos_transferencia WHERE tenant_id = %s",
        db_conn,
        params=(tenant_id,),
    )
    capacidade_maxima = df_veiculos["capacidade_kg_max"].max()

    novos_pontos = []

    for ponto in pontos:
        peso = ponto["peso"]

        if peso <= capacidade_maxima:
            novos_pontos.append(ponto)
        else:
            # Determina quantas divisões são necessárias
            n = int((peso // capacidade_maxima) + 1)
            peso_unit = peso / n
            vol_unit = ponto["volumes"] / n
            nf_unit = ponto["valor_nf"] / n
            frete_unit = ponto["valor_frete"] / n
            cte_numeros = ponto["cte_numeros"]

            for i in range(n):
                novos_pontos.append({
                    "cluster_id": f"{ponto['cluster_id']}_p{i+1}",
                    "cte_numeros": cte_numeros,
                    "lat": ponto["lat"],
                    "lon": ponto["lon"],
                    "peso": peso_unit,
                    "volumes": vol_unit,
                    "valor_nf": nf_unit,
                    "valor_frete": frete_unit
                })

            if logger:
                logger.info(f"📦 Cluster {ponto['cluster_id']} expandido em {n} rotas por exceder {capacidade_maxima:.0f}kg")

    return novos_pontos
