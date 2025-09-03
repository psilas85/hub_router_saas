#transfer_routing/domain/route_planning.py

from copy import deepcopy

from transfer_routing.infrastructure.vehicle_selector import obter_tipo_veiculo_por_peso

def calcular_distancia_e_tempo(p1, p2, obter_rota):
    distancia, tempo, _ = obter_rota((p1["lat"], p1["lon"]), (p2["lat"], p2["lon"]))
    return distancia or 0.0, tempo or 0.0


def gerar_rotas_transferencias(
    df_entregas,
    origem,
    tempo_maximo,
    tempo_parada_leve,
    tempo_parada_pesada,
    tempo_por_volume,
    peso_leve_max,
    obter_rota,
    conn,
    tenant_id,
    logger,
    hub_info
):
    logger.info("Gerando matriz de pontos...")
    agrupado = df_entregas.groupby(["centro_lat", "centro_lon"]).agg({
        "cte_numero": list,
        "cte_peso": "sum",
        "cte_volumes": "sum",
        "cte_valor_nf": "sum",
        "cte_valor_frete": "sum",
        "cluster": "first"
    }).reset_index()

    pontos = []
    for _, row in agrupado.iterrows():
        pontos.append({
            "lat": row["centro_lat"],
            "lon": row["centro_lon"],
            "cte_numeros": row["cte_numero"],
            "peso": row["cte_peso"],
            "volumes": row["cte_volumes"],
            "valor_nf": row["cte_valor_nf"],
            "valor_frete": row["cte_valor_frete"],
            "cluster_id": row["cluster"]
        })

    if not pontos:
        logger.warning("Nenhum ponto para roteirização.")
        return [], []

    logger.info(f"Total de pontos para roteirização: {len(pontos)}")

    # Matriz de distâncias e tempos
    logger.info("Calculando matriz de distâncias...")
    matriz_dist = {}
    for i, p1 in enumerate(pontos):
        for j, p2 in enumerate(pontos):
            if i != j:
                d, t = calcular_distancia_e_tempo(p1, p2, obter_rota)
                matriz_dist[(i, j)] = (d, t)

    rotas = [[i] for i in range(len(pontos))]

    # Calculando savings
    logger.info("Calculando savings...")
    savings = []
    for i in range(len(pontos)):
        for j in range(i + 1, len(pontos)):
            d_i0, t_i0 = calcular_distancia_e_tempo(pontos[i], {"lat": origem[0], "lon": origem[1]}, obter_rota)
            d_j0, t_j0 = calcular_distancia_e_tempo(pontos[j], {"lat": origem[0], "lon": origem[1]}, obter_rota)
            d_ij, t_ij = matriz_dist.get((i, j), (0.0, 0.0))
            saving = d_i0 + d_j0 - d_ij
            savings.append((saving, i, j))

    savings.sort(reverse=True)

    logger.info("Iniciando processo de junção de rotas com base nos savings...")
    for _, i, j in savings:
        rota_i = next((r for r in rotas if r[-1] == i), None)
        rota_j = next((r for r in rotas if r[0] == j), None)

        if rota_i and rota_j and rota_i != rota_j:
            nova_rota = rota_i + rota_j

            # Cálculo de tempo parcial sem volta
            tempo_transito = 0.0
            anterior = origem
            for idx in nova_rota:
                atual = (pontos[idx]["lat"], pontos[idx]["lon"])
                _, tempo = calcular_distancia_e_tempo({"lat": anterior[0], "lon": anterior[1]},
                                                       {"lat": atual[0], "lon": atual[1]}, obter_rota)
                tempo_transito += tempo
                anterior = atual

            # Tempo da volta
            _, tempo_volta = calcular_distancia_e_tempo({"lat": anterior[0], "lon": anterior[1]},
                                                         {"lat": origem[0], "lon": origem[1]}, obter_rota)

            # Tempo de parada
            tempo_parada = sum(
                tempo_parada_pesada if pontos[idx]["peso"] > peso_leve_max else tempo_parada_leve
                for idx in nova_rota
            )

            # Tempo de descarregamento
            total_volumes = sum(pontos[idx]["volumes"] for idx in nova_rota)
            tempo_descarga = total_volumes * tempo_por_volume

            # Tempo total com volta
            tempo_total = tempo_transito + tempo_parada + tempo_descarga + tempo_volta

            if tempo_total <= tempo_maximo:
                rotas.remove(rota_i)
                rotas.remove(rota_j)
                rotas.append(nova_rota)

    logger.info("Montando resumos e detalhes das rotas...")

    rotas_final = []
    detalhes = []

    for idx_rota, rota in enumerate(rotas, start=1):
        rota_id = str(1000 + idx_rota)

        ctes = []
        peso_total = 0.0
        volumes_total = 0
        valor_nf_total = 0.0
        valor_frete_total = 0.0
        quantidade_entregas = 0

        rota_coords = []
        anterior = origem

        for idx_ponto in rota:
            ponto = pontos[idx_ponto]
            ctes.extend(ponto["cte_numeros"])
            peso_total += ponto["peso"]
            volumes_total += ponto["volumes"]
            valor_nf_total += ponto["valor_nf"]
            valor_frete_total += ponto["valor_frete"]
            quantidade_entregas += len(ponto["cte_numeros"])

            rota_coords.append({"lat": ponto["lat"], "lon": ponto["lon"]})

            for cte in ponto["cte_numeros"]:
                detalhes.append({
                    "cte_numero": cte,
                    "cluster": ponto["cluster_id"],
                    "rota_id": rota_id,
                    "hub_central_nome": hub_info["nome"],
                    "cte_peso": ponto["peso"],
                    "cte_valor_nf": ponto["valor_nf"],
                    "cte_valor_frete": ponto["valor_frete"],
                    "centro_lat": ponto["lat"],
                    "centro_lon": ponto["lon"],
                    "cte_volumes": ponto["volumes"]
                })

        # Cálculo de distância e tempo
        distancia_ida = 0.0
        tempo_transito_ida = 0.0

        anterior = origem
        for idx_ponto in rota:
            atual = (pontos[idx_ponto]["lat"], pontos[idx_ponto]["lon"])
            dist, tempo = calcular_distancia_e_tempo({"lat": anterior[0], "lon": anterior[1]},
                                                      {"lat": atual[0], "lon": atual[1]}, obter_rota)
            distancia_ida += dist
            tempo_transito_ida += tempo
            anterior = atual

        dist_volta, tempo_volta = calcular_distancia_e_tempo({"lat": anterior[0], "lon": anterior[1]},
                                                              {"lat": origem[0], "lon": origem[1]}, obter_rota)

        distancia_total = distancia_ida + dist_volta
        tempo_transito_total = tempo_transito_ida + tempo_volta

        # Tempos operacionais
        tempo_parada = sum(
            tempo_parada_pesada if pontos[idx]["peso"] > peso_leve_max else tempo_parada_leve
            for idx in rota
        )
        tempo_descarga = volumes_total * tempo_por_volume

        tempo_ida = tempo_transito_ida + tempo_parada + tempo_descarga
        tempo_total = tempo_ida + tempo_volta
        
        # Obter tipo de veículo com base no peso total da rota
        veiculo_info = obter_tipo_veiculo_por_peso(peso_total, tenant_id, conn)
        tipo_veiculo = veiculo_info["tipo_veiculo"] if veiculo_info else "Desconhecido"
        
        
        rotas_final.append({
            "rota_id": rota_id,
            "quantidade_entregas": quantidade_entregas,
            "cte_peso": round(peso_total, 2),
            "cte_valor_nf": round(valor_nf_total, 2),
            "cte_valor_frete": round(valor_frete_total, 2),
            "clusters_qde": len(rota),
            "rota_coord": rota_coords,
            "hub_central_nome": hub_info["nome"],
            "hub_central_latitude": hub_info["latitude"],
            "hub_central_longitude": hub_info["longitude"],
            "distancia_ida_km": round(distancia_ida, 2),
            "distancia_total_km": round(distancia_total, 2),
            "tempo_ida_min": round(tempo_ida, 2),
            "tempo_total_min": round(tempo_total, 2),
            "tempo_transito_ida": round(tempo_transito_ida, 2),
            "tempo_transito_total": round(tempo_transito_total, 2),
            "tempo_paradas": round(tempo_parada, 2),
            "tempo_descarga": round(tempo_descarga, 2),
            "tipo_veiculo": tipo_veiculo,  # ✅ corrigido dinamicamente
            "volumes_total": int(volumes_total),
            "peso_total_kg": round(peso_total, 2)
        })



    logger.info("Rotas geradas com sucesso.")
    return rotas_final, detalhes
