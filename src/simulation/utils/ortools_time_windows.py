# hub_router_1.0.1/src/simulation/utils/ortools_time_windows.py

from simulation.domain.entities import SimulationParams
from ortools.constraint_solver import pywrapcp, routing_enums_pb2


def solve_time_windows_vrp(
    locations,
    special_flags,
    time_windows,
    service_times,
    params: SimulationParams,
    depot_location=None,
    num_vehicles=None,
    route_time_limit_min=None,
    delivery_debug_rows=None,
    retry_depth=0,
    max_retry_depth=1,
):
    """
    Retorna rotas como lista de listas de índices DAS ENTREGAS ORIGINAIS.
    Ex.: [[0, 5, 2], [1, 3, 4]]
    """

    import math

    max_special_per_route = params.max_especiais_por_rota
    route_time_limit_min = (
        int(route_time_limit_min)
        if route_time_limit_min is not None
        else int(params.tempo_max_roteirizacao)
    )
    permitir_rotas_excedentes = params.permitir_rotas_excedentes
    velocidade_kmh = params.velocidade_kmh
    entregas_por_rota = params.entregas_por_rota

    # ------------------------------------------------------------------
    # Validações básicas
    # ------------------------------------------------------------------
    if not locations:
        return []

    if depot_location is None:
        raise ValueError("depot_location é obrigatório no modo Time Windows.")

    if len(locations) != len(special_flags):
        raise ValueError("locations e special_flags devem ter o mesmo tamanho.")

    if len(locations) != len(time_windows):
        raise ValueError("locations e time_windows devem ter o mesmo tamanho.")

    if len(locations) != len(service_times):
        raise ValueError("locations e service_times devem ter o mesmo tamanho.")

    if max_special_per_route <= 0:
        raise ValueError("max_special_per_route deve ser maior que zero.")

    # ------------------------------------------------------------------
    # Função auxiliar: matriz de tempo via haversine
    # ------------------------------------------------------------------
    def compute_haversine_time_matrix(points, velocidade_kmh=45.0):
        n = len(points)
        matrix = [[0] * n for _ in range(n)]
        r_km = 6371.0

        for i in range(n):
            lat1, lon1 = points[i]

            for j in range(n):
                if i == j:
                    continue

                lat2, lon2 = points[j]

                dlat = math.radians(lat2 - lat1)
                dlon = math.radians(lon2 - lon1)

                a = (
                    math.sin(dlat / 2) ** 2
                    + math.cos(math.radians(lat1))
                    * math.cos(math.radians(lat2))
                    * math.sin(dlon / 2) ** 2
                )

                c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
                dist_km = r_km * c

                fator = getattr(params, "fator_correcao_distancia", 1.3)
                velocidade = max(float(params.velocidade_kmh or 45.0), 1)

                dist_real = dist_km * fator
                tempo_min = (dist_real / velocidade) * 60.0

                matrix[i][j] = max(1, int(round(tempo_min)))

        return matrix

    # ------------------------------------------------------------------
    # Monta nós com depot real na posição 0
    # ------------------------------------------------------------------
    all_locations = [depot_location] + list(locations)
    all_special_flags = [False] + list(special_flags)

    if permitir_rotas_excedentes:
        all_time_windows = [(0, 100000)] + [(0, 100000)] * len(locations)
    else:
        all_time_windows = [(0, int(route_time_limit_min))] + list(time_windows)

    all_service_times = [0] + [int(max(0, round(v))) for v in service_times]

    # ------------------------------------------------------------------
    # Matriz de tempo
    # ------------------------------------------------------------------
    time_matrix = compute_haversine_time_matrix(
        all_locations,
        velocidade_kmh=velocidade_kmh,
    )

    # ------------------------------------------------------------------
    # Quantidade de veículos (CONTROLADO PELO PIPELINE)
    # ------------------------------------------------------------------
    if num_vehicles is None:
        raise ValueError("num_vehicles deve ser definido antes de chamar o solver.")

    num_vehicles = int(num_vehicles)

    print(f"🚚 TW veículos={num_vehicles} (input)")

    # ------------------------------------------------------------------
    # Manager + Routing
    # ------------------------------------------------------------------
    manager = pywrapcp.RoutingIndexManager(
        len(time_matrix),
        num_vehicles,
        0,
    )
    routing = pywrapcp.RoutingModel(manager)

    # ------------------------------------------------------------------
    # Custo fixo por veículo
    # ------------------------------------------------------------------
    fixed_cost = getattr(params, "custo_fixo_veiculo", 100)

    for v in range(num_vehicles):
        routing.SetFixedCostOfVehicle(int(fixed_cost), v)

    # ------------------------------------------------------------------
    # Callback de custo base: tempo de deslocamento
    # ------------------------------------------------------------------
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return time_matrix[from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # ------------------------------------------------------------------
    # Restrição: máximo de especiais por rota
    # ------------------------------------------------------------------
    special_demands = [1 if flag else 0 for flag in all_special_flags]

    def special_demand_callback(from_index):
        node = manager.IndexToNode(from_index)
        return special_demands[node]

    special_callback_index = routing.RegisterUnaryTransitCallback(
        special_demand_callback
    )

    routing.AddDimensionWithVehicleCapacity(
        special_callback_index,
        0,
        [max_special_per_route] * num_vehicles,
        True,
        "Specials",
    )

    special_dimension = routing.GetDimensionOrDie("Specials")

    for vehicle_id in range(num_vehicles):
        end_index = routing.End(vehicle_id)
        special_dimension.CumulVar(end_index).SetMax(max_special_per_route)

    # ------------------------------------------------------------------
    # Time dimension = deslocamento + tempo de serviço
    # ------------------------------------------------------------------
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        travel_time = time_matrix[from_node][manager.IndexToNode(to_index)]
        service_time = all_service_times[from_node]
        return int(travel_time + service_time)

    time_callback_index = routing.RegisterTransitCallback(time_callback)

    routing.AddDimension(
        time_callback_index,
        30,
        int(route_time_limit_min * 3),
        False,
        "Time",
    )

    time_dimension = routing.GetDimensionOrDie("Time")

    for vehicle_id in range(num_vehicles):
        start_index = routing.Start(vehicle_id)
        end_index = routing.End(vehicle_id)

        if permitir_rotas_excedentes:
            time_dimension.CumulVar(start_index).SetRange(0, 100000)
            time_dimension.CumulVar(end_index).SetRange(0, 100000)
        else:
            time_dimension.CumulVar(start_index).SetRange(0, int(route_time_limit_min))
            time_dimension.CumulVar(end_index).SetRange(0, int(route_time_limit_min))

    for node_idx, tw in enumerate(all_time_windows):
        if tw is None:
            continue

        index = manager.NodeToIndex(node_idx)
        ini, fim = int(tw[0]), int(tw[1])

        if permitir_rotas_excedentes:
            time_dimension.CumulVar(index).SetRange(0, 100000)
        else:
            time_dimension.CumulVar(index).SetRange(ini, fim)

    # ------------------------------------------------------------------
    # Controle de drops
    # ------------------------------------------------------------------
    penalty_normal = int(route_time_limit_min * 200)
    penalty_special = 10_000_000

    for node in range(1, len(all_locations)):
        index = manager.NodeToIndex(node)
        is_special = all_special_flags[node]

        if permitir_rotas_excedentes:
            penalty = 10_000_000_000
        else:
            penalty = penalty_special if is_special else penalty_normal

        routing.AddDisjunction([index], penalty)

    # ------------------------------------------------------------------
    # Busca
    # ------------------------------------------------------------------
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    )
    search_parameters.time_limit.FromSeconds(10)

    print(f"[TW DEBUG] tempo_max={route_time_limit_min} | vehicles={num_vehicles}")
    print(f"[TW DEBUG] maior_service_time={max(service_times) if service_times else 0}")

    solution = routing.SolveWithParameters(search_parameters)

    if not solution:
        return []

    # ------------------------------------------------------------------
    # Debug: entregas não atendidas
    # ------------------------------------------------------------------
    dropped_nodes = []

    for node in range(routing.Size()):
        if routing.IsStart(node) or routing.IsEnd(node):
            continue

        if solution.Value(routing.NextVar(node)) == node:
            dropped_nodes.append(node)

    print(f"⚠️ DROPPED NODES: {len(dropped_nodes)}")

    # ------------------------------------------------------------------
    # Extrai rotas
    # ------------------------------------------------------------------
    routes = []
    atendidos = set()

    for vehicle_id in range(num_vehicles):
        index = routing.Start(vehicle_id)
        route = []

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)

            if node != 0:
                original_idx = node - 1
                route.append(original_idx)
                atendidos.add(original_idx)

            index = solution.Value(routing.NextVar(index))

        if route:
            routes.append(route)

    total_atendidas = len(atendidos)
    total_esperadas = len(locations)
    faltantes = set(range(total_esperadas)) - atendidos

    print(f"📊 TOTAL ESPERADO: {total_esperadas}")
    print(f"📊 TOTAL ATENDIDO: {total_atendidas}")
    print(f"📊 FALTANTES: {len(faltantes)}")

    if permitir_rotas_excedentes and faltantes:
        faltantes_ordenadas = sorted(faltantes)

        if retry_depth < max_retry_depth:
            num_vehicles_retry = min(
                len(locations),
                int(num_vehicles) + len(faltantes_ordenadas),
            )
            if num_vehicles_retry > int(num_vehicles):
                print(
                    "⚠️ TW retry: aumentando veículos de "
                    f"{num_vehicles} para {num_vehicles_retry} "
                    "antes do fallback"
                )
                return solve_time_windows_vrp(
                    locations=locations,
                    special_flags=special_flags,
                    time_windows=time_windows,
                    service_times=service_times,
                    params=params,
                    depot_location=depot_location,
                    num_vehicles=num_vehicles_retry,
                    route_time_limit_min=route_time_limit_min,
                    delivery_debug_rows=delivery_debug_rows,
                    retry_depth=retry_depth + 1,
                    max_retry_depth=max_retry_depth,
                )

        print(
            "⚠️ TW fallback: adicionando "
            f"{len(faltantes_ordenadas)} rotas unitárias "
            f"para entregas não roteirizadas {faltantes_ordenadas[:10]}"
        )
        if delivery_debug_rows:
            faltantes_detalhes = []
            for original_idx in faltantes_ordenadas:
                if 0 <= original_idx < len(delivery_debug_rows):
                    detalhe = dict(delivery_debug_rows[original_idx])
                    detalhe["solver_idx"] = original_idx
                    node_idx = original_idx + 1
                    tempo_unitario = (
                        time_matrix[0][node_idx]
                        + all_service_times[node_idx]
                        + time_matrix[node_idx][0]
                    )
                    detalhe["tempo_unitario_estimado_min"] = tempo_unitario
                    detalhe["excede_tempo_limite"] = (
                        tempo_unitario > int(route_time_limit_min)
                    )
                    faltantes_detalhes.append(detalhe)
            if faltantes_detalhes:
                print(f"🚨 TW DROPPED DETALHES: {faltantes_detalhes}")
        for original_idx in faltantes_ordenadas:
            routes.append([original_idx])

    return routes
