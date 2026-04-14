#hub_router_1.0.1/src/simulation/utils/ortools_time_windows.py

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
):
    """
    Retorna rotas como lista de listas de índices DAS ENTREGAS ORIGINAIS.
    Ex.: [[0, 5, 2], [1, 3, 4]]
    """

    import math

    max_special_per_route = params.max_especiais_por_rota
    route_time_limit_min = params.tempo_max_roteirizacao
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

                # 🔥 ALINHADO COM O SISTEMA
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
    # Quantidade de veículos
    # ------------------------------------------------------------------
    total_entregas = len(locations)
    total_especiais = sum(1 for x in special_flags if x)

    min_vehicles_by_special = (
        max(1, math.ceil(total_especiais / max_special_per_route))
        if total_especiais > 0 else 1
    )

    vehicles_by_volume = max(1, math.ceil(total_entregas / entregas_por_rota))

    # 🔥 BASE MÍNIMA
    vehicles_base = max(min_vehicles_by_special, vehicles_by_volume)

    # 🔥 FLEXIBILIZAÇÃO (ESSA É A CHAVE)
    fator_flex = 1.3 if params.modo_simulacao == "balanceado" else 1.6
    vehicles_flex = int(vehicles_base * fator_flex)

    # 🔥 LIMITES
    num_vehicles = max(vehicles_base, vehicles_flex)
    num_vehicles = min(num_vehicles, total_entregas)

    print(
        f"🚚 TW veículos={num_vehicles} | "
        f"min_especiais={min_vehicles_by_special} | "
        f"por_volume={vehicles_by_volume} | "
        f"base={vehicles_base}"
    )

    # ------------------------------------------------------------------
    # Manager + Routing
    # ------------------------------------------------------------------
    manager = pywrapcp.RoutingIndexManager(
        len(time_matrix),
        num_vehicles,
        0,  # depot
    )
    routing = pywrapcp.RoutingModel(manager)

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
        to_node = manager.IndexToNode(to_index)

        travel_time = time_matrix[from_node][to_node]
        service_time = all_service_times[from_node]

        return int(travel_time + service_time)

    time_callback_index = routing.RegisterTransitCallback(time_callback)

    routing.AddDimension(
        time_callback_index,
        30,
        int(route_time_limit_min),
        False,
        "Time",
    )

    time_dimension = routing.GetDimensionOrDie("Time")

    # Depot
    # Depot
    for vehicle_id in range(num_vehicles):
        start_index = routing.Start(vehicle_id)
        end_index = routing.End(vehicle_id)

        if permitir_rotas_excedentes:
            time_dimension.CumulVar(start_index).SetMin(0)
            time_dimension.SetCumulVarSoftUpperBound(
                end_index,
                int(route_time_limit_min),
                1000
            )
        else:
            time_dimension.CumulVar(start_index).SetRange(0, int(route_time_limit_min))
            time_dimension.CumulVar(end_index).SetRange(0, int(route_time_limit_min))

    # Janelas de tempo
    for node_idx, tw in enumerate(all_time_windows):
        if tw is None:
            continue

        index = manager.NodeToIndex(node_idx)
        ini, fim = int(tw[0]), int(tw[1])

        if fim < ini:
            raise ValueError(f"Janela inválida no nó {node_idx}: ({ini}, {fim})")

        if permitir_rotas_excedentes:
            # 🔥 não trava no tempo máximo
            time_dimension.CumulVar(index).SetMin(ini)
        else:
            time_dimension.CumulVar(index).SetRange(ini, fim)

    # ------------------------------------------------------------------
    # Penalidade para permitir descartar nós se necessário
    # Isso evita retorno vazio quando a solução perfeita não existe.
    # ------------------------------------------------------------------
    penalty = (
        params.penalty_excedente
        if permitir_rotas_excedentes
        else params.penalty_drop_node
    )

    for node in range(1, len(all_locations)):
        is_special = all_special_flags[node]

        if is_special:
            routing.AddDisjunction([manager.NodeToIndex(node)], 10_000_000)
        else:
            routing.AddDisjunction([manager.NodeToIndex(node)], penalty)

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


    print(f"[TW DEBUG] tempo_max={route_time_limit_min}")
    print(f"[TW DEBUG] maior_service_time={max(service_times)}")

    solution = routing.SolveWithParameters(search_parameters)

    if not solution:
        return []

    # --------------------------------------------------
    # 🔍 DEBUG: ENTREGAS NÃO ATENDIDAS
    # --------------------------------------------------
    dropped_nodes = []

    for node in range(routing.Size()):
        if routing.IsStart(node) or routing.IsEnd(node):
            continue

        if solution.Value(routing.NextVar(node)) == node:
            dropped_nodes.append(node)

    print(f"⚠️ DROPPED NODES: {len(dropped_nodes)}")

    # ------------------------------------------------------------------
    # Extrai rotas convertendo de volta para índices originais das entregas
    # OR-Tools: 0 = depot, 1..N = entregas
    # Saída final: 0..N-1 = índice original do df
    # ------------------------------------------------------------------
    routes = []

    for vehicle_id in range(num_vehicles):
        index = routing.Start(vehicle_id)
        route = []

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)

            if node != 0:
                route.append(node - 1)

            index = solution.Value(routing.NextVar(index))

        if route:
            routes.append(route)

    return routes