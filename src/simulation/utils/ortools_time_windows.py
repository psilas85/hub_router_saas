#hub_router_1.0.1/src/simulation/utils/ortools_time_windows.py

from ortools.constraint_solver import pywrapcp, routing_enums_pb2


def solve_time_windows_vrp(
    locations,              # [(lat, lon), ...] SOMENTE ENTREGAS
    special_flags,          # [True/False, ...] SOMENTE ENTREGAS
    time_windows,           # [(start, end), ...] SOMENTE ENTREGAS ou None
    service_times,          # [min, ...] SOMENTE ENTREGAS
    max_special_per_route=1,
    depot_location=None,    # (lat, lon) do hub/depot
    num_vehicles=None,
    route_time_limit_min=600,
):
    """
    Retorna rotas como lista de listas de índices DAS ENTREGAS ORIGINAIS.
    Ex.: [[0, 5, 2], [1, 3, 4]]
    """

    import math
    import numpy as np

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
    # Monta nós com depot real na posição 0
    # ------------------------------------------------------------------
    all_locations = [depot_location] + list(locations)
    all_special_flags = [False] + list(special_flags)
    all_time_windows = [None] + list(time_windows)
    all_service_times = [0] + [int(max(0, round(v))) for v in service_times]

    # ------------------------------------------------------------------
    # Matriz de distância simples
    # ------------------------------------------------------------------
    def compute_euclidean_distance_matrix(points):
        n = len(points)
        matrix = np.zeros((n, n))
        for i, (lat1, lon1) in enumerate(points):
            for j, (lat2, lon2) in enumerate(points):
                matrix[i][j] = ((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2) ** 0.5
        return matrix

    distance_matrix = compute_euclidean_distance_matrix(all_locations)

    total_entregas = len(locations)
    total_especiais = sum(1 for x in special_flags if x)

    min_vehicles_by_special = max(
        1,
        math.ceil(total_especiais / max_special_per_route)
    ) if total_especiais > 0 else 1

    heuristic_vehicles = max(1, math.ceil(total_entregas / 10))

    if num_vehicles is None:
        num_vehicles = max(min_vehicles_by_special, heuristic_vehicles)
    else:
        num_vehicles = max(int(num_vehicles), min_vehicles_by_special, 1)

    manager = pywrapcp.RoutingIndexManager(
        len(distance_matrix),
        num_vehicles,
        0,   # depot real
    )
    routing = pywrapcp.RoutingModel(manager)

    # ------------------------------------------------------------------
    # Custo/distância
    # ------------------------------------------------------------------
    def distance_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return int(distance_matrix[from_node][to_node] * 1000)

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # ------------------------------------------------------------------
    # Restrição: máximo de especiais por rota
    # ------------------------------------------------------------------
    special_demands = [1 if flag else 0 for flag in all_special_flags]

    def special_demand_callback(from_index):
        node = manager.IndexToNode(from_index)
        return special_demands[node]

    special_callback_index = routing.RegisterUnaryTransitCallback(special_demand_callback)

    routing.AddDimensionWithVehicleCapacity(
        special_callback_index,
        0,
        [max_special_per_route] * num_vehicles,
        True,
        "Specials",
    )

    special_dimension = routing.GetDimensionOrDie("Specials")

    # força hard no final da rota
    for vehicle_id in range(num_vehicles):
        end_index = routing.End(vehicle_id)
        special_dimension.CumulVar(end_index).SetMax(max_special_per_route)

    # ------------------------------------------------------------------
    # Time dimension = deslocamento + service time
    # ------------------------------------------------------------------
    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)

        travel_time = int(distance_matrix[from_node][to_node] * 60)
        service_time = int(all_service_times[from_node])

        return travel_time + service_time

    time_callback_index = routing.RegisterTransitCallback(time_callback)

    routing.AddDimension(
        time_callback_index,
        30,                         # slack
        int(route_time_limit_min),  # limite total da rota
        False,
        "Time",
    )

    time_dimension = routing.GetDimensionOrDie("Time")

    # Depot
    for vehicle_id in range(num_vehicles):
        start_index = routing.Start(vehicle_id)
        end_index = routing.End(vehicle_id)
        time_dimension.CumulVar(start_index).SetRange(0, int(route_time_limit_min))
        time_dimension.CumulVar(end_index).SetRange(0, int(route_time_limit_min))

    # Janelas, se existirem
    for node_idx, tw in enumerate(all_time_windows):
        if node_idx == 0:
            continue
        if tw is not None:
            index = manager.NodeToIndex(node_idx)
            ini, fim = int(tw[0]), int(tw[1])
            if fim < ini:
                raise ValueError(f"Janela inválida no nó {node_idx}: ({ini}, {fim})")
            time_dimension.CumulVar(index).SetRange(ini, fim)

    # ------------------------------------------------------------------
    # Busca
    # ------------------------------------------------------------------
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.FromSeconds(10)

    solution = routing.SolveWithParameters(search_parameters)

    if not solution:
        return []

    routes = []
    for vehicle_id in range(num_vehicles):
        index = routing.Start(vehicle_id)
        route = []

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)

            # ignora depot no retorno
            if node != 0:
                # converte de volta para índice original das entregas
                route.append(node - 1)

            index = solution.Value(routing.NextVar(index))

        if route:
            routes.append(route)

    return routes