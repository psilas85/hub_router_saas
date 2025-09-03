# simulation/main_plot_simulation.py

import argparse
from datetime import datetime
from simulation.visualization.plot_simulation_transfer import plotar_mapa_simulacao as plotar_transfer
from simulation.visualization.plot_simulation_cluster import plotar_mapa_clusterizacao_simulation as plotar_cluster

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera mapa da simulação (transfer ou clusterização)")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data", required=True, help="Data de envio (YYYY-MM-DD)")
    parser.add_argument("--k", type=int, required=True, help="Número de clusters")
    parser.add_argument("--tipo", choices=["cluster", "transfer"], required=True, help="Tipo de mapa")

    args = parser.parse_args()
    envio_data = datetime.strptime(args.data, "%Y-%m-%d").date()

    if args.tipo == "transfer":
        plotar_transfer(args.tenant, envio_data, args.k)
    elif args.tipo == "cluster":
        plotar_cluster(args.tenant, envio_data, args.k)
