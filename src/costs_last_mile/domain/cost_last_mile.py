#costs_last_mile/domain/cost_last_mile.py

class Cost:
    def __init__(self, data_envio, cluster, sub_cluster, quantidade_entregas, peso_total_kg,
                 distancia_total_km, cte_frete_total, veiculo, custo_entrega_total, percentual_custo):
        self.data_envio = data_envio
        self.cluster = cluster
        self.sub_cluster = sub_cluster
        self.quantidade_entregas = quantidade_entregas
        self.peso_total_kg = peso_total_kg
        self.distancia_total_km = distancia_total_km
        self.cte_frete_total = cte_frete_total
        self.veiculo = veiculo
        self.custo_entrega_total = custo_entrega_total
        self.percentual_custo = percentual_custo
