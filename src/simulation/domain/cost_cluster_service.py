#domain/cost_cluster_service.py

def calcular_cluster_cost(total_entregas: int, config: dict) -> float:
    if total_entregas <= config["limite_qtd_entregas"]:
        return config["custo_fixo_diario"]
    else:
        return total_entregas * config["custo_variavel_por_entrega"]

def calcular_custo_clusters_por_scenario(
    df_resumo_clusters,
    entregas_minimas_por_cluster: int,
    custo_minimo_cluster: float,
    custo_variavel_por_entrega: float,
    logger=None
) -> float:
    """
    Calcula o custo total dos clusters com base na regra:
    - Se o cluster tiver menos entregas que o mínimo, aplica custo fixo.
    - Se tiver igual ou mais, aplica custo variável por entrega.
    Retorna o custo total do cenário (soma dos custos dos clusters).
    """
    df = df_resumo_clusters.copy()
    
    # Garante que as colunas necessárias existem
    col_required = {'cluster', 'qde_ctes'}
    if not col_required.issubset(df.columns):
        raise ValueError(f"DataFrame resumo de clusters precisa conter as colunas {col_required}")

    # Aplica a regra de custo por cluster
    def calcular_custo(qde_ctes):
        if qde_ctes < entregas_minimas_por_cluster:
            return custo_minimo_cluster
        else:
            return qde_ctes * custo_variavel_por_entrega

    df['custo_cluster'] = df['qde_ctes'].apply(calcular_custo)
    custo_total = df['custo_cluster'].sum()

    if logger:
        logger.info(f"💰 Custo total dos clusters: R$ {custo_total:,.2f}")
    
    return round(custo_total, 2)
