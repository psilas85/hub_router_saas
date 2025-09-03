# hub_router_1.0.1/src/ml_pipeline/interface/costs_clients.py

import pandas as pd
import numpy as np

class CostsTransferClient:
    """
    Cliente simplificado para estimar custos de transferências (middle mile).
    """
    def __init__(self, geo=None):
        self.geo = geo

    def estimate(self, df: pd.DataFrame, tenant_id: str) -> float:
        """
        Recebe dataframe de cidades→hubs, devolve custo de transferência.
        Aqui simulamos um custo simplificado: peso_total * 0.05.
        """
        if df.empty:
            return 0.0

        peso_total = df["peso"].astype(float).sum()
        custo = peso_total * 0.05  # R$ por kg, só placeholder
        return custo


class CostsLastMileClient:
    """
    Cliente simplificado para estimar custos de last mile + gerar rotas.
    """
    def __init__(self):
        pass

    def estimate(self, df: pd.DataFrame, tenant_id: str) -> dict:
        """
        Recebe dataframe de cidades→hubs, devolve:
        - custo_total: custo do last mile
        - rotas_df: DataFrame com viagens (coluna 'peso_total_subrota')
        """
        if df.empty:
            return {"custo_total": 0.0, "rotas_df": pd.DataFrame(columns=["peso_total_subrota"])}

        rotas = []
        for _, row in df.iterrows():
            peso_total = float(row["peso"])
            entregas = int(row["entregas"])

            # Dividimos em subrotas de até 200kg (moto/fiorino etc.)
            capacidade_padrao = 200.0
            n_viagens = max(1, int(np.ceil(peso_total / capacidade_padrao)))

            # Cada viagem recebe uma fração do peso
            peso_por_viagem = peso_total / n_viagens
            for _ in range(n_viagens):
                rotas.append({"peso_total_subrota": peso_por_viagem})

        rotas_df = pd.DataFrame(rotas)

        # custo = nº de viagens * R$50 (placeholder simples)
        custo_total = len(rotas_df) * 50.0

        return {"custo_total": custo_total, "rotas_df": rotas_df}
