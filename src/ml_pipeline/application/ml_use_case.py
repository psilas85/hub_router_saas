# ml_pipeline/application/ml_use_case.py

import pandas as pd
from ml_pipeline.infrastructure.dataset_repository import DatasetRepository


class DatasetUseCase:
    def __init__(self, repository: DatasetRepository):
        """
        Injeta o repositório de datasets (infraestrutura).
        """
        self.repository = repository

    def get_simulation_data(self, start_date: str, end_date: str, tenant_id: str) -> pd.DataFrame:
        """
        Carrega o dataset de simulações em um intervalo de datas e tenant específico.
        Retorna DataFrame Pandas.
        """
        df = self.repository.load_simulation_dataset(
            start_date=start_date,
            end_date=end_date,
            tenant_id=tenant_id
        )

        if df.empty:
            raise ValueError(
                f"Nenhum dado encontrado entre {start_date} e {end_date} para tenant {tenant_id}"
            )

        return df
