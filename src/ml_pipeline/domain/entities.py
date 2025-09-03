#ml_pipeline/domain/entities.py

from dataclasses import dataclass
import pandas as pd

@dataclass
class SimulationDataset:
    df: pd.DataFrame

@dataclass
class MLModel:
    name: str
    model: object
    metrics: dict
