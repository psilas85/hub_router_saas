#hub_router_1.0.1/src/machine_learning/demand/domain/entities.py

from dataclasses import dataclass
from datetime import date
from typing import Optional

@dataclass
class DemandRecord:
    dt: date
    cidade: str
    uf: str
    entregas: int
    volumes_total: float
    peso_total: float

@dataclass
class DemandForecast:
    dt: date
    cidade: str
    uf: str
    pred_entregas: float
    pred_volumes: Optional[float] = None
    pred_peso: Optional[float] = None
    lower_ci: Optional[float] = None
    upper_ci: Optional[float] = None
