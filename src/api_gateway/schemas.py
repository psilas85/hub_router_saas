#api_gateway/schemas.py

from pydantic import BaseModel
from typing import List, Optional, Union

# ========================
# Autenticação
# ========================
class UserLogin(BaseModel):
    username: str
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ========================
# ML Pipeline
# ========================
class TrainRequest(BaseModel):
    dataset_name: str
    target_column: str
    start_date: str
    end_date: str
    algorithm: Optional[str] = None   # 👈 agora é opcional

class Features(BaseModel):
    k_clusters: int
    total_entregas: int
    custo_transfer_total: float
    custo_last_mile: float
    total_peso: float
    total_volumes: int
    valor_total_nf: float
    qtd_clusters: Optional[int] = None
    total_distancia_lastmile_km: Optional[float] = None
    total_tempo_lastmile_min: Optional[float] = None
    peso_lastmile: Optional[float] = None
    entregas_lastmile: Optional[int] = None
    volumes_lastmile: Optional[int] = None
    is_ponto_otimo: Optional[int] = None


class PredictRequest(BaseModel):
    dataset_name: str
    target_column: str
    features: Union[Features, List[Features]]
    algorithm: Optional[str] = None


class PredictResponse(BaseModel):
    prediction: List[float]
    model_used: str
