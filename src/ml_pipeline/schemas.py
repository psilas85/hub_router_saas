# ml_pipeline/schemas.py
from pydantic import BaseModel, Field, constr
from typing import Optional, List, Union

# ----------------------
# SCHEMAS DE ENTRADA
# ----------------------

class TrainRequest(BaseModel):
    dataset_name: constr(min_length=1)
    target_column: constr(min_length=1)
    start_date: Optional[str] = None   # formato "YYYY-MM-DD"
    end_date: Optional[str] = None     # formato "YYYY-MM-DD"
    algorithm: Optional[str] = Field(
        default=None,
        description="Algoritmo de ML a ser utilizado (default = LinearRegression)"
    )


class FeaturesInput(BaseModel):
    """Schema para as features passadas na predição"""
    k_clusters: float = Field(..., description="Número de clusters")
    total_entregas: float = Field(..., description="Quantidade total de entregas")
    custo_transfer_total: float = Field(..., description="Custo total de transferência")
    custo_last_mile: float = Field(..., description="Custo total do last-mile")
    total_peso: float = Field(..., description="Peso total das entregas")
    total_volumes: float = Field(..., description="Total de volumes")
    valor_total_nf: float = Field(..., description="Valor total das notas fiscais")
    qtd_clusters: Optional[int] = None
    total_distancia_lastmile_km: Optional[float] = None
    total_tempo_lastmile_min: Optional[float] = None
    peso_lastmile: Optional[float] = None
    entregas_lastmile: Optional[int] = None
    volumes_lastmile: Optional[int] = None
    is_ponto_otimo: Optional[int] = None


class PredictRequest(BaseModel):
    dataset_name: constr(min_length=1)
    target_column: constr(min_length=1)
    # ✅ pode ser um objeto único ou uma lista
    features: Union[FeaturesInput, List[FeaturesInput]]
    algorithm: Optional[str] = Field(
        default=None,
        description="Algoritmo de ML a ser utilizado (default = modelo salvo)"
    )


# ----------------------
# SCHEMAS DE SAÍDA
# ----------------------

class PredictionResponse(BaseModel):
    prediction: float
    probability: Optional[float] = None  # apenas para classificadores
    algorithm: Optional[str] = None


class BatchPredictionResponse(BaseModel):
    predictions: List[PredictionResponse]
