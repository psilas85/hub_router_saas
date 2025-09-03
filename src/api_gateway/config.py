#api_gateway/config.py

from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    JWT_SECRET_KEY: str
    JWT_ALGORITHM: str
    AUTH_URL: str
    DATA_INPUT_URL: str
    CLUSTERIZATION_URL: str
    LAST_MILE_URL: str
    TRANSFER_ROUTING_URL: str
    COSTS_LAST_MILE_URL: str
    COSTS_TRANSFER_URL: str
    SIMULATION_URL: str   # 👈 sem valor default aqui
    ML_URL: str
    EXPLORATORY_ANALYSIS_URL: str


    class Config:
        env_file = "api_gateway/.env"

settings = Settings()

