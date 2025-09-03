#exploratory_analysis/api/main.py

from fastapi import FastAPI
from exploratory_analysis.api.routes import router

app = FastAPI(
    title="HubRouter API - Exploratory Analysis",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    root_path="/exploratory"  # importante para funcionar no Nginx
)

app.include_router(router)

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Análise Exploratória"}
