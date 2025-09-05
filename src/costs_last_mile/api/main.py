#costs_last_mile/api/main.py

from fastapi import FastAPI
from costs_last_mile.api.routes import router

app = FastAPI(
    title="HubRouter API - Custos Transferência",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},
)



app.include_router(router)

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Custos Last Mile"}
