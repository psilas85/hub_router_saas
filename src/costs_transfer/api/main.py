#costs_transfer/api/main.py

from fastapi import FastAPI
from costs_transfer.api.routes import router as custos_router
from costs_transfer.api.routes_tarifas import router as tarifas_router

app = FastAPI(
    title="HubRouter API - Custos Transferência",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},
    root_path="/costs_transfer",
)

# ✅ inclui AMBOS os routers
app.include_router(custos_router, prefix="")
app.include_router(tarifas_router, prefix="")

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Custos Transferência"}



