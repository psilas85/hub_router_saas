#data_input/api/main.py

from fastapi import FastAPI
from data_input.api.routes import router

app = FastAPI(
    title="HubRouter API - Data Input",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},
    root_path="/data_input"  # ✅ mantém root_path para gateway
)

# Router sem prefixo (já ajustado)
app.include_router(router)

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Data Input"}
