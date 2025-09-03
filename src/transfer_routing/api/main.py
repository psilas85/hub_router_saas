# transfer_routing/api/main.py

from fastapi import FastAPI
from transfer_routing.api.routes import router as transfer_router

app = FastAPI(
    title="HubRouter API - Transfer Routing",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},
)

# 🚀 inclui router sem prefix extra
app.include_router(transfer_router)

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Transfer Routing"}
