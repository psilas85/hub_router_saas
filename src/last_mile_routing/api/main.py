#last_mile_routing/api/main.py

from fastapi import FastAPI
from last_mile_routing.api.routes import router

app = FastAPI(
    title="HubRouter API - Last Mile Routing",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},
)

# 🚀 inclui router com prefixo já definido no routes.py
app.include_router(router)

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Last Mile Routing"}
