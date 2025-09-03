#clusterization/api/main.py

from fastapi import FastAPI
from clusterization.api.routes import router

app = FastAPI(
    title="Clusterization Service",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True}
    # ❌ root_path removido
)

app.include_router(router)

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Clusterization"}
