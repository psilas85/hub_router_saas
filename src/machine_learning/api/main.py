# machine_learning/api/main.py
from fastapi import FastAPI
from machine_learning.api.routes import router

app = FastAPI(
    title="HubRouter API - Machine Learning",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},  # 🔑 mantém o JWT no Swagger
)

# inclui o router de previsão de demanda
app.include_router(router)

@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Machine Learning"}
