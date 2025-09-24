# src/simulation/api/main_simulation_service.py

import uvicorn
from fastapi import FastAPI
from simulation.api import routes

app = FastAPI(
    title="HubRouter - Simulation Service",
    description="📊 Serviço responsável por executar simulações de clusterização, roteirização e custeio.",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},
    root_path=""  # ✅ removido '/simulation' para evitar duplicação
)

# Registrar rotas (todas já têm prefix="/simulation")
app.include_router(routes.router, tags=["Simulation"])


if __name__ == "__main__":
    uvicorn.run(
        "simulation.api.main_simulation_service:app",
        host="0.0.0.0",
        port=8006,   # ✅ porta interna padrão do container
        reload=False
    )
