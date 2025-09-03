# api_gateway/main.py

from fastapi import FastAPI
from api_gateway.routers import (
    authentication_router,
    health,
    data_input_router,
    routes_clusterization,
    transfer_routing_routes,
    last_mile_routes,
    simulation_routes,
    ml_routes,
    costs_transfer_routes,
    costs_last_mile_routes,
    exploratory_analysis_routes
)
from fastapi.staticfiles import StaticFiles
import os
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="API Gateway",
    description="Gateway central do HubRouter",
    version="1.0.0"
)

# Routers
app.include_router(health.router, prefix="/health")
app.include_router(authentication_router.router)
app.include_router(data_input_router.router)
app.include_router(routes_clusterization.router)
app.include_router(transfer_routing_routes.router)
app.include_router(last_mile_routes.router)
app.include_router(simulation_routes.router)
app.include_router(ml_routes.router)
app.include_router(costs_transfer_routes.router)
app.include_router(costs_last_mile_routes.router)
app.include_router(exploratory_analysis_routes.router)

# 🔓 CORS liberado para todos (somente para testes!)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # depois restringir para ["http://44.210.12.52"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔹 garante que a pasta exista dentro do container
os.makedirs("exports", exist_ok=True)

# 🔹 rota estática para servir relatórios/exportações
app.mount(
    "/exports",
    StaticFiles(directory="exports"),
    name="exports"
)
