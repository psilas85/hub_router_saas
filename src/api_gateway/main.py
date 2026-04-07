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
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
import os


# ============================================================
# 🚫 Middleware para limitar tamanho de upload (10 MB)
# ============================================================
MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10 MB

class LimitUploadSizeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > MAX_UPLOAD_SIZE:
            return JSONResponse(
                {"detail": "Arquivo excede o limite máximo de 10MB."},
                status_code=413,
            )
        return await call_next(request)


# ============================================================
# 🚀 Inicialização do app
# ============================================================
app = FastAPI(
    title="API Gateway",
    description="Gateway central do HubRouter",
    version="1.0.0"
)

# 🔹 Middleware de limite antes do CORS
app.add_middleware(LimitUploadSizeMiddleware)

# 🔓 CORS liberado (ajustar depois para domínios específicos)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # depois restringir, ex: ["https://hubrouter.com.br"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# 📦 Routers
# ============================================================
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

# ============================================================
# 📂 Exportações
# ============================================================
os.makedirs("exports", exist_ok=True)
app.mount(
    "/exports",
    StaticFiles(directory="exports"),
    name="exports"
)
