#authentication/api/main.py

from fastapi import FastAPI
from authentication.api.routes import router

app = FastAPI(
    title="HubRouter - Authentication Service",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/auth/openapi.json",   # ✅ corrigido para manter prefixo /auth
    swagger_ui_parameters={"persistAuthorization": True}
)

# Rotas do módulo de autenticação
app.include_router(router, prefix="/auth", tags=["Auth"])  # ✅ prefixo explícito

@app.get("/health", tags=["Health"])
def healthcheck():
    """Verifica se o serviço de autenticação está ativo."""
    return {"status": "ok", "service": "authentication"}
