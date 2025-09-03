# authentication/main.py

from fastapi import FastAPI, Depends
from dotenv import load_dotenv
from pathlib import Path

from authentication.utils.dependencies import obter_tenant_id_do_token

# Routers com tags padronizadas
from authentication.api.routes import router as auth_router
from data_input.api.routes import router as data_input_router
from clusterization.api.routes import router as cluster_router
from transfer_routing.api.routes import router as transfer_router
from last_mile_routing.api.routes import router as lastmile_router
from costs_transfer.api.routes import router as transfer_costs_router
from costs_last_mile.api.routes import router as lastmile_costs_router
from simulation.api.routes import router as simulation_router
from exploratory_analysis.api.routes import router as eda_router

# Carregar variáveis de ambiente do .env deste módulo
dotenv_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=dotenv_path, override=True)

# Configuração da aplicação FastAPI com persistência de autorização no Swagger
app = FastAPI(
    title="HubRouter API",
    version="1.0",
    docs_url="/swagger",
    redoc_url="/redoc",
    swagger_ui_parameters={"persistAuthorization": True}
)

# Registro das rotas
app.include_router(auth_router, tags=["Autenticação"])

# Rotas protegidas por autenticação
app.include_router(data_input_router, tags=["Importação de Dados"], dependencies=[Depends(obter_tenant_id_do_token)])
app.include_router(cluster_router, tags=["Clusterização"], dependencies=[Depends(obter_tenant_id_do_token)])
app.include_router(transfer_router, tags=["Roteirização Transferências"], dependencies=[Depends(obter_tenant_id_do_token)])
app.include_router(lastmile_router, tags=["Roteirização Last Mile"], dependencies=[Depends(obter_tenant_id_do_token)])
app.include_router(transfer_costs_router, tags=["Custos Transferências"], dependencies=[Depends(obter_tenant_id_do_token)])
app.include_router(lastmile_costs_router, tags=["Custos Last Mile"], dependencies=[Depends(obter_tenant_id_do_token)])
app.include_router(simulation_router, tags=["Simulação"], dependencies=[Depends(obter_tenant_id_do_token)])
app.include_router(eda_router, tags=["Análise Exploratória"], dependencies=[Depends(obter_tenant_id_do_token)])


@app.get("/")
def read_root():
    return {"status": "ok", "mensagem": "HubRouter API online"}
