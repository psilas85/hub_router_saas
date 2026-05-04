# exploratory_analysis/main.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from exploratory_analysis.api.routes import (
    concentracao,
    correlacao,
    distribuicao,
    geografico,
    qualidade,
    rankings,
    resumo,
    temporal,
)

app = FastAPI(
    title="HubRouter — Análise Exploratória",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    root_path="/exploratory",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["Healthcheck"])
@app.get("/", tags=["Healthcheck"])
def healthcheck():
    return {"status": "ok", "servico": "Análise Exploratória"}


app.include_router(resumo.router, tags=["EDA"])
app.include_router(qualidade.router, tags=["EDA"])
app.include_router(temporal.router, tags=["EDA"])
app.include_router(distribuicao.router, tags=["EDA"])
app.include_router(rankings.router, tags=["EDA"])
app.include_router(geografico.router, tags=["EDA"])
app.include_router(correlacao.router, tags=["EDA"])
app.include_router(concentracao.router, tags=["EDA"])
