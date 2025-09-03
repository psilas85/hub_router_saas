#hub_router_1.0.1/src/ml_pipeline/api/main_ml_service.py

import argparse
import os
import uvicorn
from typing import Union
import pandas as pd
from pathlib import Path

from fastapi import FastAPI, HTTPException, Depends, APIRouter, Query
from ml_pipeline.ml_pipeline import MLPipeline
from ml_pipeline.schemas import (
    TrainRequest,
    PredictRequest,
    PredictionResponse,
    BatchPredictionResponse,
)

# 🔐 usa o mesmo dependency dos demais serviços
from authentication.utils.dependencies import obter_tenant_id_do_token

# ============================
# App principal
# ============================
app = FastAPI(
    title="ML Pipeline Service",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    swagger_ui_parameters={"persistAuthorization": True},
    root_path="/ml",  # ✅ agora todos endpoints ficam sob /ml
)

# Instância global do pipeline
pipeline = MLPipeline()

# ============================
# Endpoints diretos
# ============================

@app.post("/train")
def train(
    request: TrainRequest,
    tenant_id: str = Depends(obter_tenant_id_do_token),
    fast: bool = Query(False, description="Se true, ativa modo rápido (amostra até 500 registros)")
):
    try:
        model, metrics, algorithm = pipeline.train(
            tenant_id=tenant_id,
            dataset_name=request.dataset_name,
            target_column=request.target_column,
            start_date=request.start_date,
            end_date=request.end_date,
            algorithm=request.algorithm,
            fast=fast
        )
        return {
            "detail": "Treino concluído",
            "metrics": metrics,
            "algorithm": algorithm  # ✅ agora vem do retorno real
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no treino: {str(e)}")


@app.post("/train_compare")
def train_compare(
    request: TrainRequest,
    tenant_id: str = Depends(obter_tenant_id_do_token),
    fast: bool = Query(False, description="Se true, ativa modo rápido (amostra até 500 registros)")
):
    try:
        # Define algoritmos conforme o target
        if request.target_column == "custo_total":
            algos = ["linear", "random_forest"]
        elif request.target_column == "is_ponto_otimo":
            algos = ["logistic", "random_forest"]
        else:
            raise HTTPException(status_code=400, detail=f"Target '{request.target_column}' não suportado")

        # 🔹 nomes "marketeiros"
        algo_labels = {
            "linear": "Modelo Básico Linear",
            "random_forest": "Modelo Inteligente Floresta",
            "logistic": "Classificador Logístico",
        }

        results = []
        for algo in algos:
            model, metrics, algorithm_used = pipeline.train(
                tenant_id=tenant_id,
                dataset_name=request.dataset_name,
                target_column=request.target_column,
                start_date=request.start_date,
                end_date=request.end_date,
                algorithm=algo,
                fast=fast
            )

            results.append({
                "algorithm": algorithm_used,
                "algorithm_label": algo_labels.get(algorithm_used, algorithm_used),
                "metrics": metrics
            })

            pipeline.logger.info(f"📈 {algo_labels.get(algorithm_used, algorithm_used)} → {metrics}")

        # 🔹 Salva resultados em CSV
        export_dir = Path("/app/exports/ml_pipeline")
        export_dir.mkdir(parents=True, exist_ok=True)

        df_results = pd.DataFrame([{
            "algorithm": r["algorithm_label"],
            "start_date": request.start_date,
            "end_date": request.end_date,
            **r["metrics"]
        } for r in results])

        csv_path = export_dir / f"compare_{request.target_column}.csv"
        df_results.to_csv(csv_path, index=False)

        pipeline.logger.info(f"📊 Resultados comparativos salvos em {csv_path}")

        return {
            "detail": "Treino concluído",
            "target_column": request.target_column,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "fast": fast,
            "results": results,
            "csv": str(csv_path)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no treino comparativo: {str(e)}")



@app.post(
    "/predict",
    response_model=Union[PredictionResponse, BatchPredictionResponse],
)
def predict(
    request: PredictRequest,
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    try:
        # Lista de cenários
        if isinstance(request.features, list):
            predictions = []
            for feat in request.features:
                pred = pipeline.predict(
                    features=feat.dict(),
                    tenant_id=tenant_id,
                    dataset_name=request.dataset_name,
                    target_column=request.target_column,
                    algorithm=request.algorithm,  # 👈 apenas informativo
                )
                predictions.append(PredictionResponse(**pred))
            return BatchPredictionResponse(predictions=predictions)

        # Cenário único
        prediction = pipeline.predict(
            features=request.features.dict(),
            tenant_id=tenant_id,
            dataset_name=request.dataset_name,
            target_column=request.target_column,
            algorithm=request.algorithm,  # 👈 apenas informativo
        )
        return PredictionResponse(**prediction)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na previsão: {str(e)}")

# ============================
# Router de planejamento
# ============================

router = APIRouter(tags=["ML Planning"])  # ✅ sem prefix extra

@router.get("/plan", summary="Recomendação de estrutura (próximos meses)")
def plan(
    start_date: str = Query(...),
    months: int = Query(3),
    scenarios: str = Query("base,baixo,alto"),
    debug: bool = Query(False, description="Se true, exporta CSVs em /exports/ml_pipeline"),
    fast: bool = Query(False, description="Se true, ativa modo rápido (limita hubs/k/cenários)"),
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    pipe = MLPipeline()
    planos = pipe.plan_next_months(
        tenant_id,
        start_date,
        months,
        tuple(s.strip() for s in scenarios.split(",")),
        debug=debug,
        fast=fast  # 👈 novo parâmetro propagado
    )
    out = {k: v.to_dict(orient="records") for k, v in planos.items()}
    return out

# 👉 registra router no app
app.include_router(router)

# ============================
# Inicialização via argparse
# ============================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Serviço ML Pipeline")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host para servir API")
    parser.add_argument("--port", type=int, default=int(os.getenv("SERVICE_PORT", 8011)), help="Porta do serviço")
    parser.add_argument("--reload", action="store_true", help="Ativar reload automático (dev only)")
    args = parser.parse_args()

    uvicorn.run(
        "ml_pipeline.api.main_ml_service:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
