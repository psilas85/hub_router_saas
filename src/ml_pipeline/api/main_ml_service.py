# ml_pipeline/api/main_ml_service.py
import argparse
import os
import uvicorn
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Union
from joblib import dump


from fastapi import FastAPI, HTTPException, Depends, APIRouter, Query
from fastapi.responses import JSONResponse

from utils.json_sanitize import clean_for_json
from ml_pipeline.schemas import (
    TrainRequest,
    PredictRequest,
    PredictionResponse,
    BatchPredictionResponse,
    PlanResponse,   # ✅ já corrigido com RootModel
)

from ml_pipeline.ml_pipeline import MLPipeline
from ml_pipeline.models.trainer_factory import TrainerFactory
from ml_pipeline.planning.planning_use_case import PlanningUseCase
from ml_pipeline.interface.costs_clients import CostsTransferClient, CostsLastMileClient

# 🔐 usa o mesmo dependency dos demais serviços
from authentication.utils.dependencies import obter_tenant_id_do_token

import traceback
import logging

from ml_pipeline.infrastructure.dataset_repository import DatasetRepository

db_config = {
    "host": os.getenv("POSTGRES_HOST", "postgres_db"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "simulation_db"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

repo = DatasetRepository(db_config=db_config)


logger = logging.getLogger(__name__)

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
    root_path="/ml",
)

# Instância global do pipeline (reutilizada para cache)
pipeline = MLPipeline()

# ============================
# Endpoints diretos
# ============================

@app.post("/train_compare")
def train_compare(
    request: TrainRequest,
    tenant_id: str = Depends(obter_tenant_id_do_token),
    fast: bool = Query(False, description="Se true, ativa modo rápido (amostra até 500 registros)")
):
    try:
        # ===============================
        # Escolha de algoritmos por target
        # ===============================
        if request.target_column == "custo_total":
            algos = ["linear", "random_forest"]
        elif request.target_column == "is_ponto_otimo":
            algos = ["logistic", "random_forest"]
        elif request.target_column in ["custo_last_mile", "custo_transfer_total"]:
            algos = ["linear", "random_forest"]
        else:
            raise HTTPException(status_code=400, detail=f"Target '{request.target_column}' não suportado")

        algo_labels = {
            "linear": "Modelo Básico Linear",
            "random_forest": "Modelo Inteligente Floresta",
            "logistic": "Classificador Logístico",
        }

        # ===============================
        # Carregamento do dataset
        # ===============================
        df = pipeline.load_data(request.start_date, request.end_date, tenant_id)
        if fast and not df.empty:
            df = df.sample(n=min(500, len(df)), random_state=42)
        if df.empty:
            raise HTTPException(status_code=400, detail="Dataset vazio para o período selecionado")

        # ===============================
        # Deriva colunas, se necessário
        # ===============================
        if request.target_column == "custo_last_mile":
            # diferença entre total e transfer
            df["custo_last_mile"] = df["custo_total"] - df["custo_transfer_total"]
        elif request.target_column == "custo_transfer_total":
            # já existe, mas garantimos que está presente
            if "custo_transfer_total" not in df.columns:
                raise HTTPException(status_code=400, detail="Coluna custo_transfer_total ausente no dataset")

        # ===============================
        # Preprocessamento
        # ===============================
        X, y = pipeline.preprocess_data(df, request.target_column)

        # ===============================
        # Loop de algoritmos
        # ===============================
        results = []
        for algo in algos:
            trainer = TrainerFactory.get_trainer(request.target_column, algorithm=algo)
            model, (X_eval, y_eval) = trainer.train(X, y)
            metrics = trainer.evaluate(model, X_eval, y_eval)

            results.append({
                "algorithm": algo,
                "algorithm_label": algo_labels.get(algo, algo),
                "metrics": metrics
            })
            pipeline.logger.info(f"📈 {algo_labels.get(algo, algo)} → {metrics}")

        # ===============================
        # Export CSV
        # ===============================
        export_dir = Path("/app/exports/ml_pipeline")
        export_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = export_dir / f"compare_{request.target_column}_{ts}.csv"
        pd.DataFrame([
            {"algorithm": r["algorithm_label"], "start_date": request.start_date,
             "end_date": request.end_date, **r["metrics"]}
            for r in results
        ]).to_csv(csv_path, index=False)

        # ===============================
        # Payload de saída
        # ===============================
        payload = {
            "detail": "Treino comparativo concluído",
            "target_column": request.target_column,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "fast": fast,
            "results": results,
            "csv": str(csv_path)
        }
        return JSONResponse(content=clean_for_json(payload), status_code=200)

    except Exception as e:
        tb_str = traceback.format_exc()
        logger.error(f"❌ Erro no treino comparativo: {e}\n{tb_str}")
        raise HTTPException(status_code=500, detail=f"Erro no treino comparativo: {e}")


@app.post(
    "/predict",
    response_model=Union[PredictionResponse, BatchPredictionResponse],
)
def predict(
    request: PredictRequest,
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    try:
        if isinstance(request.features, list):
            predictions = []
            for feat in request.features:
                pred = pipeline.predict(
                    features=feat.model_dump(),
                    tenant_id=tenant_id,
                    dataset_name=request.dataset_name,
                    target_column=request.target_column,
                    algorithm=request.algorithm,
                )
                predictions.append(PredictionResponse(**pred))
            return BatchPredictionResponse(predictions=predictions)

        prediction = pipeline.predict(
            features=request.features.model_dump(),
            tenant_id=tenant_id,
            dataset_name=request.dataset_name,
            target_column=request.target_column,
            algorithm=request.algorithm,
        )
        return PredictionResponse(**prediction)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na previsão: {str(e)}")

# ============================
# Router de planejamento
# ============================

router = APIRouter(tags=["ML Planning"])

@app.get("/plan", summary="Recomendação de estrutura (próximos meses)", response_model=PlanResponse)
def plan(
    start_date: str = Query(...),
    months: int = Query(3),
    scenarios: str = "base,baixo,alto",
    debug: bool = False,
    fast: bool = False,
    tenant_id: str = Depends(obter_tenant_id_do_token),
):
    try:
        planos = pipeline.plan_next_months(
            tenant_id,
            start_date,
            months,
            tuple(s.strip() for s in scenarios.split(",")),
            debug=debug,
            fast=fast,
        )

        out = {}
        for k, v in planos.items():
            if isinstance(v, pd.DataFrame):
                out[k] = v.to_dict(orient="records")
            else:
                # já é lista/dict pronto
                out[k] = v

        return JSONResponse(content=clean_for_json(out), status_code=200)

    except Exception as e:
        logger.exception("❌ Erro no planejamento")
        raise HTTPException(status_code=500, detail=f"Erro no planejamento: {e}")

from ml_pipeline.planning.planning_use_case import PlanningUseCase
from ml_pipeline.interface.costs_clients import CostsTransferClient, CostsLastMileClient
from ml_pipeline.infrastructure.dataset_repository import DatasetRepository

# Inicializa dependências do planner (mesmo estilo do pipeline)
ct_client = CostsTransferClient(db_config=db_config)
lm_client = CostsLastMileClient(db_config=db_config)

geo_adapter = None
planner = PlanningUseCase(repo, pipeline, ct_client, lm_client, geo_adapter, logger)


@router.get("/plan_v2", summary="Recomendação de estrutura V2 (DemandForecaster)")
def plan_v2(start_date: str,
            months: int = 2,
            scenarios: str = "base,baixo,alto",
            debug: bool = False,
            fast: bool = True,
            tenant_id: str = Depends(obter_tenant_id_do_token)):   # 🔑 usa o mesmo do resto
    """
    Novo planejamento baseado no DemandForecaster (recommend_structure_v2).
    """
    scenarios_list = [s.strip() for s in scenarios.split(",") if s.strip()]

    planos = planner.recommend_structure_v2(
        tenant_id=tenant_id,
        start_date=start_date,
        months=months,
        scenarios=scenarios_list,
        debug=debug,
        fast=fast
    )

    return {
        "detail": "Planejamento v2 concluído",
        "tenant_id": tenant_id,
        "start_date": start_date,
        "months": months,
        "scenarios": scenarios_list,
        "results": planos
    }


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

@app.post("/train_and_save_models")
def train_and_save_models(
    start_date: str,
    end_date: str,
    tenant_id: str = Depends(obter_tenant_id_do_token),
    fast: bool = Query(False, description="Se true, usa amostra de até 500 registros")
):
    """
    Treina e salva modelos ML para os targets:
    - custo_total
    - custo_last_mile
    - custo_transfer_total
    Retorna métricas de cada modelo.
    """
    try:
        targets = ["custo_total", "custo_last_mile", "custo_transfer_total"]
        results = {}

        # carrega dados brutos
        df = pipeline.load_data(start_date, end_date, tenant_id)
        if fast and not df.empty:
            df = df.sample(n=min(500, len(df)), random_state=42)
        if df.empty:
            raise HTTPException(status_code=400, detail="Dataset vazio para o período selecionado")

        # gera colunas derivadas
        df["custo_last_mile"] = df["custo_total"] - df["custo_transfer_total"]

        for target in targets:
            if target not in df.columns:
                raise HTTPException(status_code=400, detail=f"Coluna {target} ausente no dataset")

            X, y = pipeline.preprocess_data(df, target)

            trainer = TrainerFactory.get_trainer(target, algorithm="random_forest")
            model, (X_eval, y_eval) = trainer.train(X, y)
            metrics = trainer.evaluate(model, X_eval, y_eval)

            # salva modelo
            TrainerFactory.save_trained(model, target, tenant_id)

            results[target] = {
                "metrics": metrics,
                "model_path": f"/app/exports/models/{tenant_id}/{target}.pkl"
            }

            pipeline.logger.info(f"✅ Modelo {target} treinado e salvo → métricas: {metrics}")

        payload = {
            "detail": "Modelos treinados e salvos com sucesso",
            "tenant_id": tenant_id,
            "start_date": start_date,
            "end_date": end_date,
            "results": results
        }
        return JSONResponse(content=clean_for_json(payload), status_code=200)

    except Exception as e:
        tb_str = traceback.format_exc()
        logger.error(f"❌ Erro no train_and_save_models: {e}\n{tb_str}")
        raise HTTPException(status_code=500, detail=f"Erro no train_and_save_models: {e}")
