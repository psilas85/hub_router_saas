# api_gateway/routers/exploratory_analysis_routes.py

from fastapi import APIRouter, Request, Query, Depends
from fastapi.responses import FileResponse, JSONResponse
from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings
from authentication.utils.dependencies import obter_tenant_id_do_token
import os

router = APIRouter(prefix="/exploratory", tags=["Análise Exploratória"])

EXPLORATORY_URL = settings.EXPLORATORY_ANALYSIS_URL
EXPORTS_PATH = "./exports/exploratory_analysis"


# 🔹 Healthcheck
@router.get("/health", summary="Healthcheck Exploratory Analysis")
async def healthcheck(request: Request, tenant_id: str = Depends(obter_tenant_id_do_token)):
    """
    Verifica se o serviço de Análise Exploratória está ativo.
    """
    auth = request.headers.get("authorization")
    headers = {}
    if auth:
        headers["authorization"] = auth
    return await forward_request("GET", f"{EXPLORATORY_URL}/exploratory/", headers=headers)


# 🔹 Executar EDA
@router.post("/eda", summary="Executar Análise Exploratória de Entregas")
async def executar_eda(
    request: Request,
    data_inicial: str = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    data_final: str = Query(..., description="Data final no formato YYYY-MM-DD"),
    granularidade: str = Query("diária", description="Granularidade da análise (diária, mensal ou anual)"),
    faixa_cores: str = Query("0:800:green,801:2000:orange,2001:999999:red", description="Faixas de cores no formato min:max:cor"),
    incluir_outliers: bool = Query(False, description="Incluir análise de outliers"),
    tenant_id: str = Depends(obter_tenant_id_do_token)  # 🔐 Autenticação aqui no gateway
):
    """
    Executa análise exploratória de entregas para o tenant autenticado.
    Encaminha requisição ao microserviço Exploratory.
    """
    auth = request.headers.get("authorization")
    headers = {}
    if auth:
        headers["authorization"] = auth

    params = dict(request.query_params)
    return await forward_request(
        "POST",
        f"{EXPLORATORY_URL}/exploratory/eda/",
        headers=headers,
        params=params
    )


# 🔹 Listar arquivos disponíveis
@router.get("/files", summary="Listar arquivos de EDA")
async def listar_arquivos(
    tipo: str = Query("relatorios", description="Tipo de arquivo: relatorios, csvs ou graficos"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    """
    Lista os arquivos disponíveis no diretório de saída da análise exploratória.
    """
    pasta = os.path.join(EXPORTS_PATH, tipo)
    if not os.path.exists(pasta):
        return JSONResponse(status_code=404, content={"erro": f"Pasta {tipo} não encontrada"})
    arquivos = os.listdir(pasta)
    return {"arquivos": arquivos}


# 🔹 Download de arquivo específico
@router.get("/download", summary="Baixar arquivo de EDA")
async def baixar_arquivo(
    tipo: str = Query(..., description="Tipo de arquivo: relatorios, csvs ou graficos"),
    filename: str = Query(..., description="Nome do arquivo para download"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    """
    Faz o download de um arquivo específico da análise exploratória.
    """
    pasta = os.path.join(EXPORTS_PATH, tipo)
    caminho = os.path.join(pasta, filename)
    if not os.path.exists(caminho):
        return JSONResponse(status_code=404, content={"erro": f"Arquivo {filename} não encontrado"})
    return FileResponse(caminho, filename=filename)
