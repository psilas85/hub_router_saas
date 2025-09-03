#exploratory_analysis/api/routes.py

from fastapi import APIRouter, Query, Depends
from datetime import date
import subprocess
from authentication.utils.dependencies import obter_tenant_id_do_token

router = APIRouter(prefix="/eda", tags=["Análise Exploratória"])

@router.post("/", summary="Executar Análise Exploratória de Entregas")
def executar_eda(
    data_inicial: date = Query(..., description="Data inicial no formato YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final no formato YYYY-MM-DD"),
    granularidade: str = Query("diária", description="Granularidade da análise (diária, mensal ou anual)"),
    faixa_cores: str = Query("0:800:green,801:2000:orange,2001:999999:red", description="Faixas de cores por valor da NF"),
    incluir_outliers: bool = Query(False, description="Incluir análise de outliers"),
    tenant_id: str = Depends(obter_tenant_id_do_token)
):
    """
    Executa a análise exploratória de entregas para o tenant autenticado.
    Gera gráficos, mapas e relatórios conforme parâmetros definidos.
    """
    try:
        comando = [
            "python",
            "-m",
            "exploratory_analysis.main_eda",
            "--tenant", tenant_id,
            "--data-inicial", str(data_inicial),
            "--data-final", str(data_final),
            "--granularidade", granularidade,
            "--faixa-cores", faixa_cores
        ]

        if incluir_outliers:
            comando.append("--incluir-outliers")

        result = subprocess.run(comando, capture_output=True, text=True)

        if result.returncode != 0:
            return {"status": "erro", "mensagem": result.stderr}

        return {
            "status": "ok",
            "mensagem": f"✅ EDA executada com sucesso para tenant {tenant_id}",
            "output": result.stdout
        }

    except Exception as e:
        return {"status": "erro", "mensagem": str(e)}
