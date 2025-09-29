# src/api_gateway/routers/simulation_routes.py

from fastapi import APIRouter, Request, Depends, Query, HTTPException
from datetime import date
from authentication.utils.dependencies import obter_tenant_id_do_token
from authentication.domain.entities import UsuarioToken
from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings

router = APIRouter(prefix="/simulation", tags=["Simulation"])

SIMULATION_URL = settings.SIMULATION_URL

@router.get("/health", summary="Healthcheck Simulation")
async def healthcheck(request: Request):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("GET", f"{SIMULATION_URL}/simulation/health", headers=headers)

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    return result["content"]


@router.post("/executar", summary="Executar simulação completa")
async def executar_simulacao(
    request: Request,

    # 📅 Datas e controle
    data_inicial: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_final: date = Query(..., description="Data final (YYYY-MM-DD)"),
    modo_forcar: bool = Query(False, description="Sobrescreve simulações existentes"),

    # 🔗 Hub central (👈 Faltava)
    hub_id: int = Query(..., description="ID do hub central"),  # ✅ adicionado

    # 🔢 Clusterização
    k_min: int = Query(2, description="Valor mínimo de k_clusters"),
    k_max: int = Query(50, description="Valor máximo de k_clusters"),
    k_inicial_transferencia: int = Query(1, description="Valor inicial de k para clusterização de transferências"),
    min_entregas_cluster: int = Query(25, description="Qtd mínima de entregas por cluster"),
    fundir_clusters_pequenos: bool = Query(False, description="Funde clusters pequenos com menos entregas"),

    # 🔗 Cluster hub
    desativar_cluster_hub: bool = Query(False, description="Desativa cluster automático do hub central"),
    raio_hub_km: float = Query(80.0, description="Raio em km para considerar entregas no hub central"),

    # ⏱️ Tempos
    parada_leve: int = Query(10, description="Tempo de parada leve (min)"),
    parada_pesada: int = Query(20, description="Tempo de parada pesada (min)"),
    tempo_volume: float = Query(0.40, description="Tempo por volume (min)"),

    # 🚚 Operações
    velocidade: float = Query(60.0, description="Velocidade média (km/h)"),
    limite_peso: float = Query(50.0, description="Limite de peso para parada pesada (kg)"),

    # ⚙️ Restrições de veículo
    restricao_veiculo_leve_municipio: bool = Query(False, description="Impede veículos leves fora do município do hub central"),
    peso_leve_max: float = Query(50.0, description="Peso máximo para considerar veículo leve"),

    # 🔗 Transferências
    tempo_max_transferencia: int = Query(1200, description="Tempo máximo de rota de transferência (min)"),
    peso_max_transferencia: float = Query(15000.0, description="Peso máximo por rota de transferência (kg)"),

    # 📦 Last-mile
    entregas_por_subcluster: int = Query(25, description="Qtd alvo de entregas por subcluster"),
    tempo_max_roteirizacao: int = Query(1200, description="Tempo máximo total por rota last-mile (min)"),
    tempo_max_k1: int = Query(2400, description="Tempo máximo quando k=1 (rota única pelo hub central)"),

    # ⚙️ Rotas excedentes
    permitir_rotas_excedentes: bool = Query(False, description="Permite aceitar rotas que ultrapassem o tempo máximo"),

    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    """
    Encaminha requisição do API Gateway → Simulation Service.
    Todos os parâmetros do main_simulation.py, exceto modo_forcar (fixo = True).
    """

    params = {
        # 📅 Datas
        "data_inicial": data_inicial,
        "data_final": data_final,
        "modo_forcar": modo_forcar,
        "hub_id": hub_id,

        # 🔢 Clusterização
        "k_min": k_min,
        "k_max": k_max,
        "k_inicial_transferencia": k_inicial_transferencia,
        "min_entregas_cluster": min_entregas_cluster,
        "fundir_clusters_pequenos": fundir_clusters_pequenos,

        # 🔗 Cluster hub
        "desativar_cluster_hub": desativar_cluster_hub,
        "raio_hub_km": raio_hub_km,

        # ⏱️ Tempos
        "parada_leve": parada_leve,
        "parada_pesada": parada_pesada,
        "tempo_volume": tempo_volume,

        # 🚚 Operações
        "velocidade": velocidade,
        "limite_peso": limite_peso,

        # ⚙️ Restrições
        "restricao_veiculo_leve_municipio": restricao_veiculo_leve_municipio,
        "peso_leve_max": peso_leve_max,

        # 🔗 Transferências
        "tempo_max_transferencia": tempo_max_transferencia,
        "peso_max_transferencia": peso_max_transferencia,

        # 📦 Last-mile
        "entregas_por_subcluster": entregas_por_subcluster,
        "tempo_max_roteirizacao": tempo_max_roteirizacao,
        "tempo_max_k1": tempo_max_k1,

        # ⚙️ Rotas excedentes
        "permitir_rotas_excedentes": permitir_rotas_excedentes,
    }

    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request(
        "POST",
        f"{SIMULATION_URL}/simulation/executar",
        headers=headers,
        params=params
    )

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    return result["content"]


@router.get("/visualizar", summary="Visualizar artefatos da simulação")
async def visualizar_simulacao(
    request: Request,
    data: date = Query(..., description="Data no formato YYYY-MM-DD"),
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    """
    Encaminha requisição do API Gateway → Simulation Service para visualizar artefatos da simulação.
    """
    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/visualizar",
        headers=headers,
        params={"data": data}
    )

    # 🔑 Correção: checar se veio status de erro e propagar corretamente
    status_code = result.get("status_code", 500)
    content = result.get("content")

    if status_code >= 400:
        # Se o serviço já mandou detail, repassa direto
        detail = content if isinstance(content, str) else content or "Erro ao buscar artefatos."
        raise HTTPException(status_code=status_code, detail=detail)

    return content


@router.get("/distribuicao_k", summary="Distribuição de k_clusters ponto ótimo")
async def distribuicao_k(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    """
    Encaminha requisição do API Gateway → Simulation Service para obter distribuição de k_clusters ponto ótimo.
    """
    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/distribuicao_k",
        headers=headers,
        params={"data_inicial": data_inicial, "data_final": data_final}
    )

    status_code = result.get("status_code", 500)
    content = result.get("content")

    if status_code >= 400:
        detail = content if isinstance(content, str) else content or "Erro ao buscar distribuição de k."
        raise HTTPException(status_code=status_code, detail=detail)

    return content

@router.get("/frequencia_cidades", summary="Frequência de cidades em pontos ótimos")
async def frequencia_cidades(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    """
    Encaminha requisição do API Gateway → Simulation Service
    para obter a frequência de cidades em pontos ótimos.
    """
    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/frequencia_cidades",
        headers=headers,
        params={"data_inicial": data_inicial, "data_final": data_final}
    )

    status_code = result.get("status_code", 500)
    content = result.get("content")

    if status_code >= 400:
        detail = content if isinstance(content, str) else content or "Erro ao buscar frequência de cidades."
        raise HTTPException(status_code=status_code, detail=detail)

    return content


@router.get("/k_fixo", summary="Custos consolidados para k fixo")
async def k_fixo(
    request: Request,
    data_inicial: date = Query(..., description="Data inicial YYYY-MM-DD"),
    data_final: date = Query(..., description="Data final YYYY-MM-DD"),
    min_cobertura_parcial: float = Query(
        0.70, description="Cobertura mínima exigida (ex: 0.70 = 70%)"
    ),
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    """
    Encaminha requisição do API Gateway → Simulation Service para obter cenários k fixo.
    """
    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/k_fixo",
        headers=headers,
        params={
            "data_inicial": data_inicial,
            "data_final": data_final,
            "min_cobertura_parcial": min_cobertura_parcial,
        },
    )

    status_code = result.get("status_code", 500)
    content = result.get("content")

    if status_code >= 400:
        detail = (
            content if isinstance(content, str) else content or "Erro ao buscar k_fixo."
        )
        raise HTTPException(status_code=status_code, detail=detail)

    return content

# ============================
# CRUD Hubs
# ============================
@router.get("/hubs", summary="Listar hubs")
async def listar_hubs(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("GET", f"{SIMULATION_URL}/simulation/hubs", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.post("/hubs", summary="Criar hub")
async def criar_hub(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    body = await request.json()
    result = await forward_request("POST", f"{SIMULATION_URL}/simulation/hubs", headers=headers, json=body)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.put("/hubs/{hub_id}", summary="Atualizar hub")
async def atualizar_hub(hub_id: int, request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    body = await request.json()
    result = await forward_request("PUT", f"{SIMULATION_URL}/simulation/hubs/{hub_id}", headers=headers, json=body)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.delete("/hubs/{hub_id}", summary="Excluir hub")
async def excluir_hub(hub_id: int, request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("DELETE", f"{SIMULATION_URL}/simulation/hubs/{hub_id}", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


# ============================
# CRUD Cluster Costs
# ============================
@router.get("/cluster_costs", summary="Obter custos do tenant")
async def obter_costs(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("GET", f"{SIMULATION_URL}/simulation/cluster_costs", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.post("/cluster_costs", summary="Criar/atualizar custos do tenant")
async def upsert_costs(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    body = await request.json()
    result = await forward_request("POST", f"{SIMULATION_URL}/simulation/cluster_costs", headers=headers, json=body)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.delete("/cluster_costs/{id}", summary="Excluir custo específico do tenant")
async def excluir_cost(id: int, request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("DELETE", f"{SIMULATION_URL}/simulation/cluster_costs/{id}", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.get("/cluster_costs/list", summary="Listar custos do tenant (array)")
async def listar_costs(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/cluster_costs/list",
        headers=headers
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

@router.get("/frota_k_fixo", summary="Frota média sugerida para k fixo")
async def frota_k_fixo(
    request: Request,
    data_inicial: date = Query(...),
    data_final: date = Query(...),
    k: int = Query(...),
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/frota_k_fixo",
        headers=headers,
        params={"data_inicial": data_inicial, "data_final": data_final, "k": k},
    )

    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])

    return result["content"]

@router.get("/status/{job_id}", summary="Status do processamento da simulação")
async def status_simulacao(
    job_id: str,
    request: Request,
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    """
    Proxy do API Gateway → Simulation Service para consultar status de job.
    Normaliza a saída (status, job_id, tenant_id, progress, step, mensagem, error).
    """
    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/status/{job_id}",
        headers=headers,
    )

    status_code = result.get("status_code", 500)
    content = result.get("content")

    if status_code >= 400:
        if isinstance(content, dict) and "detail" in content:
            detail = content["detail"]
        elif isinstance(content, str):
            detail = content
        else:
            detail = f"Erro ao consultar status do job {job_id}."
        raise HTTPException(status_code=status_code, detail=detail)

    # 🔹 Normaliza a resposta
    retorno = {
        "status": content.get("status"),
    }

    # Se backend devolveu `result` dentro
    if "result" in content and isinstance(content["result"], dict):
        retorno.update(content["result"])
    else:
        for campo in [
            "job_id",
            "tenant_id",
            "mensagem",
            "progress",
            "step",
            "error",
            "datas_processadas",
            "ended_at",
        ]:
            if campo in content:
                retorno[campo] = content[campo]

    return retorno


@router.get("/historico", summary="Histórico de simulações")
async def proxy_historico_simulation(
    request: Request,
    limit: int = 10,
    usuario: UsuarioToken = Depends(obter_tenant_id_do_token),
):
    headers = {"authorization": request.headers.get("authorization")}

    result = await forward_request(
        "GET",
        f"{SIMULATION_URL}/simulation/historico",
        headers=headers,
        params={"limit": limit}
    )

    if result["status_code"] >= 400:
        raise HTTPException(
            status_code=result["status_code"],
            detail=result["content"]
        )

    return result["content"]

from urllib.parse import quote

# ============================
# CRUD Tarifas Last-mile
# ============================
@router.get("/tarifas/lastmile", summary="Listar tarifas last-mile")
async def listar_tarifas_lm(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("GET", f"{SIMULATION_URL}/simulation/tarifas/lastmile", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.post("/tarifas/lastmile", summary="Inserir/atualizar tarifa last-mile")
async def upsert_tarifa_lm(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    body = await request.json()
    result = await forward_request("POST", f"{SIMULATION_URL}/simulation/tarifas/lastmile", headers=headers, json=body)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.delete("/tarifas/lastmile/{veiculo:path}", summary="Remover tarifa last-mile")
async def remover_tarifa_lm(veiculo: str, request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request(
        "DELETE",
        f"{SIMULATION_URL}/simulation/tarifas/lastmile/{quote(veiculo, safe='')}",
        headers=headers
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


# ============================
# CRUD Tarifas Transferência
# ============================
@router.get("/tarifas/transferencia", summary="Listar tarifas de transferência")
async def listar_tarifas_transf(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request("GET", f"{SIMULATION_URL}/simulation/tarifas/transferencia", headers=headers)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.post("/tarifas/transferencia", summary="Inserir/atualizar tarifa de transferência")
async def upsert_tarifa_transf(request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    body = await request.json()
    result = await forward_request("POST", f"{SIMULATION_URL}/simulation/tarifas/transferencia", headers=headers, json=body)
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]


@router.delete("/tarifas/transferencia/{veiculo:path}", summary="Remover tarifa de transferência")
async def remover_tarifa_transf(veiculo: str, request: Request, usuario: UsuarioToken = Depends(obter_tenant_id_do_token)):
    headers = {"authorization": request.headers.get("authorization")}
    result = await forward_request(
        "DELETE",
        f"{SIMULATION_URL}/simulation/tarifas/transferencia/{quote(veiculo, safe='')}",
        headers=headers
    )
    if result["status_code"] >= 400:
        raise HTTPException(status_code=result["status_code"], detail=result["content"])
    return result["content"]

