# api_gateway/routers/authentication_router.py

from fastapi import APIRouter, HTTPException, Depends, Path, Request
from pydantic import BaseModel
from typing import Literal

from api_gateway.utils.http_client import forward_request
from api_gateway.config import settings
from api_gateway.auth import verify_token

router = APIRouter(prefix="/auth", tags=["Authentication"])

# --------
# Models
# --------
class LoginRequest(BaseModel):
    email: str
    senha: str


class RegisterRequest(BaseModel):
    nome: str
    email: str
    senha: str
    role: str
    tenant_id: str


class UpdateUserRequest(BaseModel):
    nome: str | None = None
    email: str | None = None
    senha: str | None = None
    role: str | None = None
    tenant_id: str | None = None


class CreateTenantRequest(BaseModel):
    nome: str
    cnpj: str
    plano: Literal["basic", "simulation"]


class UpdateTenantRequest(BaseModel):
    nome: str | None = None
    cnpj: str | None = None
    plano: Literal["basic", "simulation"] | None = None
    ativo: bool | None = None


# --------
# Users/Auth
# --------
@router.post("/login", summary="Login do usuário e obtenção de token JWT")
async def login(request_body: LoginRequest):
    url = f"{settings.AUTH_URL}/auth/login"
    try:
        response = await forward_request("POST", url, json=request_body.dict())
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao autenticar usuário: {str(e)}")


@router.post("/register", summary="Registrar novo usuário")
async def register(request: Request, request_body: RegisterRequest, _=Depends(verify_token)):
    url = f"{settings.AUTH_URL}/auth/register"
    try:
        auth_header = request.headers.get("Authorization")
        response = await forward_request("POST", url, json=request_body.dict(), headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao registrar usuário: {str(e)}")


@router.get("/me", summary="Obter informações do usuário autenticado")
async def get_current_user(user=Depends(verify_token)):
    return {"user": user, "service": "api_gateway"}


@router.get("/list", summary="Listar usuários")
async def list_users(request: Request, _=Depends(verify_token)):
    url = f"{settings.AUTH_URL}/auth/list"
    try:
        auth_header = request.headers.get("Authorization")
        response = await forward_request("GET", url, headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar usuários: {str(e)}")


@router.delete("/{usuario_id}", summary="Remover usuário")
async def delete_user(request: Request, usuario_id: str = Path(..., description="ID do usuário"), _=Depends(verify_token)):
    url = f"{settings.AUTH_URL}/auth/{usuario_id}"
    try:
        auth_header = request.headers.get("Authorization")
        response = await forward_request("DELETE", url, headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao remover usuário: {str(e)}")


@router.put("/{usuario_id}", summary="Atualizar usuário")
async def update_user(
    request: Request,
    usuario_id: str = Path(..., description="ID do usuário"),
    request_body: UpdateUserRequest | None = None,
    _=Depends(verify_token)
):
    url = f"{settings.AUTH_URL}/auth/{usuario_id}"
    try:
        auth_header = request.headers.get("Authorization")
        payload = (request_body.dict(exclude_none=True) if request_body else {})
        response = await forward_request("PUT", url, json=payload, headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar usuário: {str(e)}")


# --------
# Tenants
# --------
@router.get("/tenants", summary="Listar tenants")
async def list_tenants(request: Request, _=Depends(verify_token)):
    url = f"{settings.AUTH_URL}/auth/tenants"
    try:
        auth_header = request.headers.get("Authorization")
        response = await forward_request("GET", url, headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar tenants: {str(e)}")


@router.post("/tenants", summary="Criar tenant")
async def create_tenant(request: Request, body: CreateTenantRequest, _=Depends(verify_token)):
    url = f"{settings.AUTH_URL}/auth/tenants"
    try:
        auth_header = request.headers.get("Authorization")
        response = await forward_request("POST", url, json=body.dict(), headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao criar tenant: {str(e)}")


@router.put("/tenants/{tenant_id}", summary="Atualizar tenant")
async def update_tenant(request: Request, tenant_id: str, body: UpdateTenantRequest, _=Depends(verify_token)):
    url = f"{settings.AUTH_URL}/auth/tenants/{tenant_id}"
    try:
        auth_header = request.headers.get("Authorization")
        response = await forward_request("PUT", url, json=body.dict(exclude_none=True), headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar tenant: {str(e)}")


@router.delete("/tenants/{tenant_id}", summary="Remover tenant")
async def delete_tenant(request: Request, tenant_id: str, _=Depends(verify_token)):
    url = f"{settings.AUTH_URL}/auth/tenants/{tenant_id}"
    try:
        auth_header = request.headers.get("Authorization")
        response = await forward_request("DELETE", url, headers={"Authorization": auth_header})
        return response["content"]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao remover tenant: {str(e)}")
