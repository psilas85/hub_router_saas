# authentication/api/routes.py

from fastapi import APIRouter, HTTPException, Depends, Path
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime
from typing import Literal

from authentication.application.auth_service import AuthService
from authentication.infrastructure.auth_repository import AuthRepository
from authentication.infrastructure.database_connection import conectar_banco_cluster as conectar_banco
from authentication.middleware.jwt_middleware import get_current_user
from authentication.utils.password_utils import gerar_hash_senha

router = APIRouter(tags=["Authentication"])

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


def _only_digits(s: str) -> str:
    return "".join(ch for ch in s if ch.isdigit())


# --------
# Endpoints Auth/Users
# --------
@router.post("/login", summary="Realizar login e obter token JWT")
def login(request: LoginRequest):
    conn = conectar_banco()
    service = AuthService(AuthRepository(conn))
    try:
        return service.login(request.email, request.senha)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Erro ao autenticar usuário: {str(e)}")


@router.post("/register", summary="Registrar novo usuário")
def register(request: RegisterRequest, current_user=Depends(get_current_user)):
    if current_user["role"] not in ["hub_admin", "cliente_admin"]:
        raise HTTPException(status_code=403, detail="Sem permissão para criar usuários")

    conn = conectar_banco()
    service = AuthService(AuthRepository(conn))
    try:
        tenant_id = request.tenant_id
        if current_user["role"] == "cliente_admin":
            tenant_id = current_user["tenant_id"]
            if request.role not in ["cliente_operacional"]:
                raise HTTPException(
                    status_code=403,
                    detail="Cliente Admin só pode criar usuários operacionais"
                )

        service.cadastrar_usuario(
            nome=request.nome,
            email=request.email,
            senha=request.senha,
            role=request.role,
            tenant_id=tenant_id
        )
        return {"mensagem": "Usuário cadastrado com sucesso"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao registrar usuário: {str(e)}")
    finally:
        conn.close()


@router.get("/me", summary="Obter informações do usuário autenticado")
def get_current_user_info(user=Depends(get_current_user)):
    return {"user": user, "service": "authentication"}


@router.get("/list", summary="Listar usuários")
def list_users(user=Depends(get_current_user)):
    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            if user["role"] == "hub_admin":
                cur.execute("""
                    SELECT id, nome, email, role, tenant_id
                    FROM usuarios
                    ORDER BY criado_em DESC
                """)
            else:
                cur.execute("""
                    SELECT id, nome, email, role, tenant_id
                    FROM usuarios
                    WHERE tenant_id = %s
                    ORDER BY criado_em DESC
                """, (user["tenant_id"],))
            rows = cur.fetchall()
            return [
                {"id": r[0], "nome": r[1], "email": r[2], "role": r[3], "tenant_id": r[4]}
                for r in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar usuários: {str(e)}")
    finally:
        conn.close()


@router.delete("/{usuario_id}", summary="Remover usuário")
def delete_user(
    usuario_id: str = Path(..., description="ID do usuário a ser removido"),
    current_user=Depends(get_current_user)
):
    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, tenant_id, role FROM usuarios WHERE id = %s", (usuario_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Usuário não encontrado")

            _, tenant_id, role = row
            if current_user["role"] == "hub_admin":
                pass
            elif current_user["role"] == "cliente_admin":
                if tenant_id != current_user["tenant_id"]:
                    raise HTTPException(status_code=403, detail="Sem permissão para deletar usuários de outro tenant")
                if role == "cliente_admin":
                    raise HTTPException(status_code=403, detail="Cliente Admin não pode remover outro Cliente Admin")
            else:
                raise HTTPException(status_code=403, detail="Operacionais não podem remover usuários")

            cur.execute("DELETE FROM usuarios WHERE id = %s", (usuario_id,))
            conn.commit()
            return {"mensagem": f"Usuário {usuario_id} removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erro ao remover usuário: {str(e)}")
    finally:
        conn.close()


@router.put("/{usuario_id}", summary="Atualizar usuário")
def update_user(
    usuario_id: str = Path(..., description="ID do usuário a ser atualizado"),
    request: UpdateUserRequest | None = None,
    current_user=Depends(get_current_user)
):
    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, tenant_id, role FROM usuarios WHERE id = %s", (usuario_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Usuário não encontrado")

            _, tenant_id, role_atual = row

            if current_user["role"] == "hub_admin":
                pass
            elif current_user["role"] == "cliente_admin":
                if tenant_id != current_user["tenant_id"]:
                    raise HTTPException(status_code=403, detail="Sem permissão para atualizar usuários de outro tenant")
                if role_atual == "cliente_admin" and request and request.role and request.role != "cliente_admin":
                    raise HTTPException(status_code=403, detail="Cliente Admin não pode alterar role de outro Cliente Admin")
            else:
                raise HTTPException(status_code=403, detail="Operacionais não podem atualizar usuários")

            if not request:
                raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

            updates, values = [], []
            if request.nome is not None:
                updates.append("nome = %s"); values.append(request.nome)
            if request.email is not None:
                updates.append("email = %s"); values.append(request.email)
            if request.senha:
                updates.append("senha_hash = %s"); values.append(gerar_hash_senha(request.senha))
            if request.role is not None:
                updates.append("role = %s"); values.append(request.role)
            if request.tenant_id is not None and current_user["role"] == "hub_admin":
                updates.append("tenant_id = %s"); values.append(request.tenant_id)

            if not updates:
                raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

            query = f"UPDATE usuarios SET {', '.join(updates)} WHERE id = %s"
            values.append(usuario_id)
            cur.execute(query, tuple(values))
            conn.commit()
            return {"mensagem": f"Usuário {usuario_id} atualizado com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Erro ao atualizar usuário: {str(e)}")
    finally:
        conn.close()


# --------
# Endpoints Tenants
# --------
@router.get("/tenants", summary="Listar tenants")
def list_tenants(current_user=Depends(get_current_user)):
    if current_user["role"] != "hub_admin":
        raise HTTPException(status_code=403, detail="Apenas Hub Admin pode listar tenants")

    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, nome, cnpj, plano, ativo, criado_em
                FROM tenants
                ORDER BY criado_em DESC
            """)
            rows = cur.fetchall()
            return [
                {
                    "id": r[0],
                    "nome": r[1],
                    "cnpj": r[2],
                    "plano": r[3],
                    "ativo": r[4],
                    "criado_em": r[5].isoformat() if r[5] else None,
                }
                for r in rows
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao listar tenants: {str(e)}")
    finally:
        conn.close()


@router.post("/tenants", summary="Criar tenant")
def create_tenant(request: CreateTenantRequest, current_user=Depends(get_current_user)):
    if current_user["role"] != "hub_admin":
        raise HTTPException(status_code=403, detail="Apenas Hub Admin pode criar tenants")

    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            tenant_id = str(uuid4())
            now = datetime.utcnow()
            cnpj = _only_digits(request.cnpj)
            if len(cnpj) != 14:
                raise HTTPException(status_code=400, detail="CNPJ inválido. Informe 14 dígitos.")

            cur.execute("""
                INSERT INTO tenants (id, nome, cnpj, plano, ativo, criado_em)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (tenant_id, request.nome, cnpj, request.plano, True, now))
            conn.commit()
            return {
                "id": tenant_id,
                "nome": request.nome,
                "cnpj": cnpj,
                "plano": request.plano,
                "ativo": True,
                "criado_em": now.isoformat(),
            }
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao criar tenant: {str(e)}")
    finally:
        conn.close()


@router.put("/tenants/{tenant_id}", summary="Atualizar tenant")
def update_tenant(
    tenant_id: str = Path(..., description="ID do tenant"),
    request: UpdateTenantRequest | None = None,
    current_user=Depends(get_current_user)
):
    if current_user["role"] != "hub_admin":
        raise HTTPException(status_code=403, detail="Apenas Hub Admin pode atualizar tenants")

    if not request:
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM tenants WHERE id = %s", (tenant_id,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Tenant não encontrado")

            updates, values = [], []
            if request.nome is not None:
                updates.append("nome = %s"); values.append(request.nome)
            if request.cnpj is not None:
                cnpj = _only_digits(request.cnpj)
                if len(cnpj) != 14:
                    raise HTTPException(status_code=400, detail="CNPJ inválido. Informe 14 dígitos.")
                updates.append("cnpj = %s"); values.append(cnpj)
            if request.plano is not None:
                updates.append("plano = %s"); values.append(request.plano)
            if request.ativo is not None:
                updates.append("ativo = %s"); values.append(request.ativo)

            if not updates:
                raise HTTPException(status_code=400, detail="Nenhum campo para atualizar")

            query = f"UPDATE tenants SET {', '.join(updates)} WHERE id = %s"
            values.append(tenant_id)
            cur.execute(query, tuple(values))
            conn.commit()
            return {"mensagem": f"Tenant {tenant_id} atualizado com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao atualizar tenant: {str(e)}")
    finally:
        conn.close()


@router.delete("/tenants/{tenant_id}", summary="Remover tenant")
def delete_tenant(
    tenant_id: str = Path(..., description="ID do tenant"),
    current_user=Depends(get_current_user)
):
    if current_user["role"] != "hub_admin":
        raise HTTPException(status_code=403, detail="Apenas Hub Admin pode remover tenants")

    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            # Opcional: impedir remoção se existir usuário vinculado
            cur.execute("SELECT 1 FROM usuarios WHERE tenant_id = %s LIMIT 1", (tenant_id,))
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Não é possível remover: existem usuários vinculados")

            cur.execute("DELETE FROM tenants WHERE id = %s", (tenant_id,))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Tenant não encontrado")
            conn.commit()
            return {"mensagem": f"Tenant {tenant_id} removido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=400, detail=f"Erro ao remover tenant: {str(e)}")
    finally:
        conn.close()
