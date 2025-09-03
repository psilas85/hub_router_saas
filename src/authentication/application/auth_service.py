# authentication/use_case/auth_service.py

import uuid
from datetime import datetime

from authentication.infrastructure.token_service import gerar_token
from authentication.utils.password_utils import gerar_hash_senha, verificar_senha
from authentication.domain.entities import Usuario
from authentication.infrastructure.auth_repository import AuthRepository

class AuthService:
    def __init__(self, repo: AuthRepository):
        self.repo = repo

    def login(self, email: str, senha: str) -> dict:
        usuario = self.repo.buscar_usuario_por_email(email)
        if not usuario or not usuario.ativo:
            raise ValueError("Usuário não encontrado ou inativo")

        if not verificar_senha(senha, usuario.senha_hash):
            raise ValueError("Senha inválida")

        # 🔹 Caso especial: usuários globais não precisam de tenant
        if usuario.role in ["hub_admin", "hub_operacional"]:
            token = gerar_token(usuario.id, None, usuario.role, "hub")
            return {
                "access_token": token,
                "token_type": "bearer",
                "usuario": {
                    "id": usuario.id,
                    "nome": usuario.nome,
                    "email": usuario.email,
                    "role": usuario.role,
                    "tenant_id": None,   # ✅ garante consistência
                },
                "tenant": None  # ✅ sempre null, nunca {}
            }

        # 🔹 Usuários cliente precisam de tenant válido
        tenant = self.repo.buscar_tenant_por_id(usuario.tenant_id)
        if not tenant or not tenant.ativo:
            raise ValueError("Tenant inativo")

        token = gerar_token(usuario.id, usuario.tenant_id, usuario.role, tenant.plano)

        return {
            "access_token": token,
            "token_type": "bearer",
            "usuario": {
                "id": usuario.id,
                "nome": usuario.nome,
                "email": usuario.email,
                "role": usuario.role,
                "tenant_id": usuario.tenant_id,  # ✅ agora vai junto no payload
            },
            "tenant": {
                "id": tenant.id,
                "nome": tenant.nome,
                "plano": tenant.plano,
            }
        }


    def cadastrar_usuario(self, nome: str, email: str, senha: str, role: str, tenant_id: str | None):
        if not tenant_id:  # se vier "" ou None
            tenant_id = None

        novo_usuario = Usuario(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            nome=nome,
            email=email,
            senha_hash=gerar_hash_senha(senha),
            role=role,
            ativo=True,
            criado_em=datetime.utcnow()
        )
        self.repo.criar_usuario(novo_usuario)

