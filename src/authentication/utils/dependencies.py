# authentication/utils/dependencies.py

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from authentication.infrastructure.token_service import verificar_token
from authentication.domain.entities import UsuarioToken

# Instância global de HTTPBearer para uso em toda a aplicação
bearer_scheme = HTTPBearer()


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> UsuarioToken:
    """
    Valida o token JWT e retorna um objeto UsuarioToken com as informações do usuário.
    Permite usuários globais (hub_admin, hub_operacional) sem tenant.
    Levanta HTTP 401 se o token for inválido ou expirado.
    """
    try:
        token = credentials.credentials
        payload = verificar_token(token)

        role = payload.get("role")
        tenant_id = payload.get("tenant")

        # 🔹 Globais podem não ter tenant
        if role in ["hub_admin", "hub_operacional"]:
            return UsuarioToken(
                id=payload["sub"],
                tenant_id=None,
                role=role,
                plano=payload.get("plano"),
            )

        # 🔹 Clientes sempre precisam de tenant
        if not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token inválido ou sem tenant_id.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return UsuarioToken(
            id=payload["sub"],
            tenant_id=tenant_id,
            role=role,
            plano=payload.get("plano"),
        )

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def obter_tenant_id_do_token(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> str:
    """
    Valida o token JWT e retorna apenas o tenant_id.
    🔹 Para usuários globais (hub_admin, hub_operacional), levanta erro,
       já que eles não têm tenant.
    """
    try:
        payload = verificar_token(credentials.credentials)

        role = payload.get("role")
        tenant_id = payload.get("tenant")

        if role in ["hub_admin", "hub_operacional"]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Usuários globais não possuem tenant_id.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if not tenant_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token inválido ou ausente",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return tenant_id

    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou ausente",
            headers={"WWW-Authenticate": "Bearer"},
        )
