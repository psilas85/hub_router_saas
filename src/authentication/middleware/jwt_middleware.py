# authentication/middleware/jwt_middleware.py

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from authentication.utils.dependencies import bearer_scheme
from authentication.infrastructure.token_service import verificar_token

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
):
    token = credentials.credentials
    try:
        payload = verificar_token(token)
        return {
            "usuario_id": payload.get("sub"),
            "tenant_id": payload.get("tenant"),   # ✅ agora opcional
            "role": payload.get("role"),
            "plano": payload.get("plano")
        }
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")
