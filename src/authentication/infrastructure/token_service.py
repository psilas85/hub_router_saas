# authentication/infrastructure/token_service.py
import os
import jwt
from datetime import datetime, timedelta

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "changeme")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
EXPIRACAO_MINUTOS = 120

def gerar_token(usuario_id: str, tenant_id: str | None, role: str, plano: str) -> str:
    payload = {
        "sub": usuario_id,
        "role": role,
        "plano": plano,
        "exp": datetime.utcnow() + timedelta(minutes=EXPIRACAO_MINUTOS)
    }

    # 🔹 só adiciona tenant se existir
    if tenant_id is not None:
        payload["tenant"] = tenant_id

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verificar_token(token: str) -> dict:
    return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
