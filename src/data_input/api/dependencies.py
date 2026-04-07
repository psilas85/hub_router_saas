#hub_router_1.0.1/src/data_input/api/dependencies.py

from fastapi import Request, HTTPException, status
import jwt
import os

JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "supersecretkey")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")


async def verify_token(request: Request):

    auth = request.headers.get("Authorization")

    if not auth:
        raise HTTPException(status_code=401, detail="Token ausente")

    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Formato inválido")

    token = auth.replace("Bearer ", "")

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM]
        )

        request.state.user = {
            "user_id": payload.get("user_id") or payload.get("sub"),
            "tenant_id": payload.get("tenant_id") or payload.get("tenant"),
            "role": payload.get("role")
        }

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")

    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")