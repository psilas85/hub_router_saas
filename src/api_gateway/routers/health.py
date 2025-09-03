#api_gateway/routers/health.py

from fastapi import APIRouter

router = APIRouter()

@router.get("", include_in_schema=False)   # cobre /health sem barra
@router.get("/", tags=["Healthcheck"])     # cobre /health/ com barra
async def health_check():
    return {"status": "ok", "service": "api_gateway"}
