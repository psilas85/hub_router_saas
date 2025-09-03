#api_gateway/utils/http_client.py

import httpx
from fastapi import HTTPException

async def forward_request(method, url, headers=None, params=None, json=None, data=None):
    async with httpx.AsyncClient(timeout=1200.0) as client:
        try:
            response = await client.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json,
                data=data
            )
            response.raise_for_status()

            content_type = response.headers.get("content-type", "").lower()

            # JSON → parse normalmente
            if "application/json" in content_type:
                return {
                    "status_code": response.status_code,
                    "content": response.json(),
                    "headers": dict(response.headers)
                }

            # PDF ou qualquer outro binário
            if any(ct in content_type for ct in ["application/pdf", "application/octet-stream"]):
                return {
                    "status_code": response.status_code,
                    "content": response.content,  # bytes!
                    "headers": dict(response.headers)
                }

            # fallback: devolve texto puro
            return {
                "status_code": response.status_code,
                "content": response.text,
                "headers": dict(response.headers)
            }

        except httpx.HTTPStatusError as e:
            try:
                detail = e.response.json()
            except Exception:
                detail = e.response.text
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"Erro no serviço {url}: {detail}"
            )

        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Erro inesperado ao chamar {url}: {str(e) or 'sem detalhes'}"
            )
