# exploratory_analysis/api/deps.py

from typing import Annotated

from fastapi import Depends

from authentication.utils.dependencies import obter_tenant_id_do_token
from exploratory_analysis.infrastructure.cache import cache_get, cache_set, make_cache_key


TenantId = Annotated[str, Depends(obter_tenant_id_do_token)]


class CacheLayer:
    def __init__(self, tenant_id: str, data_inicial: str, data_final: str, granularidade: str):
        self.tenant_id = tenant_id
        self.data_inicial = data_inicial
        self.data_final = data_final
        self.granularidade = granularidade

    def get(self, analysis: str) -> dict | None:
        key = make_cache_key(self.tenant_id, self.data_inicial, self.data_final, self.granularidade, analysis)
        return cache_get(key)

    def set(self, analysis: str, value: dict, ttl: int) -> None:
        key = make_cache_key(self.tenant_id, self.data_inicial, self.data_final, self.granularidade, analysis)
        cache_set(key, value, ttl)
