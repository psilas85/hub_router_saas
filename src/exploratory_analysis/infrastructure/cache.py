# exploratory_analysis/infrastructure/cache.py

import hashlib
import json
import logging
from typing import Any

from redis import Redis

logger = logging.getLogger(__name__)

_redis: Redis | None = None


def get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis(host="redis", port=6379, decode_responses=False)
    return _redis


def make_cache_key(tenant_id: str, data_inicial: str, data_final: str, granularidade: str, analysis: str) -> str:
    raw = f"{tenant_id}:{data_inicial}:{data_final}:{granularidade}:{analysis}"
    return "eda:" + hashlib.sha256(raw.encode()).hexdigest()


def cache_get(key: str) -> dict | None:
    try:
        value = get_redis().get(key)
        if value is None:
            return None
        return json.loads(value)
    except Exception as e:
        logger.warning(f"Cache GET falhou ({key}): {e}")
        return None


def cache_set(key: str, value: Any, ttl: int) -> None:
    try:
        get_redis().setex(key, ttl, json.dumps(value, default=str))
    except Exception as e:
        logger.warning(f"Cache SET falhou ({key}): {e}")
