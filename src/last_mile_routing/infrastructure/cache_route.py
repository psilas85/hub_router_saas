#last_mile/infrastructure/cache_route.py

import json
import os


def load_cache(tenant_id):
    pasta = "cache_routes"
    os.makedirs(pasta, exist_ok=True)

    caminho = os.path.join(pasta, f"{tenant_id}.json")

    if os.path.exists(caminho):
        try:
            with open(caminho, "r", encoding="utf-8") as file:
                cache = json.load(file)
                print(f"✅ Cache carregado para {tenant_id}: {len(cache)} registros.")
                return cache
        except Exception as e:
            print(f"⚠️ Erro ao carregar cache: {e}. Iniciando cache vazio.")
            return {}
    else:
        print(f"ℹ️ Nenhum cache existente para {tenant_id}. Iniciando cache vazio.")
        return {}


def save_cache(tenant_id, cache):
    pasta = "cache_routes"
    os.makedirs(pasta, exist_ok=True)

    caminho = os.path.join(pasta, f"{tenant_id}.json")

    try:
        with open(caminho, "w", encoding="utf-8") as file:
            json.dump(cache, file, indent=4, ensure_ascii=False)
            print(f"💾 Cache salvo com sucesso para {tenant_id}: {len(cache)} registros.")
    except Exception as e:
        print(f"❌ Erro ao salvar cache: {e}")
