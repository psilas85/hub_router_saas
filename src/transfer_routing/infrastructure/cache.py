import json
import os


def obter_rota_do_cache(origem_str, destino_str, conn, logger=None):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT distancia_km, tempo_minutos, rota_json
                FROM cache_rotas
                WHERE origem = %s
                  AND destino = %s
                LIMIT 1
            """, (origem_str, destino_str))
            row = cur.fetchone()

        if row:
            distancia_km = float(row[0])
            tempo_minutos = float(row[1])
            rota_json = row[2] if isinstance(row[2], dict) else json.loads(row[2])

            if logger:
                logger.info(f"Cache encontrado para {origem_str} -> {destino_str}")

            return distancia_km, tempo_minutos, rota_json
        else:
            if logger:
                logger.info(f"Cache NÃO encontrado para {origem_str} -> {destino_str}")
            return None, None, None

    except Exception as e:
        if logger:
            logger.error(f"Erro ao consultar cache: {e}")
        return None, None, None



def save_route_to_cache(origem_str, destino_str, tenant_id,
                         distancia_km, tempo_minutos, rota_json,
                         conn, logger=None):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cache_rotas (
                    origem, destino, tenant_id,
                    distancia_km, tempo_minutos, rota_json
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (origem, destino) DO NOTHING
            """, (
                origem_str, destino_str, tenant_id,
                distancia_km, tempo_minutos, json.dumps(rota_json)  # <-- AQUI ESTAVA O ERRO
            ))

        conn.commit()

        if logger:
            logger.info(f"Cache salvo para {origem_str} -> {destino_str}")

    except Exception as e:
        conn.rollback()
        if logger:
            logger.error(f"Erro ao salvar no cache: {e}")



def limpar_cache_total(conn, logger=None):
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM cache_rotas")
        conn.commit()
        if logger:
            logger.info(f"Cache global limpo com sucesso.")
    except Exception as e:
        conn.rollback()
        if logger:
            logger.error(f"Erro ao limpar cache: {e}")


def load_cache(tenant_id: str):
    cache_path = f"output/cache/cache_rotas_{tenant_id}.json"
    if os.path.exists(cache_path):
        with open(cache_path, "r", encoding="utf-8") as file:
            return json.load(file)
    else:
        return {}
