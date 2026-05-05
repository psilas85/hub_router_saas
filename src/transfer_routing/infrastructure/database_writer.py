import json
import numpy as np


def _native(value):
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {k: _native(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_native(v) for v in value]
    return value

def salvar_transferencias_resumo(rotas, conn, tenant_id: str, envio_data, logger):
    try:
        with conn.cursor() as cur:
            logger.info("Salvando transferencias_resumo...")

            for r in rotas:
                cur.execute("""
                    INSERT INTO transferencias_resumo (
                        envio_data, rota_transf, cte_quantidade, cte_peso, cte_valor_nf, cte_valor_frete,
                        clusters_qde, rota_coord, hub_central_nome, hub_central_latitude, hub_central_longitude,
                        distancia_ida_km, distancia_total_km, tempo_ida_min, tempo_total_min,
                        tempo_transito_ida, tempo_transito_total, tempo_paradas, tempo_descarga,
                        tipo_veiculo, tenant_id, volumes_total, quantidade_entregas, peso_total_kg,
                        aproveitamento_percentual
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s
                    )
                """, (
                    envio_data, _native(r["rota_id"]), _native(r["quantidade_entregas"]), _native(r["cte_peso"]), _native(r["cte_valor_nf"]), _native(r["cte_valor_frete"]),
                    _native(r.get("clusters_qde")), json.dumps(_native(r["rota_coord"])), _native(r["hub_central_nome"]), _native(r["hub_central_latitude"]), _native(r["hub_central_longitude"]),
                    _native(r["distancia_ida_km"]), _native(r["distancia_total_km"]), _native(r["tempo_ida_min"]), _native(r["tempo_total_min"]),
                    _native(r["tempo_transito_ida"]), _native(r["tempo_transito_total"]), _native(r["tempo_paradas"]), _native(r["tempo_descarga"]),
                    _native(r["tipo_veiculo"]), tenant_id, _native(r["volumes_total"]), _native(r["quantidade_entregas"]), _native(r["peso_total_kg"]),
                    _native(r.get("aproveitamento_percentual"))
                ))


            conn.commit()
            logger.info("Dados salvos na tabela transferencias_resumo.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao salvar transferencias_resumo: {e}")
        raise


def salvar_transferencias_detalhes(detalhes, conn, tenant_id: str, envio_data, logger):
    try:
        with conn.cursor() as cur:
            logger.info("Salvando transferencias_detalhes...")

            for d in detalhes:
                cur.execute("""
                    INSERT INTO transferencias_detalhes (
                        envio_data, cte_numero, hub_central_nome, cluster, rota_transf,
                        cte_peso, cte_valor_nf, cte_valor_frete, tenant_id,
                        centro_lat, centro_lon, cte_volumes
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s
                    )
                """, (
                    envio_data, _native(d["cte_numero"]), _native(d["hub_central_nome"]), _native(d["cluster"]), _native(d["rota_id"]),
                    _native(d["cte_peso"]), _native(d["cte_valor_nf"]), _native(d["cte_valor_frete"]), tenant_id,
                    _native(d.get("centro_lat")), _native(d.get("centro_lon")), _native(d.get("cte_volumes"))
                ))

            conn.commit()
            logger.info("Dados salvos na tabela transferencias_detalhes.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao salvar transferencias_detalhes: {e}")
        raise


def salvar_transferencias(rotas_resumo, detalhes, conn, tenant_id, envio_data, logger):
    salvar_transferencias_resumo(rotas_resumo, conn, tenant_id, envio_data, logger)
    salvar_transferencias_detalhes(detalhes, conn, tenant_id, envio_data, logger)


def existe_roteirizacao_transferencias(tenant_id: str, envio_data, conn) -> bool:
    resumo_count, detalhes_count = contar_roteirizacao_transferencias(
        tenant_id,
        envio_data,
        conn,
    )
    return resumo_count > 0 and detalhes_count > 0


def contar_roteirizacao_transferencias(tenant_id: str, envio_data, conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM transferencias_resumo
            WHERE tenant_id = %s
              AND envio_data = %s
        """, (tenant_id, envio_data))
        resumo_count = int(cur.fetchone()[0] or 0)

        cur.execute("""
            SELECT COUNT(*)
            FROM transferencias_detalhes
            WHERE tenant_id = %s
              AND envio_data = %s
        """, (tenant_id, envio_data))
        detalhes_count = int(cur.fetchone()[0] or 0)

    return resumo_count, detalhes_count


def excluir_roteirizacao_transferencias(tenant_id: str, envio_data, conn, logger):
    try:
        with conn.cursor() as cur:
            logger.info(f"Excluindo roteirização existente para {envio_data}...")

            cur.execute("""
                DELETE FROM transferencias_detalhes
                WHERE tenant_id = %s
                  AND envio_data = %s
            """, (tenant_id, envio_data))

            cur.execute("""
                DELETE FROM transferencias_resumo
                WHERE tenant_id = %s
                  AND envio_data = %s
            """, (tenant_id, envio_data))

            conn.commit()
            logger.info("Dados antigos excluídos com sucesso.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro ao excluir dados existentes: {e}")
        raise
