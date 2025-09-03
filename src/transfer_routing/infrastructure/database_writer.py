import json

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
                    envio_data, r["rota_id"], r["quantidade_entregas"], r["cte_peso"], r["cte_valor_nf"], r["cte_valor_frete"],
                    r.get("clusters_qde"), json.dumps(r["rota_coord"]), r["hub_central_nome"], r["hub_central_latitude"], r["hub_central_longitude"],
                    r["distancia_ida_km"], r["distancia_total_km"], r["tempo_ida_min"], r["tempo_total_min"],
                    r["tempo_transito_ida"], r["tempo_transito_total"], r["tempo_paradas"], r["tempo_descarga"],
                    r["tipo_veiculo"], tenant_id, r["volumes_total"], r["quantidade_entregas"], r["peso_total_kg"],
                    r.get("aproveitamento_percentual")
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
                    envio_data, d["cte_numero"], d["hub_central_nome"], d["cluster"], d["rota_id"],
                    d["cte_peso"], d["cte_valor_nf"], d["cte_valor_frete"], tenant_id,
                    d.get("centro_lat"), d.get("centro_lon"), d.get("cte_volumes")
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
    query = """
        SELECT COUNT(*) 
        FROM transferencias_resumo
        WHERE tenant_id = %s
          AND envio_data = %s
    """
    with conn.cursor() as cur:
        cur.execute(query, (tenant_id, envio_data))
        count = cur.fetchone()[0]
    return count > 0


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
