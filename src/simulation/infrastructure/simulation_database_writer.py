#simulation/infrastructure/database_writer.py
import psycopg2.extras
import pandas as pd

def inserir_hub(simulation_db, tenant_id, nome, endereco, latitude, longitude):
    query = """
        INSERT INTO hubs (tenant_id, nome, endereco, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, nome) DO UPDATE
        SET endereco = EXCLUDED.endereco,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude
    """
    cursor = simulation_db.cursor()
    cursor.execute(query, (tenant_id, nome, endereco, latitude, longitude))
    simulation_db.commit()
    cursor.close()

def remover_hub(simulation_db, tenant_id, nome):
    query = "DELETE FROM hubs WHERE tenant_id = %s AND nome = %s"
    cursor = simulation_db.cursor()
    cursor.execute(query, (tenant_id, nome))
    simulation_db.commit()
    cursor.close()

def inserir_ou_atualizar_tarifa_last_mile(db, tenant_id, tipo_veiculo, capacidade_kg_min, capacidade_kg_max, tarifa_km, tarifa_entrega):
    query = """
        INSERT INTO veiculos_last_mile (tenant_id, tipo_veiculo, capacidade_kg_min, capacidade_kg_max, tarifa_km, tarifa_entrega)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, tipo_veiculo)
        DO UPDATE SET
            capacidade_kg_min = EXCLUDED.capacidade_kg_min,
            capacidade_kg_max = EXCLUDED.capacidade_kg_max,
            tarifa_km = EXCLUDED.tarifa_km,
            tarifa_entrega = EXCLUDED.tarifa_entrega
    """
    cursor = db.cursor()
    tipo_normalizado = tipo_veiculo.strip().lower()
    cursor.execute(query, (tenant_id, tipo_normalizado, capacidade_kg_min, capacidade_kg_max, tarifa_km, tarifa_entrega))
    db.commit()
    cursor.close()


def remover_tarifa_last_mile(db, tipo_veiculo):
    query = "DELETE FROM veiculos_last_mile WHERE tipo_veiculo = %s"
    cursor = db.cursor()
    tipo_normalizado = tipo_veiculo.strip().lower()
    cursor.execute(query, (tipo_normalizado,))
    removidos = cursor.rowcount
    db.commit()
    cursor.close()
    return removidos > 0


def inserir_ou_atualizar_tarifa_transferencia(db, tenant_id, tipo_veiculo, capacidade_kg_min, capacidade_kg_max, tarifa_km, tarifa_fixa):
    query = """
        INSERT INTO veiculos_transferencia (tenant_id, tipo_veiculo, capacidade_kg_min, capacidade_kg_max, tarifa_km, tarifa_fixa)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, tipo_veiculo)
        DO UPDATE SET
            capacidade_kg_min = EXCLUDED.capacidade_kg_min,
            capacidade_kg_max = EXCLUDED.capacidade_kg_max,
            tarifa_km = EXCLUDED.tarifa_km,
            tarifa_fixa = EXCLUDED.tarifa_fixa
    """
    cursor = db.cursor()
    tipo_normalizado = tipo_veiculo.strip().lower()
    cursor.execute(query, (tenant_id, tipo_normalizado, capacidade_kg_min, capacidade_kg_max, tarifa_km, tarifa_fixa))
    db.commit()
    cursor.close()


def remover_tarifa_transferencia(db, tipo_veiculo):
    query = "DELETE FROM veiculos_transferencia WHERE tipo_veiculo = %s"
    cursor = db.cursor()
    tipo_normalizado = tipo_veiculo.strip().lower()
    cursor.execute(query, (tipo_normalizado,))
    removidos = cursor.rowcount
    db.commit()
    cursor.close()
    return removidos > 0


def persistir_resumo_transferencias(lista_resumo: list, db_conn, logger=None, tenant_id=None):
    """
    Persiste os dados de resumo das transferências.
    Cada linha representa uma rota consolidada (rota_id única).
    """
    if not lista_resumo:
        if logger:
            logger.warning(f"⚠️ Nenhum resumo de transferência para persistir. Tenant: {tenant_id}")
        return

    cursor = db_conn.cursor()

    insert_query = """
        INSERT INTO resumo_transferencias (
            rota_id,
            tenant_id,
            envio_data,
            simulation_id,
            k_clusters,
            is_ponto_otimo,
            tipo_veiculo,
            distancia_total_km,
            distancia_parcial_km,
            tempo_total_min,
            tempo_parcial_min,
            peso_total_kg,
            qde_volumes,
            valor_total_nf,
            aproveitamento_percentual,
            qde_entregas,
            qde_clusters_rota,
            coordenadas_seq,
            hub_id,
            hub_nome,
            hub_latitude,
            hub_longitude
        )
        VALUES (
            %(rota_id)s,
            %(tenant_id)s,
            %(envio_data)s,
            %(simulation_id)s,
            %(k_clusters)s,
            %(is_ponto_otimo)s,
            %(tipo_veiculo)s,
            %(distancia_total_km)s,
            %(distancia_parcial_km)s,
            %(tempo_total_min)s,
            %(tempo_parcial_min)s,
            %(peso_total_kg)s,
            %(volumes_total)s,
            %(valor_total_nf)s,
            %(aproveitamento_percentual)s,
            %(qde_entregas)s,
            %(qde_clusters_rota)s,
            %(coordenadas_seq)s,
            %(hub_id)s,
            %(hub_nome)s,
            %(hub_latitude)s,
            %(hub_longitude)s
        )
        ON CONFLICT (rota_id) DO NOTHING
    """

    for resumo in lista_resumo:
        cursor.execute(insert_query, vars(resumo))

    db_conn.commit()
    cursor.close()

    if logger:
        logger.info(f"✅ {len(lista_resumo)} resumos de transferência persistidos para tenant '{tenant_id}'.")

def persistir_resultado_simulacao(db, simulation_id, tenant_id, envio_data,
                                   k_clusters, custo_total, quantidade_entregas,
                                   custo_transferencia, custo_last_mile,
                                   custo_cluster, is_ponto_otimo):
    try:
        cursor = db.cursor()
        cursor.execute("""
            INSERT INTO resultados_simulacao (
                simulation_id, tenant_id, envio_data, k_clusters,
                custo_total, quantidade_entregas, custo_transferencia,
                custo_last_mile, custo_cluster, is_ponto_otimo
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            simulation_id, tenant_id, envio_data, k_clusters,
            custo_total, quantidade_entregas, custo_transferencia,
            custo_last_mile, custo_cluster, is_ponto_otimo
        ))
        db.commit()
    except Exception as e:
        db.rollback()
        raise RuntimeError(f"Erro ao persistir resultado da simulação (k={k_clusters}): {e}")
    finally:
        cursor.close()



def salvar_resumo_clusters_em_db(db_conn, df_resumo: pd.DataFrame, logger):
    cursor = db_conn.cursor()
    # 🔧 Remove registros anteriores do mesmo simulation_id e tenant_id
    simulation_ids = df_resumo["simulation_id"].unique()
    for sim_id in simulation_ids:
        cursor.execute("""
            DELETE FROM resumo_clusters
            WHERE simulation_id = %s AND tenant_id = %s
        """, (str(sim_id), df_resumo["tenant_id"].iloc[0]))

    insert_query = """
        INSERT INTO resumo_clusters (
            tenant_id,
            envio_data,
            simulation_id,
            k_clusters,
            cluster,
            centro_lat,
            centro_lon,
            peso_total_kg,
            volumes_total,
            valor_total_nf,
            qde_ctes,
            cluster_cidade,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """


    delete_query = """
        DELETE FROM resumo_clusters
        WHERE tenant_id = %s
        AND envio_data = %s
        AND simulation_id = %s
        AND k_clusters = %s
        AND cluster = %s
    """

    total = 0
    for _, row in df_resumo.iterrows():
        cursor.execute(delete_query, (
            row["tenant_id"],
            row["envio_data"],
            row["simulation_id"],
            row["k_clusters"],
            row["cluster"]
        ))

        cursor.execute(insert_query, (
            row["tenant_id"],
            row["envio_data"],
            row["simulation_id"],
            row["k_clusters"],
            row["cluster"],
            row["centro_lat"],
            row["centro_lon"],
            row["peso_total_kg"],
            row["volumes_total"],
            row["valor_total_nf"],
            row["qde_ctes"],
            row.get("cluster_cidade"),   # 👈 novo
            row["created_at"]
        ))

        total += 1

    db_conn.commit()
    cursor.close()
    logger.info(f"✅ {total} clusters salvos na tabela resumo_clusters (com sobrescrita preventiva).")



def salvar_detalhes_transferencias(detalhes: list[dict], db_conn):
    """
    Persiste os dados de detalhes das transferências.
    Cada linha representa um CTE alocado a uma rota de transferência.
    """
    if not detalhes:
        return

    # Elimina duplicações com base nas chaves únicas
    chaves_unicas = set()
    detalhes_filtrados = []

    for d in detalhes:
        chave = (d['cte_numero'], d['tenant_id'], d['envio_data'], d['simulation_id'], d['k_clusters'])
        if chave not in chaves_unicas:
            chaves_unicas.add(chave)
            detalhes_filtrados.append(d)

    query = """
        INSERT INTO detalhes_transferencias (
            tenant_id,
            envio_data,
            simulation_id,
            k_clusters,
            is_ponto_otimo,
            cte_numero,
            cluster,
            rota_id,
            tipo_veiculo,
            cte_peso,
            cte_volumes,
            cte_valor_nf,
            cte_valor_frete,
            centro_lat,
            centro_lon,
            created_at
        ) VALUES (
            %(tenant_id)s,
            %(envio_data)s,
            %(simulation_id)s,
            %(k_clusters)s,
            %(is_ponto_otimo)s,
            %(cte_numero)s,
            %(cluster)s,
            %(rota_id)s,
            %(tipo_veiculo)s,
            %(cte_peso)s,
            %(cte_volumes)s,
            %(cte_valor_nf)s,
            %(cte_valor_frete)s,
            %(centro_lat)s,
            %(centro_lon)s,
            CURRENT_TIMESTAMP
        )
    """
    cursor = db_conn.cursor()
    cursor.executemany(query, detalhes_filtrados)
    db_conn.commit()
    cursor.close()



def salvar_rotas_transferencias(rotas: list[dict], db_conn):
    """
    Persiste as rotas de transferência com sequência de coordenadas e tempos/dimensões totais.
    Cada linha representa uma rota_id única.
    """
    if not rotas:
        return

    query = """
    INSERT INTO rotas_transferencias (
        rota_id,
        tenant_id,
        envio_data,
        tipo_veiculo,
        cte_peso,
        coordenadas_seq,
        rota_completa_json,
        distancia_ida_km,
        distancia_total_km,
        tempo_ida_min,
        tempo_total_min,
        k_clusters,
        data_processamento
    )
    VALUES (
        %(rota_id)s,
        %(tenant_id)s,
        %(envio_data)s,
        %(tipo_veiculo)s,
        %(cte_peso)s,
        %(coordenadas_seq)s,
        %(rota_completa_json)s,
        %(distancia_ida_km)s,
        %(distancia_total_km)s,
        %(tempo_ida_min)s,
        %(tempo_total_min)s,
        %(k_clusters)s,
        CURRENT_TIMESTAMP
    )
    ON CONFLICT (rota_id) DO UPDATE SET
        coordenadas_seq = EXCLUDED.coordenadas_seq,
        rota_completa_json = EXCLUDED.rota_completa_json,
        distancia_ida_km = EXCLUDED.distancia_ida_km,
        distancia_total_km = EXCLUDED.distancia_total_km,
        tempo_ida_min = EXCLUDED.tempo_ida_min,
        tempo_total_min = EXCLUDED.tempo_total_min,
        k_clusters = EXCLUDED.k_clusters,
        data_processamento = CURRENT_TIMESTAMP
    """

    cursor = db_conn.cursor()
    cursor.executemany(query, rotas)
    db_conn.commit()
    cursor.close()

