#simulation_database_reader.py

import pandas as pd

def carregar_tarifas_transferencia(db_conn) -> pd.DataFrame:
    query = "SELECT * FROM veiculos_transferencia"
    return pd.read_sql(query, db_conn)

def carregar_tarifas_last_mile(db_conn) -> pd.DataFrame:
    query = "SELECT * FROM veiculos_last_mile"
    return pd.read_sql(query, db_conn)

def definir_tipo_veiculo_transferencia(peso_total: float, db_conn) -> str:
    """
    Retorna o tipo de veículo adequado ao peso total da transferência
    com base na tabela `veiculos_transferencia`. Se nenhum veículo for
    compatível, retorna o de maior capacidade.
    """
    cursor = db_conn.cursor()
    query = """
        SELECT tipo_veiculo
        FROM veiculos_transferencia
        WHERE capacidade_kg_min <= %s AND capacidade_kg_max >= %s
        ORDER BY capacidade_kg_max ASC
        LIMIT 1
    """
    cursor.execute(query, (peso_total, peso_total))
    resultado = cursor.fetchone()

    if resultado:
        return resultado[0]

    # Fallback: retorna o veículo com maior capacidade
    cursor.execute("""
        SELECT tipo_veiculo
        FROM veiculos_transferencia
        ORDER BY capacidade_kg_max DESC
        LIMIT 1
    """)
    fallback = cursor.fetchone()
    cursor.close()

    return fallback[0] if fallback else "Desconhecido"

def definir_tipo_veiculo_last_mile(
    peso_total: float,
    df_tarifas: pd.DataFrame,
    cluster_cidade=None,
    cidades_entregas=None,
    logger=None
) -> tuple[str, float]:
    """
    Retorna o tipo de veículo ideal para o last-mile dado o peso total.
    Remove motocicleta se as entregas forem para mais de uma cidade.
    Retorna o veículo de menor capacidade que suporte o peso.
    """
    peso_total = float(peso_total or 0.0)
    df_filtrado = df_tarifas.copy()

    # ⚠️ Remove motocicleta se entregas forem em múltiplas cidades
    if cluster_cidade and cidades_entregas:
        cidades_unicas = set([c.lower() for c in cidades_entregas])
        if cluster_cidade.lower() not in cidades_unicas or len(cidades_unicas) > 1:
            df_filtrado = df_filtrado[df_filtrado['tipo_veiculo'].str.lower() != 'motocicleta']
            if logger:
                logger.debug(f"🚫 Motocicleta removida (cluster: {cluster_cidade}, entregas: {cidades_unicas})")

    # ✅ Busca o veículo de menor capacidade que comporte o peso
    df_compativel = df_filtrado[
        (df_filtrado['capacidade_kg_min'] < peso_total) &
        (df_filtrado['capacidade_kg_max'] >= peso_total)
    ].sort_values(by='capacidade_kg_max')

    if not df_compativel.empty:
        tipo = df_compativel.iloc[0]['tipo_veiculo']
        capacidade = df_compativel.iloc[0]['capacidade_kg_max']
        return tipo, capacidade

    # 🚨 Se não houver nenhum compatível, tenta retornar o menor veículo acima do peso
    df_acima = df_filtrado[df_filtrado['capacidade_kg_max'] > peso_total].sort_values(by='capacidade_kg_max')
    if not df_acima.empty:
        tipo = df_acima.iloc[0]['tipo_veiculo']
        capacidade = df_acima.iloc[0]['capacidade_kg_max']
        if logger:
            logger.warning(f"⚠️ Peso {peso_total:.2f}kg acima da faixa disponível. Atribuindo tipo: {tipo}")
        return tipo, capacidade

    # 🚫 Último fallback
    if logger:
        logger.error(f"❌ Nenhum veículo compatível com peso {peso_total:.2f}kg. Retornando 'DESCONHECIDO'")
    return "DESCONHECIDO", 0.0




def carregar_entregas_clusterizacao(db_conn, tenant_id: str, envio_data: str, logger=None) -> pd.DataFrame:

    query = f"""
        SELECT
            cte_numero,
            cte_uf,
            cte_nf,
            cte_volumes,
            cte_peso,
            cte_valor_nf,
            cte_valor_frete,
            envio_data,
            endereco_completo,
            transportadora,
            remetente_nome,
            destinatario_nome,
            destinatario_cnpj,
            destino_latitude,
            destino_longitude,
            remetente_cidade,
            remetente_uf
        FROM entregas
        WHERE tenant_id = '{tenant_id}' AND envio_data = '{envio_data}'
    """

    df = pd.read_sql(query, db_conn)

    # Renomear manualmente as colunas para latitude/longitude
    df.rename(columns={
    'destino_latitude': 'latitude',
    'destino_longitude': 'longitude'
    }, inplace=True)
    df.rename(columns={
    'destino_latitude': 'latitude',
    'destino_longitude': 'longitude'
}, inplace=True)

    if logger:
        logger.info(f"🔍 Colunas após renomear: {df.columns.tolist()}")



    # Padronização e log
    df.columns = [col.lower() for col in df.columns]
    print(f"📋 Colunas carregadas: {df.columns.tolist()}")
    print(f"🔢 Total de registros carregados: {len(df)}")

    df['cte_peso'] = pd.to_numeric(df['cte_peso'], errors='coerce').fillna(0)
    df['cte_volumes'] = pd.to_numeric(df['cte_volumes'], errors='coerce').fillna(0)
    df['cte_valor_nf'] = pd.to_numeric(df['cte_valor_nf'], errors='coerce').fillna(0)
    df['cte_valor_frete'] = pd.to_numeric(df['cte_valor_frete'], errors='coerce').fillna(0)

    return df


def carregar_hubs(db_conn, tenant_id):
    """
    Carrega todos os hubs cadastrados para o tenant.
    Retorna uma lista de dicionários com os campos: hub_id, nome, latitude, longitude.
    """
    query = """
        SELECT hub_id, nome, latitude, longitude
        FROM hubs
        WHERE tenant_id = %s
    """
    cursor = db_conn.cursor()
    cursor.execute(query, (tenant_id,))
    rows = cursor.fetchall()
    cursor.close()

    hubs = []
    for row in rows:
        hubs.append({
            'hub_id': row[0],
            'nome': row[1],
            'latitude': row[2],
            'longitude': row[3]
        })

    return hubs

def obter_veiculo_transferencia_por_peso(peso_total: float, db_conn) -> str:
    query = """
        SELECT tipo_veiculo
        FROM veiculos_transferencia
        WHERE %s BETWEEN capacidade_kg_min AND capacidade_kg_max
        LIMIT 1
    """
    cursor = db_conn.cursor()
    cursor.execute(query, (peso_total,))
    result = cursor.fetchone()
    cursor.close()

    return result[0] if result else "Desconhecido"

def obter_tarifa_km_veiculo_transferencia(tipo_veiculo: str, db_conn) -> float:
    query = """
        SELECT tarifa_km
        FROM veiculos_transferencia
        WHERE tipo_veiculo = %s
    """
    cursor = db_conn.cursor()
    cursor.execute(query, (tipo_veiculo,))
    result = cursor.fetchone()
    cursor.close()

    return float(result[0]) if result else 0.0

def listar_tarifas_last_mile(db, tenant_id: str):
    query = """
        SELECT *
        FROM veiculos_last_mile
        WHERE tenant_id = %s
        ORDER BY capacidade_kg_min
    """
    return pd.read_sql(query, db, params=(tenant_id,))


def listar_tarifas_transferencia(db, tenant_id: str):
    query = """
        SELECT *
        FROM veiculos_transferencia
        WHERE tenant_id = %s
        ORDER BY capacidade_kg_min
    """
    return pd.read_sql(query, db, params=(tenant_id,))


def obter_capacidade_veiculo(tipo_veiculo: str, db_conn) -> float:
    """
    Retorna a capacidade máxima em kg do tipo de veículo informado.
    """
    query = """
        SELECT capacidade_kg_max
        FROM veiculos_transferencia
        WHERE tipo_veiculo = %s
    """
    cursor = db_conn.cursor()
    cursor.execute(query, (tipo_veiculo,))
    result = cursor.fetchone()
    cursor.close()

    return float(result[0]) if result else 0.0

def carregar_resumo_clusters(db_conn, tenant_id: str, envio_data: str, k_clusters: int) -> pd.DataFrame:
    query = """
        SELECT * FROM resumo_clusters
        WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
    """
    return pd.read_sql(query, db_conn, params=(tenant_id, envio_data, k_clusters))

def carregar_resumo_transferencias(db_conn, tenant_id: str, envio_data: str, k_clusters: int) -> pd.DataFrame:
    query = """
        SELECT
            rota_id,
            tipo_veiculo,
            peso_total_kg,
            qde_volumes,
            distancia_parcial_km,
            tempo_parcial_min,
            distancia_total_km,
            tempo_total_min,
            qde_entregas,
            qde_clusters_rota
        FROM resumo_transferencias
        WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
        ORDER BY rota_id
    """
    return pd.read_sql(query, db_conn, params=(tenant_id, envio_data, k_clusters))

def carregar_detalhes_transferencias(db_conn, tenant_id, envio_data, k_clusters):
    query = """
        SELECT *
        FROM detalhes_transferencias
        WHERE tenant_id = %s
        AND envio_data = %s
        AND k_clusters = %s
    """
    return pd.read_sql(query, db_conn, params=(tenant_id, envio_data, k_clusters))

def carregar_entregas_clusterizadas(simulation_db, clusterization_db, tenant_id: str, envio_data: str, k_clusters: int) -> pd.DataFrame:
    """
    Carrega os dados de entregas clusterizadas e junta com coordenadas e cidade de destino.
    Necessário para roteirização com restrição de uso de motocicleta por cidade.
    """
    # 1. Clusterizações salvas (simulation_db)
    df_cluster = pd.read_sql(
        """
        SELECT *
        FROM entregas_clusterizadas
        WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
        """,
        simulation_db,
        params=(tenant_id, envio_data, k_clusters)
    )

    # 2. Coordenadas e cidade de destino (clusterization_db)
    df_coords = pd.read_sql(
        """
        SELECT cte_numero, tenant_id, envio_data, destino_latitude, destino_longitude, cte_cidade
        FROM entregas
        WHERE tenant_id = %s AND envio_data = %s
        """,
        clusterization_db,
        params=(tenant_id, envio_data)
    )

    # 3. Merge
    df = pd.merge(df_cluster, df_coords, on=["cte_numero", "tenant_id", "envio_data"], how="left")
    print("📌 Colunas após merge clusterizado:", df.columns.tolist())
    print("🔍 Amostra cte_cidade:", df["cte_cidade"].dropna().unique())

    return df


def carregar_rotas_transferencias(db_conn, tenant_id, envio_data, k_clusters):
    query = """
        SELECT rota_id, rota_completa_json
        FROM rotas_transferencias
        WHERE tenant_id = %s
        AND envio_data = %s
        AND k_clusters = %s
    """
    return pd.read_sql(query, db_conn, params=(tenant_id, envio_data, k_clusters))

def carregar_rotas_last_mile(db_conn, tenant_id, envio_data, k_clusters):
    query = """
        SELECT rota_id, cluster, ordem_entrega, cte_numero
        FROM rotas_last_mile
        WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
        ORDER BY rota_id, ordem_entrega
    """
    df = pd.read_sql(query, db_conn, params=(tenant_id, envio_data, k_clusters))
    return df

def carregar_detalhes_last_mile(simulation_db, clusterization_db, tenant_id, envio_data, k_clusters):
    query = """
        SELECT
            rota_id,
            cte_numero,
            cluster,
            ordem_entrega,
            distancia_km,
            tempo_minutos,
            tipo_veiculo,
            peso_total as cte_peso,
            volumes_total as cte_volumes,
            coordenadas_seq  -- ✅ agora incluído
        FROM rotas_last_mile
        WHERE tenant_id = %s
          AND envio_data = %s
          AND k_clusters = %s
        ORDER BY rota_id, ordem_entrega
    """
    df = pd.read_sql(query, simulation_db, params=(tenant_id, envio_data, k_clusters))

    if df.empty:
        return df

    # 🔄 Buscar coordenadas reais da tabela entregas (clusterization_db)
    df_coord = buscar_latlon_ctes(
    clusterization_db=clusterization_db,
    simulation_db=simulation_db,
    tenant_id=tenant_id,
    envio_data=envio_data,
    lista_ctes=df["cte_numero"].astype(str).unique().tolist(),
    k_clusters=k_clusters
)

    df_final = pd.merge(df, df_coord, on="cte_numero", how="left")

    return df_final


def buscar_latlon_ctes(clusterization_db, simulation_db, tenant_id, envio_data, lista_ctes, k_clusters, logger=None):
    lista_ctes = list(map(str, lista_ctes))

    query_sim = """
        SELECT
            cte_numero,
            centro_lat,
            centro_lon
        FROM entregas_clusterizadas
        WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s AND cte_numero = ANY(%s)
    """
    df_sim = pd.read_sql(query_sim, simulation_db, params=(tenant_id, envio_data, k_clusters, lista_ctes))
    df_sim.columns = df_sim.columns.str.strip()

    if logger:
        logger.info(f"📄 entregas_clusterizadas (simulation_db): {df_sim.shape[0]} linhas")
        logger.info(f"📌 df_sim.columns: {df_sim.columns.tolist()}")

    query_clu = """
    SELECT
        cte_numero,
        destino_latitude,
        destino_longitude,
        cte_cidade
    FROM entregas
    WHERE tenant_id = %s AND envio_data = %s AND cte_numero = ANY(%s)
"""

    df_clu = pd.read_sql(query_clu, clusterization_db, params=(tenant_id, envio_data, list(lista_ctes)))
    df_clu.columns = df_clu.columns.str.strip()

    if logger:
        logger.info(f"📄 entregas (clusterization_db): {df_clu.shape[0]} linhas")
        logger.info(f"📌 df_clu.columns: {df_clu.columns.tolist()}")

    df = pd.merge(df_sim, df_clu, on="cte_numero", how="inner")
    df.columns = df.columns.str.strip()

    if logger:
        logger.info(f"🔗 Após merge: {df.shape[0]} linhas | colunas: {df.columns.tolist()}")
        if not df.empty:
            logger.info(f"🧪 Amostra dict (bruto): {df.head().to_dict(orient='records')}")
            if 'centro_lat' not in df.columns:
                logger.error("🚨 ERRO: coluna 'centro_lat' ausente após merge!")
        else:
            logger.warning("⚠️ Merge resultou em DataFrame vazio — verifique correspondência de cte_numero.")

    return df

def carregar_resumo_lastmile(db_conn, tenant_id: str, envio_data: str, k_clusters: int) -> pd.DataFrame:
    query = """
        SELECT
            rota_id,
            tipo_veiculo,
            peso_total_kg,
            qde_volumes,
            distancia_total_km,
            tempo_total_min,
            distancia_parcial_km,
            tempo_parcial_min,
            qde_entregas
        FROM resumo_rotas_last_mile
        WHERE tenant_id = %s AND envio_data = %s AND k_clusters = %s
        ORDER BY rota_id
    """
    return pd.read_sql(query, db_conn, params=(tenant_id, envio_data, k_clusters))

def carregar_cluster_costs(simulation_db, tenant_id: str) -> dict:
    query = """
        SELECT limite_qtd_entregas, custo_fixo_diario, custo_variavel_por_entrega
        FROM cluster_costs
        WHERE tenant_id = %s
        LIMIT 1
    """
    cursor = simulation_db.cursor()
    cursor.execute(query, (tenant_id,))
    row = cursor.fetchone()
    if row:
        return {
            "limite_qtd_entregas": row[0],
            "custo_fixo_diario": float(row[1]),
            "custo_variavel_por_entrega": float(row[2])
        }
    else:
        raise ValueError(f"Nenhuma configuração de cluster_cost encontrada para tenant '{tenant_id}'")



__all__ = [
    "carregar_tarifas_transferencia",
    "carregar_tarifas_last_mile",
    "definir_tipo_veiculo_transferencia",
    "definir_tipo_veiculo_last_mile",
    "carregar_entregas_clusterizacao",
    "carregar_hubs",
    "obter_veiculo_transferencia_por_peso",
    "obter_tarifa_km_veiculo_transferencia",
    "listar_tarifas_last_mile",
    "listar_tarifas_transferencia",
    "obter_capacidade_veiculo",
    "carregar_resumo_clusters",
    "carregar_resumo_transferencias",
    "carregar_resumo_lastmile",
    "carregar_entregas_clusterizadas"
]


def carregar_historico_simulation(db_conn, tenant_id: str, limit: int = 10) -> pd.DataFrame:
    query = """
        SELECT job_id, status, mensagem, datas, parametros, criado_em
        FROM historico_simulation
        WHERE tenant_id = %s
        ORDER BY criado_em DESC
        LIMIT %s
    """
    return pd.read_sql(query, db_conn, params=(tenant_id, limit))











