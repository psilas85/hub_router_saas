#last_mile_routing/infrastructure/database_reader.py

import pandas as pd
from last_mile_routing.infrastructure.database_connection import (
    conectar_banco_cluster,
    conectar_banco_routing,
    fechar_conexao
)


def buscar_entregas_clusterizadas(tenant_id, envio_data):
    conn = conectar_banco_cluster()
    try:
        query = """
            SELECT *
            FROM entregas_clusterizadas
            WHERE tenant_id = %s
              AND envio_data = %s
        """
        df = pd.read_sql(query, conn, params=(tenant_id, envio_data))
        return df
    finally:
        fechar_conexao(conn)


def buscar_dados_entregas(tenant_id, envio_data):
    conn = conectar_banco_cluster()
    try:
        query = """
            SELECT *
            FROM entregas
            WHERE tenant_id = %s
              AND envio_data = %s
        """
        df = pd.read_sql(query, conn, params=(tenant_id, envio_data))
        return df
    finally:
        fechar_conexao(conn)


def buscar_custos_veiculos(tenant_id):
    conn = conectar_banco_routing()
    try:
        query = """
            SELECT veiculo, peso_minimo_kg, peso_maximo_kg
            FROM custos_entrega
            WHERE tenant_id = %s
        """
        df = pd.read_sql(query, conn, params=(tenant_id,))
        return df.to_dict(orient="records")
    finally:
        fechar_conexao(conn)

