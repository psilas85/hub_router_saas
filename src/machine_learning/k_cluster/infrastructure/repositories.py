# hub_router_1.0.1/src/machine_learning/k_cluster/infrastructure/repositories.py

from __future__ import annotations
import os
from typing import Optional
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Conexão simples via ENV (mesma convenção usada nos outros módulos)
# Ex.: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME (simulation_db)
def _connect():
    conn = psycopg2.connect(
        host=os.getenv("SIMULATION_HOST", os.getenv("POSTGRES_HOST", "localhost")),
        port=int(os.getenv("SIMULATION_PORT", os.getenv("POSTGRES_PORT", "5432"))),
        user=os.getenv("SIMULATION_USER", os.getenv("POSTGRES_USER", "postgres")),
        password=os.getenv("SIMULATION_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")),
        dbname=os.getenv("SIMULATION_DB", "simulation_db"),
    )
    return conn



def _date_clause(start: Optional[str], end: Optional[str]) -> str:
    clauses = []
    if start:
        clauses.append(f"envio_data >= DATE '{start}'")
    if end:
        clauses.append(f"envio_data <= DATE '{end}'")
    return (" AND ".join(clauses)) if clauses else "TRUE"


class PgResultsRepository:
    """Lê as três tabelas base diretamente do Postgres."""

    def load_resultados_simulacao(self, tenant_id: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        sql = f"""
            SELECT *
            FROM resultados_simulacao
            WHERE tenant_id = %s
              AND {_date_clause(start, end)}
            ORDER BY envio_data, k_clusters
        """
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (tenant_id,))
            rows = cur.fetchall()
        return pd.DataFrame(rows)

    def load_resumo_clusters(self, tenant_id: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        sql = f"""
            SELECT *
            FROM resumo_clusters
            WHERE tenant_id = %s
              AND {_date_clause(start, end)}
            ORDER BY envio_data, k_clusters, cluster
        """
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (tenant_id,))
            rows = cur.fetchall()
        return pd.DataFrame(rows)

    def load_entregas_clusterizadas(self, tenant_id: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        sql = f"""
            SELECT *
            FROM entregas_clusterizadas
            WHERE tenant_id = %s
              AND {_date_clause(start, end)}
            ORDER BY envio_data, cluster
        """
        with _connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (tenant_id,))
            rows = cur.fetchall()
        return pd.DataFrame(rows)
