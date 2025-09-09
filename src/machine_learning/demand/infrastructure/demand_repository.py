#hub_router_1.0.1/src/machine_learning/demand/infrastructure/demand_repository.py

import os
from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

class DemandRepository:
    def __init__(self, env_path: str = None):
        # Carrega variáveis de ambiente do arquivo .env
        if env_path:
            load_dotenv(env_path)
        else:
            load_dotenv()  # tenta o .env padrão da pasta

        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_pass = quote_plus(os.getenv("POSTGRES_PASSWORD", "postgres"))
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB", "clusterization_db")

        conn_str = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        self.engine = create_engine(conn_str)

    def fetch_daily_city(self, tenant_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Busca dados agregados de entregas no grão diário × cidade.
        Se tenant_id=None, ignora filtro de tenant.
        """
        if tenant_id:
            query = """
            SELECT
                envio_data::date AS dt,
                COALESCE(NULLIF(TRIM(cte_cidade), ''), NULLIF(TRIM(remetente_cidade), '')) AS cidade,
                cte_uf AS uf,
                COUNT(*) AS entregas,
                SUM(cte_volumes) AS volumes_total,
                SUM(cte_peso) AS peso_total
            FROM public.entregas
            WHERE tenant_id = %(tenant_id)s
              AND envio_data BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY envio_data::date,
                     COALESCE(NULLIF(TRIM(cte_cidade), ''), NULLIF(TRIM(remetente_cidade), '')),
                     cte_uf
            ORDER BY dt, cidade
            """
            params = {"tenant_id": tenant_id, "start_date": start_date, "end_date": end_date}
        else:
            query = """
            SELECT
                envio_data::date AS dt,
                COALESCE(NULLIF(TRIM(cte_cidade), ''), NULLIF(TRIM(remetente_cidade), '')) AS cidade,
                cte_uf AS uf,
                COUNT(*) AS entregas,
                SUM(cte_volumes) AS volumes_total,
                SUM(cte_peso) AS peso_total
            FROM public.entregas
            WHERE envio_data BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY envio_data::date,
                     COALESCE(NULLIF(TRIM(cte_cidade), ''), NULLIF(TRIM(remetente_cidade), '')),
                     cte_uf
            ORDER BY dt, cidade
            """
            params = {"start_date": start_date, "end_date": end_date}

        return pd.read_sql(query, self.engine, params=params)
