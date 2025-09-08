# hub_router_1.0.1/src/ml_pipeline/interface/costs_clients.py
import pandas as pd
import psycopg2

class CostsTransferClient:
    """
    Cliente real para custos de transferências (middle mile),
    baseado nos dados de resumo_transferencias.
    """
    def __init__(self, db_config: dict, logger=None):
        self.db_config = db_config
        self.logger = logger

    def estimate(self, df: pd.DataFrame, tenant_id: str) -> float:
        if df.empty:
            return 0.0

        start_date = str(df["data"].min())
        end_date   = str(df["data"].max())

        query = """
            SELECT COALESCE(SUM(custo_total),0) AS custo
            FROM resumo_transferencias
            WHERE tenant_id = %s
              AND envio_data BETWEEN %s AND %s
        """
        conn = psycopg2.connect(**self.db_config)
        try:
            custo_df = pd.read_sql(query, conn, params=(tenant_id, start_date, end_date))
            custo = float(custo_df.iloc[0]["custo"])
            if self.logger:
                self.logger.info(f"💰 Transferência (real): {custo:.2f} | {tenant_id} {start_date}..{end_date}")
            return custo
        finally:
            conn.close()


class CostsLastMileClient:
    """
    Cliente real para custos de last mile,
    baseado nos dados de resumo_rotas_last_mile.
    """
    def __init__(self, db_config: dict, logger=None):
        self.db_config = db_config
        self.logger = logger

    def estimate(self, df: pd.DataFrame, tenant_id: str) -> dict:
        if df.empty:
            return {"custo_total": 0.0, "rotas_df": pd.DataFrame()}

        start_date = str(df["data"].min())
        end_date   = str(df["data"].max())

        query = """
            SELECT COALESCE(SUM(custo_total),0) AS custo
            FROM resumo_rotas_last_mile
            WHERE tenant_id = %s
              AND envio_data BETWEEN %s AND %s
        """
        conn = psycopg2.connect(**self.db_config)
        try:
            custo_df = pd.read_sql(query, conn, params=(tenant_id, start_date, end_date))
            custo = float(custo_df.iloc[0]["custo"])
            if self.logger:
                self.logger.info(f"🚚 Last-mile (real): {custo:.2f} | {tenant_id} {start_date}..{end_date}")
            return {
                "custo_total": custo,
                "rotas_df": pd.DataFrame()  # podemos expandir se precisar das rotas detalhadas
            }
        finally:
            conn.close()
