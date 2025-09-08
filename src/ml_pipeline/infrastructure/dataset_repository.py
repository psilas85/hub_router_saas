#hub_router_1.0.1/src/ml_pipeline/infrastructure/dataset_repository.py

import os
import psycopg2
import pandas as pd


class DatasetRepository:
    def __init__(self, db_config: dict):
        """
        db_config deve conter:
        {
            "host": ...,
            "port": ...,
            "dbname": ...,
            "user": ...,
            "password": ...
        }
        """
        self.db_config = db_config
        # 🔑 tenant default vem do .env
        self.default_tenant_id = os.getenv("DEFAULT_TENANT_ID", "default-tenant")

    def _get_connection(self):
        """Cria e retorna conexão com o banco simulation_db."""
        return psycopg2.connect(**self.db_config)

    def load_simulation_dataset(
        self,
        start_date: str,
        end_date: str,
        tenant_id: str = None
    ) -> pd.DataFrame:
        """
        Carrega dataset de simulação no intervalo de datas informado,
        filtrando sempre por tenant_id (explícito ou default).
        Retorna uma linha por simulation_id + k_clusters.
        """
        tenant_id = tenant_id or self.default_tenant_id

        query = """
            SELECT
                r.simulation_id,
                r.tenant_id,
                r.envio_data,
                r.k_clusters,
                COALESCE(r.custo_total, 0)                 AS custo_total,
                COALESCE(CAST(r.is_ponto_otimo AS INT), 0) AS is_ponto_otimo,
                COALESCE(r.quantidade_entregas, 0)         AS total_entregas,
                -- custos diretos da simulação
                COALESCE(r.custo_transferencia, 0)         AS custo_transfer_total,

                -- agregações de clusters
                COALESCE(SUM(c.peso_total_kg), 0)          AS total_peso,
                COALESCE(SUM(c.volumes_total), 0)          AS total_volumes,
                COALESCE(SUM(c.valor_total_nf), 0)         AS valor_total_nf,
                COALESCE(COUNT(c.cluster), 0)              AS qtd_clusters,

                -- agregações de rotas last mile
                COALESCE(SUM(l.distancia_total_km), 0)     AS total_distancia_lastmile_km,
                COALESCE(SUM(l.tempo_total_min), 0)        AS total_tempo_lastmile_min,
                COALESCE(SUM(l.peso_total_kg), 0)          AS peso_lastmile,
                COALESCE(SUM(l.qde_entregas), 0)           AS entregas_lastmile,
                COALESCE(SUM(l.qde_volumes), 0)            AS volumes_lastmile

            FROM public.resultados_simulacao r
            LEFT JOIN public.resumo_clusters c
                ON c.simulation_id = r.simulation_id
                AND c.tenant_id = r.tenant_id
                AND c.k_clusters = r.k_clusters
            LEFT JOIN public.resumo_rotas_last_mile l
                ON l.simulation_id = r.simulation_id
                AND l.tenant_id = r.tenant_id
                AND l.k_clusters = r.k_clusters
            WHERE r.envio_data BETWEEN %s AND %s
              AND r.tenant_id = %s
            GROUP BY
                r.simulation_id, r.tenant_id, r.envio_data, r.k_clusters,
                r.custo_total, r.is_ponto_otimo, r.quantidade_entregas,
                r.custo_transferencia
            ORDER BY r.envio_data, r.simulation_id;
        """

        conn = None
        try:
            conn = self._get_connection()
            df = pd.read_sql(query, conn, params=(start_date, end_date, tenant_id))

            # 🔎 DEBUG extra
            print(f"✅ DEBUG SQL retornou shape={df.shape} | tenant={tenant_id} | período={start_date}..{end_date}")
            if not df.empty:
                print(f"🔎 Colunas: {list(df.columns)}")
                print("🔎 Primeiras linhas:\n", df.head())
            else:
                print("⚠️ Nenhum registro encontrado no banco para este filtro.")

            return df

        except Exception as e:
            raise RuntimeError(f"Erro ao carregar dataset de simulação: {str(e)}")
        finally:
            if conn:
                conn.close()

    def load_city_daily_history(self, tenant_id: str) -> pd.DataFrame:
        """
        Histórico diário agregado por cidade (entregas, peso, volumes, valor_nf).
        Esta função acessa o banco 'clusterization_db' em vez do 'simulation_db'.
        """
        q = """
        SELECT
          envio_data AS data,
          cte_cidade AS cidade,
          cte_uf     AS uf,
          COUNT(*)                         AS entregas,
          COALESCE(SUM(cte_peso),0)        AS peso,
          COALESCE(SUM(cte_volumes),0)     AS volumes,
          COALESCE(SUM(cte_valor_nf),0)    AS valor_nf
        FROM public.entregas
        WHERE tenant_id = %s
        GROUP BY envio_data, cte_cidade, cte_uf
        """

        cluster_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "dbname": "clusterization_db",   # 👈 fixo para entregas
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }

        conn = psycopg2.connect(**cluster_config)
        try:
            return pd.read_sql(q, conn, params=(tenant_id,))
        finally:
            conn.close()

    def load_avg_cost_per_delivery(self, start_date: str, end_date: str, tenant_id: str) -> float:
        """
        Retorna o custo médio por entrega no período informado,
        calculado a partir da tabela resultados_simulacao.
        """
        query = """
            SELECT AVG(custo_total::numeric / NULLIF(quantidade_entregas, 0)) AS custo_medio_entrega
            FROM resultados_simulacao
            WHERE envio_data BETWEEN %s AND %s
              AND tenant_id = %s
        """

        with self._get_connection() as conn:
            df = pd.read_sql(query, conn, params=(start_date, end_date, tenant_id))

        if df.empty or df["custo_medio_entrega"].isna().all():
            return None

        return float(df.iloc[0]["custo_medio_entrega"])

    def load_monthly_real_totals(self, start_date: str, end_date: str, tenant_id: str) -> pd.DataFrame:
        """
        Retorna totais reais mensais de entregas, peso e volumes
        da tabela 'entregas' no clusterization_db.
        """
        query = """
            SELECT DATE_TRUNC('month', envio_data) AS mes,
                COUNT(*) AS entregas,
                COALESCE(SUM(cte_peso),0) AS peso,
                COALESCE(SUM(cte_volumes),0) AS volumes
            FROM public.entregas
            WHERE envio_data BETWEEN %s AND %s
            AND tenant_id = %s
            GROUP BY 1
            ORDER BY 1;
        """

        cluster_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": os.getenv("POSTGRES_PORT", "5432"),
            "dbname": "clusterization_db",   # 👈 banco das entregas
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }

        with psycopg2.connect(**cluster_config) as conn:
            return pd.read_sql(query, conn, params=(start_date, end_date, tenant_id))
