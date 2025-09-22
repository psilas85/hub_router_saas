import os
import psycopg2
from psycopg2.extras import RealDictCursor

DB_HOST = os.getenv("DB_HOST", "postgres_db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "clusterization_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "Psilas@85")

TENANT = "38ed0dcc-a92b-4c07-85e7-b59e5939b84c"

def main():
    try:
        print(f"🔗 Conectando em {DB_HOST}:{DB_PORT}/{DB_NAME} ...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = """
            SELECT id, tenant_id, job_id, arquivo, status,
                   total_processados, validos, invalidos, mensagem, criado_em
            FROM historico_data_input
            WHERE tenant_id = %s
            ORDER BY criado_em DESC
            LIMIT 10
        """
        cur.execute(query, (TENANT,))
        rows = cur.fetchall()

        print("📜 Resultado:")
        for r in rows:
            print(r)

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Erro ao consultar banco: {e}")

if __name__ == "__main__":
    main()
