
import psycopg2
import pandas as pd

def validar_tabelas_simulation_db(db_config):
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()

    # Buscar todas as tabelas do schema public
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE'
          AND table_name NOT LIKE 'pg_%'
          AND table_name NOT LIKE 'sql_%';
    """)
    tabelas = [row[0] for row in cursor.fetchall()]

    resultados = []

    for tabela in tabelas:
        # Contagem total
        cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
        total = cursor.fetchone()[0]

        resultado = {
            "tabela": tabela,
            "linhas": total,
            "nulos": {},
            "status": "✅ OK" if total > 0 else "⚠️ Vazia"
        }

        # Verificação de nulos nas primeiras 1000 linhas (evita tabelas muito grandes)
        if total > 0:
            try:
                df = pd.read_sql(f"SELECT * FROM {tabela} LIMIT 1000", conn)
                nulls = df.isnull().sum()
                nulls = nulls[nulls > 0]
                if not nulls.empty:
                    resultado["nulos"] = nulls.to_dict()
                    resultado["status"] = "❌ Nulos encontrados"
            except Exception as e:
                resultado["status"] = f"❌ Erro ao ler: {e}"

        resultados.append(resultado)

    cursor.close()
    conn.close()

    return resultados


if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "dbname": "simulation_db",
        "user": "postgres",
        "password": "Psilas@85",
        "port": 5432
    }

    resultados = validar_tabelas_simulation_db(db_config)
    for r in resultados:
        print(f"📋 Tabela: {r['tabela']} | Linhas: {r['linhas']} | Status: {r['status']}")
        if r['nulos']:
            print(f"   🔍 Colunas com nulos: {r['nulos']}")
