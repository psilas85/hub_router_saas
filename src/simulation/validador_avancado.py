
import psycopg2
import pandas as pd

def conectar_db():
    return psycopg2.connect(
        host="localhost", dbname="simulation_db",
        user="postgres", password="Psilas@85", port=5432
    )

def validar_tabelas_criticas(conn):
    tabelas = [
        'resumo_transferencias', 'rotas_last_mile', 'resumo_clusters',
        'detalhes_rotas', 'entregas_clusterizadas', 'detalhes_transferencias',
        'rotas_transferencias'
    ]
    resultados = []
    for tabela in tabelas:
        try:
            df = pd.read_sql(f"SELECT * FROM {tabela}", conn)
            nulos = df.isnull().sum()
            total = len(df)
            col_nulos = nulos[nulos > 0].to_dict()
            status = "✅ OK" if total > 0 and not col_nulos else (
                     "⚠️ Vazia" if total == 0 else "❌ Nulos encontrados")
            resultados.append({
                "tabela": tabela,
                "linhas": total,
                "colunas_com_nulos": col_nulos,
                "status": status
            })
        except Exception as e:
            resultados.append({
                "tabela": tabela,
                "linhas": 0,
                "colunas_com_nulos": {},
                "status": f"❌ Erro: {e}"
            })
    return pd.DataFrame(resultados)

def checar_cruzamentos(df_entregas, conn):
    problemas = []

    # entregas_clusterizadas
    clu = pd.read_sql("SELECT cte_numero FROM entregas_clusterizadas", conn)
    faltando = set(df_entregas['cte_numero']) - set(clu['cte_numero'])
    if faltando:
        problemas.append(f"❌ entregas_clusterizadas ausentes para {len(faltando)} CTEs")

    # detalhes_transferencias
    trf = pd.read_sql("SELECT cte_numero FROM detalhes_transferencias", conn)
    faltando = set(df_entregas['cte_numero']) - set(trf['cte_numero'])
    if faltando:
        problemas.append(f"❌ detalhes_transferencias ausentes para {len(faltando)} CTEs")

    # detalhes_rotas
    try:
        rot = pd.read_sql("SELECT cte_numero FROM detalhes_rotas", conn)
        faltando = set(df_entregas['cte_numero']) - set(rot['cte_numero'])
        if faltando:
            problemas.append(f"❌ detalhes_rotas ausentes para {len(faltando)} CTEs")
    except:
        problemas.append("⚠️ detalhes_rotas inexistente ou sem coluna cte_numero")

    return problemas

def validar_coordenadas(df_entregas):
    invalidos = df_entregas[
        (df_entregas['destino_latitude'].isnull()) |
        (df_entregas['destino_longitude'].isnull()) |
        (df_entregas['destino_latitude'] == 0) |
        (df_entregas['destino_longitude'] == 0)
    ]
    return len(invalidos)

def validar_valores_negativos(df_entregas):
    problemas = []
    for col in ['cte_peso', 'cte_volumes', 'cte_valor_nf', 'cte_valor_frete']:
        if (df_entregas[col] < 0).any():
            problemas.append(f"❌ Valores negativos encontrados em {col}")
    return problemas

def executar_validacao_completa(path_csv_entregas):
    conn = conectar_db()
    df_entregas = pd.read_csv(path_csv_entregas)

    print("📋 Validando tabelas críticas...")
    df_tabelas = validar_tabelas_criticas(conn)
    print(df_tabelas.to_string(index=False))

    print("\n🔄 Validando cruzamentos...")
    cruzamentos = checar_cruzamentos(df_entregas, conn)
    for aviso in cruzamentos:
        print(aviso)

    print("\n📍 Validando coordenadas...")
    qtd_invalidas = validar_coordenadas(df_entregas)
    print(f"❌ {qtd_invalidas} entregas com coordenadas inválidas")

    print("\n💰 Validando campos numéricos...")
    negativos = validar_valores_negativos(df_entregas)
    for erro in negativos:
        print(erro)

    df_tabelas.to_csv("relatorio_validacao.csv", index=False)
    print("\n📁 Relatório salvo em relatorio_validacao.csv")
    conn.close()

if __name__ == "__main__":
    executar_validacao_completa("entregas_2025-03-31_a_2025-04-04.csv")
