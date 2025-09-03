#exploratory_analysis/domain/eda_pre_clusterization.py

import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import folium
import numpy as np
import glob
from pandas.tseries.offsets import BDay
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from datetime import datetime

from clusterization.config import UF_BOUNDS
from exploratory_analysis.domain.eda_avancada import executar_analises_avancadas


DIRETORIO_SAIDA = "exploratory_analysis/output"
os.makedirs(f"{DIRETORIO_SAIDA}/graficos", exist_ok=True)
os.makedirs(f"{DIRETORIO_SAIDA}/csvs", exist_ok=True)
os.makedirs(f"{DIRETORIO_SAIDA}/maps", exist_ok=True)
os.makedirs(f"{DIRETORIO_SAIDA}/relatorios", exist_ok=True)  # nova pasta

RELATORIO_CAMINHO = f"{DIRETORIO_SAIDA}/relatorios/relatorio_eda.pdf"


def _agrupar(df, granularidade):
    if granularidade == 'mensal':
        return df.groupby(df['envio_data'].dt.to_period('M').dt.to_timestamp())
    elif granularidade == 'anual':
        return df.groupby(df['envio_data'].dt.to_period('Y').dt.to_timestamp())
    else:
        return df.groupby(df['envio_data'])  # diária

def analisar_quantitativos(df, granularidade):
    df['envio_data'] = pd.to_datetime(df['envio_data'])
    
    agrupado = _agrupar(df, granularidade).agg({
        'cte_peso': 'sum',
        'cte_volumes': 'sum',
        'cte_valor_nf': 'sum',
        'cte_valor_frete': 'sum',
        'cte_numero': 'count'
    }).rename(columns={'cte_numero': 'qtd_entregas'}).reset_index()

    # Converte datas para string no formato "YYYY-MM" ou "YYYY-MM-DD"
    if granularidade == "mensal":
        agrupado["envio_data"] = agrupado["envio_data"].dt.strftime("%Y-%m")
    elif granularidade == "anual":
        agrupado["envio_data"] = agrupado["envio_data"].dt.strftime("%Y")
    else:
        agrupado["envio_data"] = agrupado["envio_data"].dt.strftime("%Y-%m-%d")

    # Gráficos individuais em subplots organizados
    fig, axs = plt.subplots(nrows=3, ncols=2, figsize=(12, 10))
    fig.suptitle("Indicadores por Período")

    colunas = ['cte_peso', 'cte_volumes', 'cte_valor_nf', 'cte_valor_frete', 'qtd_entregas']
    for i, col in enumerate(colunas):
        ax = axs[i // 2, i % 2]
        sns.barplot(data=agrupado, x='envio_data', y=col, ax=ax, color='steelblue')
        ax.set_title(col)
        ax.set_xlabel('Período')
        ax.set_ylabel('')
        ax.tick_params(axis='x', rotation=45)

    # Oculta gráfico vazio (caso só tenha 5 variáveis e 3x2 = 6 plots)
    axs[2, 1].axis('off')

    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(f"{DIRETORIO_SAIDA}/graficos/indicadores_por_periodo.png")
    plt.close()


def analisar_concentracao_fim_mes(df):
    import pandas.tseries.offsets as offsets
    from pandas.tseries.holiday import USFederalHolidayCalendar

    df['envio_data'] = pd.to_datetime(df['envio_data'])
    df['mes'] = df['envio_data'].dt.to_period('M')

    # Função auxiliar para retornar últimos 5 dias úteis do mês de uma data
    def ultimos_dias_uteis_do_mes(data):
        inicio_mes = data.replace(day=1)
        fim_mes = data + offsets.MonthEnd(0)
        dias_uteis = pd.bdate_range(inicio_mes, fim_mes)
        return set(dias_uteis[-5:])

    # Marca se a data está nos últimos 5 dias úteis
    df['fim_mes_util'] = df['envio_data'].apply(
        lambda d: d in ultimos_dias_uteis_do_mes(d)
    )

    # Agrupa por mês
    resultado = df.groupby('mes').agg(
        total_entregas=('cte_numero', 'count'),
        entregas_ultimos_5uteis=('fim_mes_util', 'sum')
    )
    resultado['entregas_resto_mes'] = resultado['total_entregas'] - resultado['entregas_ultimos_5uteis']
    resultado['percentual_ultimos_5uteis'] = (
        resultado['entregas_ultimos_5uteis'] / resultado['total_entregas'] * 100
    )

    # Transforma o índice em string para evitar erros no matplotlib
    resultado['mes_str'] = resultado.index.astype(str)
    resultado_plot = resultado[['mes_str', 'entregas_resto_mes', 'entregas_ultimos_5uteis']].set_index('mes_str')

    # Gráfico empilhado
    ax = resultado_plot.plot(
        kind='bar',
        stacked=True,
        figsize=(12, 6),
        color=['#bbbbbb', '#ff7f0e'],
        edgecolor='black'
    )

    # Percentual no topo das barras
    for i, (mes_str, row) in enumerate(resultado.iterrows()):
        total = row['total_entregas']
        pct = row['percentual_ultimos_5uteis']
        ax.text(
            x=i,
            y=total + 3,
            s=f"{pct:.1f}%",
            ha='center',
            va='bottom',
            fontsize=10,
            fontweight='bold'
        )

    plt.title("📊 Concentração de Entregas nos Últimos 5 Dias Úteis do Mês")
    plt.xlabel("Mês")
    plt.ylabel("Quantidade de Entregas")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(f"{DIRETORIO_SAIDA}/graficos/concentracao_fim_mes.png")
    plt.close()


def gerar_mapa_entregas(df, faixa_cores):
    df = df.dropna(subset=['destino_latitude', 'destino_longitude', 'cte_valor_nf'])
    mapa = folium.Map(location=[-5.2, -39.3], zoom_start=7)

    def definir_cor(valor):
        for (min_v, max_v), cor in faixa_cores.items():
            if min_v <= valor <= max_v:
                return cor
        return 'gray'

    for _, row in df.iterrows():
        valor_nf = row['cte_valor_nf'] or 0
        cor = definir_cor(valor_nf)
        folium.CircleMarker(
            location=(row['destino_latitude'], row['destino_longitude']),
            radius=min(10, valor_nf / 1000),
            color=cor,
            fill=True,
            fill_color=cor,
            fill_opacity=0.7,
            popup=f"{row['destinatario_nome']}<br>R$ {valor_nf:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        ).add_to(mapa)

    mapa.save(f"{DIRETORIO_SAIDA}/maps/mapa_entregas.html")



def maiores_frequencias(df):
    ranking = df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf']) \
                .size().reset_index(name='qtd_entregas') \
                .sort_values(by='qtd_entregas', ascending=False)
    ranking.head(20).to_csv(f"{DIRETORIO_SAIDA}/csvs/top_frequencias.csv", index=False)


def maiores_valores_nf(df):
    ranking = (
        df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf'])['cte_valor_nf']
        .sum()
        .reset_index(name='valor_total_nf')
        .sort_values(by='valor_total_nf', ascending=False)
    )
    ranking.head(20).to_csv(f"{DIRETORIO_SAIDA}/csvs/top_valores_nf.csv", index=False)

def maiores_cidades(df):
    df['cte_cidade'] = df['cte_cidade'].fillna("INDEFINIDO")
    ranking = df.groupby(['cte_cidade', 'cte_uf']).agg(
        qtd_entregas=('cte_numero', 'count'),
        valor_total_nf=('cte_valor_nf', 'sum')
    ).reset_index()

    top_cidades_entregas = ranking.sort_values(by='qtd_entregas', ascending=False).head(20)
    top_cidades_valor_nf = ranking.sort_values(by='valor_total_nf', ascending=False).head(20)

    top_cidades_entregas.to_csv(f"{DIRETORIO_SAIDA}/csvs/top_cidades_entregas.csv", index=False)
    top_cidades_valor_nf.to_csv(f"{DIRETORIO_SAIDA}/csvs/top_cidades_valor_nf.csv", index=False)


def plotar_top_frequencias(df):
    ranking = df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf']) \
                .size().reset_index(name='qtd_entregas') \
                .sort_values(by='qtd_entregas', ascending=False).head(10)

    ranking['destinatario'] = ranking['destinatario_nome'] + " - " + ranking['cte_cidade'] + "/" + ranking['cte_uf']

    plt.figure(figsize=(12, 6))
    sns.barplot(data=ranking, y='destinatario', x='qtd_entregas', palette="Blues_d")
    plt.xlabel("Quantidade de Entregas")
    plt.ylabel("Destinatário")
    plt.title("📦 Top 10 Destinatários por Frequência de Entregas")
    plt.tight_layout()
    plt.savefig(f"{DIRETORIO_SAIDA}/graficos/top_frequencias.png")
    plt.close()

def plotar_top_valores_nf(df):
    ranking = (
        df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf'])['cte_valor_nf']
        .sum()
        .reset_index(name='valor_total_nf')
        .sort_values(by='valor_total_nf', ascending=False)
        .head(10)
    )

    ranking['destinatario'] = ranking['destinatario_nome'] + " - " + ranking['cte_cidade'] + "/" + ranking['cte_uf']

    plt.figure(figsize=(12, 6))
    sns.barplot(data=ranking, y='destinatario', x='valor_total_nf', palette="Reds_d")
    plt.xlabel("Valor Total das NFs (R$)")
    plt.ylabel("Destinatário")
    plt.title("💰 Top 10 Destinatários por Valor Total de NFs no Período")
    plt.tight_layout()
    plt.savefig(f"{DIRETORIO_SAIDA}/graficos/top_valores_nf.png")
    plt.close()


def gerar_mapa_top_frequencias(df):
    ranking = df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf', 'destino_latitude', 'destino_longitude']) \
                .size().reset_index(name='qtd_entregas') \
                .sort_values(by='qtd_entregas', ascending=False).head(10).dropna()

    mapa = folium.Map(location=[-5.2, -39.3], zoom_start=7)
    for _, row in ranking.iterrows():
        folium.CircleMarker(
            location=(row['destino_latitude'], row['destino_longitude']),
            radius=min(10, row['qtd_entregas']),
            color='green',
            fill=True,
            fill_opacity=0.7,
            popup=f"{row['destinatario_nome']}<br>{row['cte_cidade']}/{row['cte_uf']}<br>{row['qtd_entregas']} entregas"
        ).add_to(mapa)
    mapa.save(f"{DIRETORIO_SAIDA}/maps/mapa_top_frequencias.html")

def gerar_mapa_top_valores_nf(df):
    ranking = (
        df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf', 'destino_latitude', 'destino_longitude'])['cte_valor_nf']
        .sum()
        .reset_index(name='valor_total_nf')
        .sort_values(by='valor_total_nf', ascending=False)
        .head(10)
        .dropna()
    )

    mapa = folium.Map(location=[-5.2, -39.3], zoom_start=7)
    for _, row in ranking.iterrows():
        valor_nf = row['valor_total_nf']
        folium.CircleMarker(
            location=(row['destino_latitude'], row['destino_longitude']),
            radius=min(10, valor_nf / 1000),
            color='purple',
            fill=True,
            fill_opacity=0.7,
            popup=f"{row['destinatario_nome']}<br>{row['cte_cidade']}/{row['cte_uf']}<br>R$ {valor_nf:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        ).add_to(mapa)
    mapa.save(f"{DIRETORIO_SAIDA}/maps/mapa_top_valores_nf.html")

def estimar_uf_por_coordenada(lat, lon):
    for uf, bounds in UF_BOUNDS.items():
        if bounds['lat_min'] <= lat <= bounds['lat_max'] and bounds['lon_min'] <= lon <= bounds['lon_max']:
            return uf
    return 'FORA_DO_BRASIL'


def avaliar_outliers(df):
    st_cols = ['cte_peso', 'cte_valor_frete', 'cte_valor_nf', 'cte_volumes']
    geo_cols = ['destino_latitude', 'destino_longitude']
    all_cols = st_cols + geo_cols

    outlier_summary = []

    for col in all_cols:
        col_data = df[col].dropna()
        if col_data.empty:
            continue

        q1 = col_data.quantile(0.25)
        q3 = col_data.quantile(0.75)
        iqr = q3 - q1
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr

        # Ajuste: força limite inferior a zero para colunas que não admitem negativos
        if col in st_cols:
            lim_inf = max(lim_inf, 0)

        outliers = df[(df[col] < lim_inf) | (df[col] > lim_sup)]
        outlier_count = len(outliers)

        # Salva CSV por coluna
        out_path = f"{DIRETORIO_SAIDA}/csvs/outliers_{col}.csv"
        outliers.to_csv(out_path, index=False)

        outlier_summary.append({
            "coluna": col,
            "total_observacoes": len(col_data),
            "outliers": outlier_count,
            "percentual": round(outlier_count / len(col_data) * 100, 2),
            "lim_inf": round(lim_inf, 4),
            "lim_sup": round(lim_sup, 4)
        })

        # Gera gráfico boxplot
        plt.figure(figsize=(8, 1.2))
        sns.boxplot(x=col_data, color='tomato')
        plt.title(f"Outliers - {col}")
        plt.tight_layout()
        plt.savefig(f"{DIRETORIO_SAIDA}/graficos/boxplot_{col}.png")
        plt.close()

    # Salvar resumo geral
    pd.DataFrame(outlier_summary).to_csv(f"{DIRETORIO_SAIDA}/csvs/resumo_outliers.csv", index=False)


def plotar_mapa_correlacao(df):
    cols = ['cte_peso', 'cte_valor_nf', 'cte_valor_frete', 'cte_volumes']
    df_corr = df[cols].dropna()
    corr = df_corr.corr()

    plt.figure(figsize=(6, 4))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", square=True)
    plt.title("🔗 Correlação entre Variáveis Numéricas")
    plt.tight_layout()
    plt.savefig(f"{DIRETORIO_SAIDA}/graficos/mapa_correlacao.png")
    plt.close()

def plotar_distribuicoes(df):
    cols = ['cte_peso', 'cte_valor_nf', 'cte_valor_frete', 'cte_volumes']
    for col in cols:
        serie = df[col].dropna()
        plt.figure(figsize=(8, 3))
        sns.histplot(serie, kde=True, bins=30, color='steelblue')
        plt.title(f"📊 Distribuição: {col}")
        plt.tight_layout()
        plt.savefig(f"{DIRETORIO_SAIDA}/graficos/distribuicao_{col}.png")
        plt.close()

def plotar_mapa_dispersao_valores(df):
    df_plot = df.dropna(subset=['destino_latitude', 'destino_longitude', 'cte_valor_nf'])
    mapa = folium.Map(location=[-5.2, -39.3], zoom_start=7)

    for _, row in df_plot.iterrows():
        folium.Circle(
            location=(row['destino_latitude'], row['destino_longitude']),
            radius=max(100, row['cte_valor_nf'] / 5),
            color='crimson',
            fill=True,
            fill_opacity=0.4,
            popup=f"{row['destinatario_nome']}<br>R$ {row['cte_valor_nf']:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        ).add_to(mapa)

    mapa.save(f"{DIRETORIO_SAIDA}/maps/mapa_dispersao_valores.html")

def limpar_graficos_antigos(diretorio):
    arquivos = glob.glob(os.path.join(diretorio, "*.png"))
    for arquivo in arquivos:
        try:
            os.remove(arquivo)
        except Exception as e:
            print(f"⚠️ Não foi possível remover {arquivo}: {e}")

def gerar_relatorio_pdf(df, incluir_outliers=False):
    from pathlib import Path

    DIRETORIO_SAIDA = "exploratory_analysis/output"
    GRAFICOS_DIR = os.path.join(DIRETORIO_SAIDA, "graficos")
    RELATORIO_CAMINHO = f"{DIRETORIO_SAIDA}/relatorios/relatorio_eda.pdf"
    os.makedirs(os.path.dirname(RELATORIO_CAMINHO), exist_ok=True)

    doc = SimpleDocTemplate(RELATORIO_CAMINHO, pagesize=A4)
    styles = getSampleStyleSheet()
    style_h1 = styles['Heading1']
    style_h2 = styles['Heading2']
    style_normal = styles['Normal']
    elements = []

    elements.append(Paragraph("📦 Relatório de Análise Exploratória (EDA)", style_h1))
    elements.append(Spacer(1, 12))

    if incluir_outliers:
        nota = "⚠️ Esta análise inclui entregas com possíveis outliers estatísticos e geográficos."
    else:
        nota = "✅ Esta análise **exclui** entregas com coordenadas fora do CE **e** outliers estatísticos (baseados em IQR ou percentis)."

    elements.append(Paragraph(nota, style_normal))
    elements.append(Spacer(1, 12))

    # Captura e ordena todos os gráficos disponíveis
    graficos_png = sorted(Path(GRAFICOS_DIR).glob("*.png"))

    if not graficos_png:
        elements.append(Paragraph("⚠️ Nenhum gráfico foi encontrado para compor o relatório.", style_normal))
    else:
        for grafico_path in graficos_png:
            nome_arquivo = grafico_path.name
            # Títulos formatados manualmente para casos conhecidos
            TITULOS_NOMEADOS = {
                "entregas_por_dia_mes.png": "📅 Concentração de Entregas por Dia do Mês",
                "concentracao_fim_mes.png": "📊 Concentração de Entregas nos Últimos 5 Dias Úteis do Mês",
                "mapa_correlacao.png": "🔗 Correlação entre Variáveis Numéricas",
            }

            nome_arquivo_str = grafico_path.name
            titulo = TITULOS_NOMEADOS.get(nome_arquivo_str, "🖼️ " + nome_arquivo_str.replace("_", " ").replace(".png", "").capitalize())

            elements.append(Paragraph(f"🖼️ {titulo}", style_h2))
            elements.append(Spacer(1, 8))
            elements.append(Image(str(grafico_path), width=480, height=280, kind='proportional'))
            elements.append(Spacer(1, 12))

    doc.build(elements)
    print(f"✅ Relatório PDF gerado com sucesso em: {RELATORIO_CAMINHO}")


def remover_outliers_estatisticos(df, colunas):
    """
    Remove outliers estatísticos usando percentis 0.05%–99.95% para todas as colunas.
    Ideal para bases com caudas longas (distribuição realista).
    """
    df_limpo = df.copy()
    resumo = []

    for col in colunas:
        if col not in df_limpo or df_limpo[col].dropna().shape[0] == 0:
            continue

        original_len = len(df_limpo)

        lim_inf = df_limpo[col].quantile(0.0005)
        lim_sup = df_limpo[col].quantile(0.9995)

        antes = len(df_limpo)
        df_limpo = df_limpo[(df_limpo[col] >= lim_inf) & (df_limpo[col] <= lim_sup)]
        depois = len(df_limpo)
        removidos = antes - depois
        perc_removido = (removidos / original_len) * 100 if original_len > 0 else 0

        resumo.append({
            "coluna": col,
            "lim_inf": round(lim_inf, 2),
            "lim_sup": round(lim_sup, 2),
            "removidos": removidos,
            "percentual_removido": round(perc_removido, 2)
        })

    # Exibe resumo no console
    print("\n📉 Resumo da remoção de outliers estatísticos (0.5%–99.5%):")
    for r in resumo:
        print(f"• {r['coluna']}: {r['removidos']} removidos ({r['percentual_removido']}%) | Limites: {r['lim_inf']} a {r['lim_sup']}")

    # Salva CSV
    path_resumo = os.path.join(DIRETORIO_SAIDA, "csvs", "resumo_outliers_aplicados.csv")
    pd.DataFrame(resumo).to_csv(path_resumo, index=False)
    print(f"📄 Resumo salvo em: {path_resumo}")

    return df_limpo

def analisar_entregas_por_dia_mes(df):
    """
    Gera gráfico e CSV com a concentração de entregas por dia do mês (1 a 31).
    """
    if 'envio_data' not in df:
        print("❌ Coluna 'envio_data' não encontrada.")
        return

    df['envio_data'] = pd.to_datetime(df['envio_data'], errors='coerce')
    df = df.dropna(subset=['envio_data'])
    df['dia_mes'] = df['envio_data'].dt.day

    contagem = df['dia_mes'].value_counts().sort_index()

    # Gráfico
    plt.figure(figsize=(10, 4))
    sns.barplot(x=contagem.index, y=contagem.values, palette="viridis")
    plt.title("📅 Concentração de Entregas por Dia do Mês")
    plt.xlabel("Dia do Mês")
    plt.ylabel("Quantidade de Entregas")
    plt.tight_layout()
    plt.savefig(f"{DIRETORIO_SAIDA}/graficos/entregas_por_dia_mes.png")
    plt.close()

    # CSV
    contagem.to_frame(name="qtd_entregas").to_csv(f"{DIRETORIO_SAIDA}/csvs/entregas_por_dia_mes.csv")
    print("✅ Análise de entregas por dia do mês gerada.")



def executar_analise(df, granularidade, faixa_cores, incluir_outliers=False):
    print("🧼 Limpando gráficos antigos...")
    limpar_graficos_antigos(f"{DIRETORIO_SAIDA}/graficos")
    
    df_com_uf = df.copy()

    # Se solicitado, salva os outliers (UF ≠ CE)
    def coordenada_em_bounds_ce(lat, lon):
        bounds = UF_BOUNDS["CE"]
        return (
            bounds["lat_min"] <= lat <= bounds["lat_max"] and
            bounds["lon_min"] <= lon <= bounds["lon_max"]
        )

    
    # Verifica se está fora do CE tanto por UF estimada quanto por coordenadas geográficas
    df_com_uf["fora_bounds_ce"] = df_com_uf.apply(
        lambda row: not coordenada_em_bounds_ce(row["destino_latitude"], row["destino_longitude"])
        if pd.notnull(row["destino_latitude"]) and pd.notnull(row["destino_longitude"])
        else True,
        axis=1
    )

    # Outliers reais = fora dos bounds do CE
    outliers = df_com_uf[df_com_uf["fora_bounds_ce"]]
    outliers_path = f"{DIRETORIO_SAIDA}/csvs/outliers_geograficos.csv"
    outliers.to_csv(outliers_path, index=False)

    print(f"⚠️ {len(outliers)} entregas fora do CE (geograficamente) salvas em: {outliers_path}")


    # Filtra apenas dentro do CE geograficamente, se não for para incluir outliers
    if not incluir_outliers:
        print("🧹 Filtrando apenas entregas geograficamente dentro do CE")
        df = df_com_uf[~df_com_uf["fora_bounds_ce"]].copy()

        print("📉 Removendo outliers estatísticos nas variáveis principais...")
        df = remover_outliers_estatisticos(
            df,
            colunas=['cte_peso', 'cte_valor_nf', 'cte_valor_frete', 'cte_volumes']
        )
    else:
        df = df_com_uf.copy()


    if granularidade == 'mensal':
        print("📈 Analisando concentração de fim de mês...")
        analisar_concentracao_fim_mes(df)

    print("🗺️ Gerando mapa...")
    gerar_mapa_entregas(df, faixa_cores)

    print("📦 Maiores frequências...")
    maiores_frequencias(df)

    print("💰 Maiores valores de NF...")
    maiores_valores_nf(df)

    print("🏙️ Maiores cidades por volume e valor...")
    maiores_cidades(df)


    plotar_top_frequencias(df)
    plotar_top_valores_nf(df)

    print("🗺️ Gerando mapas dos Top 10...")
    gerar_mapa_top_frequencias(df)
    gerar_mapa_top_valores_nf(df)

    print("📊 Gerando indicadores quantitativos por período...")
    analisar_quantitativos(df, granularidade)

    if incluir_outliers:
        print("📊 Avaliando outliers estatísticos e geográficos...")
        avaliar_outliers(df_com_uf)

    print("📌 Plotando correlação e distribuições...")
    plotar_mapa_correlacao(df)
    plotar_distribuicoes(df)
    plotar_mapa_dispersao_valores(df)

    executar_analises_avancadas(df)
    print("📅 Analisando concentração por dia do mês...")
    analisar_entregas_por_dia_mes(df)


    gerar_relatorio_pdf(df, incluir_outliers=incluir_outliers)

    # Resumo final
    # Resumo final com base apenas na validação geográfica
    total = len(df_com_uf)
    total_fora_ce = df_com_uf["fora_bounds_ce"].sum()
    total_dentro_ce = total - total_fora_ce

    perc_dentro_ce = (total_dentro_ce / total) * 100 if total > 0 else 0
    perc_fora_ce = 100 - perc_dentro_ce

    print("\n📊 Resumo da validação geográfica (limites do CE):")
    print(f"🔹 Total de entregas analisadas: {total}")
    print(f"✅ Entregas dentro do CE: {total_dentro_ce} ({perc_dentro_ce:.1f}%)")
    print(f"⚠️ Entregas fora do CE (outliers): {total_fora_ce} ({perc_fora_ce:.1f}%)")


    # Salvar o DataFrame final
    df.to_csv(f"{DIRETORIO_SAIDA}/csvs/dados_filtrados.csv", index=False)

    print("\n📁 Arquivos salvos:")
    print(f"📄 Dados filtrados: {DIRETORIO_SAIDA}/csvs/dados_filtrados.csv")
    print(f"📄 Outliers geográficos: {outliers_path}")
    print(f"📊 Relatório PDF: {RELATORIO_CAMINHO}")



