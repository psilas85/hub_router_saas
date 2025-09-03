# exloratory_analysis/domain/eda_avancada.py

import os
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from folium import Map, Circle
import folium
from pandas.api.types import is_numeric_dtype

# Caminho absoluto para evitar erros de diretório
BASE_DIR = os.path.abspath("exploratory_analysis/output")
GRAFICOS_DIR = os.path.join(BASE_DIR, "graficos")
CSVS_DIR = os.path.join(BASE_DIR, "csvs")
MAPS_DIR = os.path.join(BASE_DIR, "maps")

os.makedirs(GRAFICOS_DIR, exist_ok=True)
os.makedirs(CSVS_DIR, exist_ok=True)
os.makedirs(MAPS_DIR, exist_ok=True)

def salvar_grafico(fig_path):
    try:
        plt.tight_layout()
        plt.savefig(fig_path)
        print(f"✅ Gráfico salvo: {fig_path}")
    except Exception as e:
        print(f"⚠️ Erro ao salvar gráfico {fig_path}: {e}")
    finally:
        plt.close()

# 1. Volume por entrega
def plotar_distribuicao_volumes(df):
    if 'cte_volumes' in df and df['cte_volumes'].dropna().shape[0] > 0:
        plt.figure(figsize=(8, 3))
        sns.histplot(df['cte_volumes'].dropna(), kde=True, bins=30)
        plt.title("Distribuição de Volumes por Entrega")
        salvar_grafico(os.path.join(GRAFICOS_DIR, "distribuicao_volumes.png"))
    else:
        print("❌ Dados insuficientes para plotar distribuição de volumes.")

# 2. Fluxo Origem-Destino
def gerar_matriz_fluxo_origem_destino(df):
    if 'remetente_cidade' in df and 'cte_cidade' in df:
        fluxo = df.groupby(['remetente_cidade', 'cte_cidade']).size().unstack(fill_value=0)
        if fluxo.shape[0] > 0:
            plt.figure(figsize=(10, 6))
            sns.heatmap(fluxo, cmap='Blues', linewidths=0.5)
            plt.title("Matriz Origem x Destino (Quantidade de Entregas)")
            plt.xlabel("Destino")
            plt.ylabel("Origem")
            salvar_grafico(os.path.join(GRAFICOS_DIR, "matriz_origem_destino.png"))
        else:
            print("❌ Matriz de fluxo está vazia.")
    else:
        print("❌ Colunas de origem ou destino não estão no DataFrame.")

# 3. Entregas por dia da semana
def plotar_entregas_por_dia_semana(df):
    if 'envio_data' in df:
        df['dia_semana'] = pd.to_datetime(df['envio_data'], errors='coerce').dt.day_name()
        ordem_dias = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        contagem = df['dia_semana'].value_counts().reindex(ordem_dias)
        contagem.plot(kind='bar', color='teal', edgecolor='black')
        plt.title("Entregas por Dia da Semana")
        plt.ylabel("Quantidade")
        plt.xlabel("Dia da Semana")
        salvar_grafico(os.path.join(GRAFICOS_DIR, "entregas_por_dia_semana.png"))
    else:
        print("❌ Coluna envio_data não está disponível.")

# 4. Frete sobre NF
def plotar_frete_sobre_nf(df):
    if 'cte_valor_nf' in df and 'cte_valor_frete' in df:
        df_valid = df[(df['cte_valor_nf'] > 0) & (df['cte_valor_frete'] > 0)].copy()
        if df_valid.shape[0] > 0:
            df_valid['frete_sobre_nf'] = df_valid['cte_valor_frete'] / df_valid['cte_valor_nf']
            plt.figure(figsize=(8, 3))
            sns.histplot(df_valid['frete_sobre_nf'], kde=True, bins=50, color='orange')
            plt.title("Distribuição da Razão Frete / Valor NF")
            salvar_grafico(os.path.join(GRAFICOS_DIR, "frete_sobre_nf.png"))
        else:
            print("❌ Nenhum registro válido para frete sobre NF.")
    else:
        print("❌ Colunas cte_valor_nf ou cte_valor_frete ausentes.")

# 5. Valores zerados ou nulos
def avaliar_valores_zerados(df):
    colunas = ['cte_valor_nf', 'cte_valor_frete', 'cte_peso', 'cte_volumes']
    resumo = []
    for col in colunas:
        if col in df:
            total = len(df)
            zerados = len(df[df[col] == 0])
            nulos = df[col].isna().sum()
            resumo.append({
                'coluna': col,
                'zerados': zerados,
                'nulos': nulos,
                'total': total,
                'pct_zerados': round(zerados / total * 100, 2),
                'pct_nulos': round(nulos / total * 100, 2)
            })
    if resumo:
        pd.DataFrame(resumo).to_csv(os.path.join(CSVS_DIR, "valores_zerados_nulos.csv"), index=False)
        print("✅ Resumo de valores zerados e nulos salvo.")
    else:
        print("❌ Nenhuma coluna para avaliar valores zerados ou nulos.")

# 6. Binning faixas de valor NF
def plotar_faixas_valor_nf(df):
    if 'cte_valor_nf' in df:
        bins = [0, 500, 2000, 10000, np.inf]
        labels = ['<=500', '500–2K', '2K–10K', '>10K']
        df['faixa_valor_nf'] = pd.cut(df['cte_valor_nf'], bins=bins, labels=labels)
        faixa_counts = df['faixa_valor_nf'].value_counts().reindex(labels)
        faixa_counts.plot(kind='bar', color='darkgreen', edgecolor='black')
        plt.title("Distribuição por Faixa de Valor da NF")
        plt.xlabel("Faixa de Valor")
        plt.ylabel("Entregas")
        salvar_grafico(os.path.join(GRAFICOS_DIR, "faixas_valor_nf.png"))
    else:
        print("❌ Coluna cte_valor_nf não encontrada.")

# 7. Chamada principal do módulo
def executar_analises_avancadas(df):
    print("🔍 Executando análises exploratórias adicionais...")
    plotar_distribuicao_volumes(df)
    gerar_matriz_fluxo_origem_destino(df)
    plotar_entregas_por_dia_semana(df)
    plotar_frete_sobre_nf(df)
    avaliar_valores_zerados(df)
    plotar_faixas_valor_nf(df)
    print("✅ Análises avançadas concluídas.")
