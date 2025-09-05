# exploratory_analysis/streamlit_app/main_streamlit.py

import os
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
import plotly.express as px
import requests

# =========================
# Configurações
# =========================
DIRETORIO_ENTRADA = os.environ.get("EDA_OUTPUT_DIR", "/app/exploratory_analysis/output")
RELATORIO_CAMINHO = f"{DIRETORIO_ENTRADA}/relatorios/relatorio_eda.pdf"
API_GATEWAY_URL = os.environ.get("API_GATEWAY_URL", "http://api_gateway:8000")

st.set_page_config(page_title="Análise Exploratória - HubRouter", layout="wide")

# CSS customizado para esconder menu e rodapé
hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

st.title("📊 Análise Exploratória de Entregas")

# =========================
# Funções Auxiliares
# =========================

def carregar_csv(path):
    if os.path.exists(path):
        return pd.read_csv(path, encoding="utf-8")
    return pd.DataFrame()

def carregar_dados():
    return carregar_csv(f"{DIRETORIO_ENTRADA}/csvs/dados_filtrados.csv")

def executar_analise_via_api(data_inicial, data_final, token):
    url = f"{API_GATEWAY_URL}/exploratory/eda"  # 🔧 sem a barra no final
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = {
        "data_inicial": data_inicial,
        "data_final": data_final,
        "granularidade": "mensal",
        "incluir_outliers": "true"
    }

    # 🔎 Debug: log no container
    print("🌍 Chamando API Gateway:", url)
    print("📦 Params:", params)
    print("🔑 Header Authorization:", headers.get("Authorization", "NENHUM"))

    try:
        resp = requests.post(url, headers=headers, params=params, timeout=300)
        return resp.json()
    except Exception as e:
        return {"status": "erro", "mensagem": str(e)}


def exibir_imagem(img_path, legenda=None):
    if os.path.exists(img_path):
        st.image(img_path, caption=legenda, use_container_width=True)
    else:
        st.warning(f"⚠️ Arquivo não encontrado: {os.path.basename(img_path)}")

def exibir_download_relatorio():
    st.subheader("📥 Relatório PDF")
    if os.path.exists(RELATORIO_CAMINHO):
        with open(RELATORIO_CAMINHO, "rb") as file:
            st.download_button(
                label="📄 Baixar Relatório PDF",
                data=file,
                file_name="relatorio_eda.pdf",
                mime="application/pdf"
            )
    else:
        st.warning("⚠️ Relatório PDF ainda não foi gerado.")

# =========================
# Sidebar – Executar nova análise
# =========================
st.sidebar.header("⚙️ Executar Nova Análise")
data_inicial = st.sidebar.date_input("Data inicial", pd.to_datetime("2025-08-01"))
data_final = st.sidebar.date_input("Data final", pd.to_datetime("2025-08-20"))

# =========================
# Token JWT (PROD)
# =========================
token = None
if "token" in st.query_params:
    t = st.query_params.get("token")
    if isinstance(t, list):
        token = t[0]
    else:
        token = t

# 🔎 Debug: imprime no log do container
print("🔑 Token recebido via querystring:", token[:30] + "..." if token else "NENHUM TOKEN")

if not token:
    st.error("⚠️ Nenhum token JWT recebido. Acesse este dashboard via painel autenticado.")
    st.stop()

if st.sidebar.button("🚀 Rodar análise via API Gateway"):
    with st.spinner("Rodando análise, aguarde..."):
        resultado = executar_analise_via_api(str(data_inicial), str(data_final), token)

    if resultado.get("status") == "ok":
        st.sidebar.success(resultado.get("mensagem"))
        st.cache_data.clear()   # limpa cache
        st.experimental_rerun() # reinicia para atualizar tudo
    else:
        st.sidebar.error(resultado.get("mensagem"))

# =========================
# Main – Abas de exploração
# =========================
df = carregar_dados()

tabs = st.tabs([
    "📈 Visão Geral",
    "🏆 Rankings",
    "📍 Mapas",
    "📑 CSVs Auxiliares",
    "🖼️ Gráficos Prontos",
    "📊 Dashboard Interativo",
    "📥 Relatório"
])


with tabs[0]:
    st.subheader("📊 Dashboard Resumido")

    pasta_graficos = os.path.join(DIRETORIO_ENTRADA, "graficos")
    if os.path.exists(pasta_graficos):
        arquivos = sorted([f for f in os.listdir(pasta_graficos) if f.endswith(".png")])

        categorias = {
            "📦 Boxplots": [a for a in arquivos if a.startswith("boxplot")],
            "📈 Distribuições": [a for a in arquivos if a.startswith("distribuicao")],
            "🏆 Rankings": [a for a in arquivos if a.startswith("top")],
            "🗺️ Mapas e Correlações": [a for a in arquivos if "mapa" in a or "matriz" in a],
            "📊 Outros": [a for a in arquivos if not (
                a.startswith("boxplot") or
                a.startswith("distribuicao") or
                a.startswith("top") or
                "mapa" in a or "matriz" in a
            )]
        }

        for titulo, lista in categorias.items():
            if lista:
                st.markdown(f"### {titulo}")
                cols = st.columns(2)
                for i, arq in enumerate(lista):
                    caminho = os.path.join(pasta_graficos, arq)
                    with cols[i % 2]:
                        st.image(caminho, caption=arq, use_container_width=True)  # 🔧 corrigido
    else:
        st.warning("⚠️ Pasta de gráficos não encontrada.")

with tabs[1]:
    if not df.empty:
        st.subheader("Top 10 Destinatários por Frequência")
        top_freq = df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf']).size().reset_index(name='qtd_entregas')
        st.dataframe(top_freq.sort_values("qtd_entregas", ascending=False).head(10))

        st.subheader("Top 10 por Valor Total de NF")
        top_nf = df.groupby(['destinatario_nome', 'cte_cidade', 'cte_uf'])['cte_valor_nf'].sum().reset_index()
        st.dataframe(top_nf.sort_values("cte_valor_nf", ascending=False).head(10))
    else:
        st.warning("⚠️ Nenhum dado disponível.")

with tabs[2]:
    st.subheader("Mapa dos Maiores Valores de NF")
    if not df.empty and "destino_latitude" in df.columns and "destino_longitude" in df.columns:
        df_map = df.dropna(subset=["destino_latitude", "destino_longitude"]).sort_values("cte_valor_nf", ascending=False).head(50)
        m = folium.Map(location=[-5.2, -39.3], zoom_start=7)
        for _, row in df_map.iterrows():
            folium.CircleMarker(
                location=(row["destino_latitude"], row["destino_longitude"]),
                radius=5,
                color="blue",
                fill=True,
                fill_opacity=0.6,
                popup=f"{row['destinatario_nome']}<br>R$ {row['cte_valor_nf']:.2f}"
            ).add_to(m)
        st_folium(m, width=700, height=500)
    else:
        st.warning("⚠️ Dados insuficientes para o mapa.")

with tabs[3]:
    st.subheader("CSVs Auxiliares")
    pasta_csv = os.path.join(DIRETORIO_ENTRADA, "csvs")
    if os.path.exists(pasta_csv):
        arquivos = os.listdir(pasta_csv)
        arquivo = st.selectbox("Selecione um CSV", arquivos)
        if arquivo:
            df_aux = carregar_csv(os.path.join(pasta_csv, arquivo))
            if not df_aux.empty:
                st.dataframe(df_aux, use_container_width=True)
            else:
                st.info("CSV vazio ou inválido.")
    else:
        st.warning("Pasta de CSVs não encontrada.")

with tabs[4]:
    st.subheader("Gráficos Prontos")
    pasta_graficos = os.path.join(DIRETORIO_ENTRADA, "graficos")
    if os.path.exists(pasta_graficos):
        arquivos = [f for f in os.listdir(pasta_graficos) if f.endswith(".png")]
        arquivo = st.selectbox("Selecione um gráfico", arquivos)
        if arquivo:
            exibir_imagem(os.path.join(pasta_graficos, arquivo), legenda=arquivo)
    else:
        st.warning("Pasta de gráficos não encontrada.")

with tabs[6]:  # posição da nova aba
    st.subheader("📊 Dashboard Interativo com Plotly")

    if not df.empty:
        # -----------------------
        # 🔍 Filtros dinâmicos
        # -----------------------
        st.sidebar.markdown("## 🔍 Filtros do Dashboard Interativo")

        # Valores padrão para reset
        min_nf, max_nf = (0, 0)
        if "cte_valor_nf" in df.columns and not df["cte_valor_nf"].dropna().empty:
            min_nf, max_nf = float(df["cte_valor_nf"].min()), float(df["cte_valor_nf"].max())

        # Botão reset
        if st.sidebar.button("🔄 Resetar Filtros"):
            st.session_state["uf_sel"] = []
            st.session_state["cidades_sel"] = []
            st.session_state["faixa_nf"] = (min_nf, max_nf)

        # Filtro por UF
        ufs = df["cte_uf"].dropna().unique().tolist() if "cte_uf" in df.columns else []
        uf_sel = st.sidebar.multiselect(
            "Selecionar UF",
            ufs,
            default=st.session_state.get("uf_sel", ufs),
            key="uf_sel"
        )

        # Filtro por Cidade
        cidades = df["cte_cidade"].dropna().unique().tolist() if "cte_cidade" in df.columns else []
        cidades_sel = st.sidebar.multiselect(
            "Selecionar Cidade",
            cidades,
            default=st.session_state.get("cidades_sel", cidades),
            key="cidades_sel"
        )

        # Filtro por faixa de valores de NF
        if "cte_valor_nf" in df.columns:
            default_faixa = (min_nf, max_nf)
            faixa_nf = st.sidebar.slider(
                "Faixa de Valor NF",
                min_value=min_nf,
                max_value=max_nf,
                value=st.session_state.get("faixa_nf", default_faixa),
                key="faixa_nf"
            )
        else:
            faixa_nf = (0, float("inf"))

        # -----------------------
        # 📊 Aplicando filtros
        # -----------------------
        df_filtrado = df.copy()
        if "cte_uf" in df_filtrado.columns and uf_sel:
            df_filtrado = df_filtrado[df_filtrado["cte_uf"].isin(uf_sel)]
        if "cte_cidade" in df_filtrado.columns and cidades_sel:
            df_filtrado = df_filtrado[df_filtrado["cte_cidade"].isin(cidades_sel)]
        if "cte_valor_nf" in df_filtrado.columns and faixa_nf:
            df_filtrado = df_filtrado[
                (df_filtrado["cte_valor_nf"] >= faixa_nf[0]) &
                (df_filtrado["cte_valor_nf"] <= faixa_nf[1])
            ]

        # -----------------------
        # 📊 Visualizações
        # -----------------------
        if not df_filtrado.empty:
            # 1. Distribuição de valores de NF
            st.markdown("### 💰 Distribuição de Valores de NF")
            fig_valores = px.histogram(df_filtrado, x="cte_valor_nf", nbins=50, title="Distribuição do Valor das NFs")
            st.plotly_chart(fig_valores, use_container_width=True)

            # 2. Scatter Peso vs Frete
            if "cte_peso" in df_filtrado.columns and "cte_valor_frete" in df_filtrado.columns:
                st.markdown("### ⚖️ Peso x Valor do Frete")
                fig_scatter = px.scatter(
                    df_filtrado, x="cte_peso", y="cte_valor_frete",
                    color="cte_cidade" if "cte_cidade" in df_filtrado.columns else None,
                    size="cte_valor_nf" if "cte_valor_nf" in df_filtrado.columns else None,
                    hover_data=["destinatario_nome"] if "destinatario_nome" in df_filtrado.columns else None,
                    title="Peso x Valor do Frete"
                )
                st.plotly_chart(fig_scatter, use_container_width=True)

            # 3. Entregas por Cidade
            if "cte_cidade" in df_filtrado.columns:
                st.markdown("### 🏙️ Entregas por Cidade")
                entregas_cidade = df_filtrado["cte_cidade"].value_counts().reset_index()
                entregas_cidade.columns = ["Cidade", "Qtde Entregas"]
                fig_cidade = px.bar(entregas_cidade, x="Cidade", y="Qtde Entregas", title="Entregas por Cidade")
                st.plotly_chart(fig_cidade, use_container_width=True)

            # 4. Evolução temporal (se houver envio_data)
            if "envio_data" in df_filtrado.columns:
                st.markdown("### 📅 Evolução Temporal das Entregas")
                entregas_tempo = df_filtrado.groupby("envio_data").size().reset_index(name="qtd")
                fig_tempo = px.line(entregas_tempo, x="envio_data", y="qtd", title="Entregas ao longo do tempo")
                st.plotly_chart(fig_tempo, use_container_width=True)
        else:
            st.warning("⚠️ Nenhum dado encontrado após aplicar os filtros.")
    else:
        st.warning("⚠️ Nenhum dado disponível para o Dashboard Interativo.")



with tabs[5]:
    exibir_download_relatorio()
