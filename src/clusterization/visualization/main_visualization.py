#clusterization/visualization/main_visualization.py

import argparse
import os
import logging
import pandas as pd

from clusterization.visualization.plot_clusterization import gerar_mapa_clusters, gerar_mapa_estatico
from clusterization.visualization.gerar_resumo_clusterizacao import gerar_graficos_resumo_clusterizacao
from clusterization.visualization.gerador_relatorio_clusterizacao import gerar_relatorio_clusterizacao
from clusterization.infrastructure.database_connection import conectar_banco

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("clusterization_visualization")


def carregar_dados_para_visualizacao(tenant_id: str, envio_data: str):
    conn = conectar_banco()

    query_entregas = """
        SELECT
            ec.cte_numero,
            e.destino_latitude,
            e.destino_longitude,
            ec.cluster,
            ec.centro_lat,
            ec.centro_lon,
            e.cte_valor_nf
        FROM entregas_clusterizadas ec
        JOIN entregas e
          ON ec.cte_numero = e.cte_numero
         AND ec.transportadora = e.transportadora
        WHERE ec.tenant_id = %s AND ec.envio_data = %s
    """

    query_resumo = """
        SELECT
            cluster,
            quantidade_entregas,
            peso_total_kg,
            quantidade_volumes,
            cte_valor_nf_total,
            cte_valor_frete_total,
            centro_lat,
            centro_lon
        FROM resumo_clusterizacao
        WHERE tenant_id = %s AND envio_data = %s
    """

    df_clusterizado = pd.read_sql(query_entregas, conn, params=(tenant_id, envio_data))
    df_resumo = pd.read_sql(query_resumo, conn, params=(tenant_id, envio_data))
    conn.close()
    return df_clusterizado, df_resumo


def main():
    parser = argparse.ArgumentParser(description="📊 Geração de visualizações da clusterização")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data", required=True, help="Data de envio (YYYY-MM-DD)")
    parser.add_argument("--modo-forcar", action="store_true", help="Sobrescreve relatórios existentes")
    parser.add_argument(
        "--formato",
        choices=["pdf", "csv", "json", "all"],
        default="all",
        help="Formato de saída desejado (default: all)"
    )

    args = parser.parse_args()
    tenant_id = args.tenant
    envio_data = args.data
    modo_forcar = args.modo_forcar
    formato = args.formato

    # 📂 Estrutura padronizada (usando volume /app/output)
    BASE_OUTPUT_DIR = "/app/output"
    tenant_base = os.path.join(BASE_OUTPUT_DIR, tenant_id)


    maps_dir = os.path.join(tenant_base, "maps")
    graphs_dir = os.path.join(tenant_base, "graphs")
    relatorios_dir = os.path.join(tenant_base, "relatorios")

    os.makedirs(maps_dir, exist_ok=True)
    os.makedirs(graphs_dir, exist_ok=True)
    os.makedirs(relatorios_dir, exist_ok=True)

    logger.info(f"🔎 Carregando dados para tenant '{tenant_id}' e data '{envio_data}'...")
    df_clusterizado, df_resumo = carregar_dados_para_visualizacao(tenant_id, envio_data)

    if df_clusterizado.empty or df_resumo.empty:
        logger.warning("⚠ Nenhum dado encontrado para visualização.")
        return

    # Arquivo PDF final
    caminho_pdf = os.path.join(relatorios_dir, f"clusterizacao_{envio_data}.pdf")
    if os.path.exists(caminho_pdf) and not modo_forcar:
        logger.info(f"⚠ Já existe relatório em {caminho_pdf}. Use --modo-forcar para sobrescrever.")
        return

    logger.info("🗺️ Gerando mapa interativo...")
    caminho_mapa = gerar_mapa_clusters(df_clusterizado, envio_data, tenant_id, output_path=maps_dir)

    logger.info("🖼️ Gerando mapa estático (PNG)...")
    caminho_mapa_png = gerar_mapa_estatico(df_clusterizado, envio_data, tenant_id, output_path=maps_dir)

    logger.info("📊 Gerando gráficos de resumo...")
    caminhos_graficos = gerar_graficos_resumo_clusterizacao(df_resumo, envio_data, tenant_id, output_path=graphs_dir)

    if formato in ("pdf", "all"):
        logger.info("📄 Gerando relatório PDF...")
        gerar_relatorio_clusterizacao(
            caminho_mapa_html=caminho_mapa,
            caminhos_graficos=caminhos_graficos,
            envio_data=envio_data,
            tenant_id=tenant_id,
            df_resumo=df_resumo,
            output_path=relatorios_dir,
            caminho_mapa_png=caminho_mapa_png,
        )

    logger.info("✅ Visualização finalizada com sucesso.")
    logger.info(f"📂 Arquivos disponíveis em: {tenant_base}")


if __name__ == "__main__":
    main()
