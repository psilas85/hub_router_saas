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


def carregar_entregas_clusterizadas_para_excel(tenant_id: str, envio_data: str):
    conn = conectar_banco()
    query = """
        SELECT
            e.*,
            ec.cluster,
            ec.cluster_cidade,
            ec.centro_lat,
            ec.centro_lon,
            ec.destino_latitude AS cluster_destino_latitude,
            ec.destino_longitude AS cluster_destino_longitude,
            ec.data_hora AS clusterizacao_data_hora
        FROM entregas_clusterizadas ec
        JOIN entregas e
          ON ec.cte_numero = e.cte_numero
         AND ec.transportadora = e.transportadora
         AND ec.tenant_id = e.tenant_id
         AND e.envio_data::date = ec.envio_data
        WHERE ec.tenant_id = %s
          AND ec.envio_data = %s
        ORDER BY ec.cluster, e.cte_numero
    """
    df_excel = pd.read_sql(query, conn, params=(tenant_id, envio_data))
    conn.close()
    return df_excel


def gerar_excel_entregas_clusterizadas(
    df_entregas: pd.DataFrame,
    envio_data: str,
    tenant_id: str,
    output_path: str,
) -> str:
    os.makedirs(output_path, exist_ok=True)
    caminho_excel = os.path.join(output_path, f"entregas_clusterizadas_{envio_data}.xlsx")

    df_export = df_entregas.copy()
    colunas_prioritarias = [
        "envio_data",
        "cte_numero",
        "transportadora",
        "cluster",
        "cluster_cidade",
        "centro_lat",
        "centro_lon",
        "destino_latitude",
        "destino_longitude",
        "clusterizacao_data_hora",
    ]
    colunas_ordenadas = [
        coluna for coluna in colunas_prioritarias if coluna in df_export.columns
    ] + [
        coluna for coluna in df_export.columns if coluna not in colunas_prioritarias
    ]
    df_export = df_export[colunas_ordenadas]

    for coluna in df_export.select_dtypes(include=["datetimetz"]).columns:
        df_export[coluna] = df_export[coluna].dt.tz_localize(None)

    with pd.ExcelWriter(caminho_excel, engine="openpyxl") as writer:
        df_export.to_excel(writer, index=False, sheet_name="Entregas clusterizadas")
        worksheet = writer.sheets["Entregas clusterizadas"]
        worksheet.freeze_panes = "A2"
        worksheet.auto_filter.ref = worksheet.dimensions

        for column_cells in worksheet.columns:
            header = str(column_cells[0].value or "")
            max_length = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in column_cells[:200]
            )
            worksheet.column_dimensions[column_cells[0].column_letter].width = min(
                max(max_length + 2, len(header) + 2),
                42,
            )

    logger.info(
        "✅ XLSX de entregas clusterizadas gerado: %s (tenant=%s, linhas=%s)",
        caminho_excel,
        tenant_id,
        len(df_export),
    )
    return caminho_excel


def main():
    parser = argparse.ArgumentParser(description="📊 Geração de visualizações da clusterização")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data", required=True, help="Data de envio (YYYY-MM-DD)")
    # (mantido por compatibilidade, porém ignorado)
    parser.add_argument("--modo-forcar", action="store_true", help="(Ignorado) Agora sempre sobrescreve")
    parser.add_argument(
        "--formato",
        choices=["pdf", "csv", "json", "all"],
        default="all",
        help="Formato de saída desejado (default: all)"
    )

    args = parser.parse_args()
    tenant_id = args.tenant
    envio_data = args.data
    modo_forcar = True  # 🔒 sempre sobrescreve
    formato = args.formato

    BASE_OUTPUT_DIR = "/app/output"
    tenant_base = os.path.join(BASE_OUTPUT_DIR, tenant_id)

    maps_dir = os.path.join(tenant_base, "maps")
    graphs_dir = os.path.join(tenant_base, "graphs")
    relatorios_dir = os.path.join(tenant_base, "relatorios")
    planilhas_dir = os.path.join(tenant_base, "planilhas")

    os.makedirs(maps_dir, exist_ok=True)
    os.makedirs(graphs_dir, exist_ok=True)
    os.makedirs(relatorios_dir, exist_ok=True)
    os.makedirs(planilhas_dir, exist_ok=True)


    logger.info(f"🔎 Carregando dados para tenant '{tenant_id}' e data '{envio_data}'...")
    df_clusterizado, df_resumo = carregar_dados_para_visualizacao(tenant_id, envio_data)
    df_excel = carregar_entregas_clusterizadas_para_excel(tenant_id, envio_data)

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

    if formato in ("csv", "all"):
        logger.info("📗 Gerando XLSX de entregas clusterizadas...")
        gerar_excel_entregas_clusterizadas(df_excel, envio_data, tenant_id, output_path=planilhas_dir)

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
