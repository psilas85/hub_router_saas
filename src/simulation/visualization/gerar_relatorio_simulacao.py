#visualization/gerar_relatorio_simulacao.py

import os
import pandas as pd
import matplotlib.pyplot as plt

from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.pdfgen.canvas import Canvas
from reportlab.lib.units import cm
from reportlab.graphics.shapes import Drawing, Line

from simulation.visualization.gerar_resumo_clusterizacao import gerar_resumo_clusterizacao
from simulation.utils.criar_tabela_resumo_pdf import criar_tabela_resumo_pdf
from simulation.utils.criar_tabela_transferencias_pdf import criar_tabela_transferencias_pdf
from simulation.infrastructure.simulation_database_reader import (
    carregar_resumo_transferencias,
    carregar_resumo_lastmile
)
from simulation.utils.criar_tabela_lastmile_pdf import criar_tabela_lastmile_pdf


# Função para desenhar o rodapé com número da página
def rodape(canvas: Canvas, doc):
    canvas.saveState()
    page_number = canvas.getPageNumber()
    canvas.setFont('Helvetica', 8)
    canvas.drawCentredString(A4[0] / 2.0, 1 * cm, f"Pág. {page_number}")
    canvas.restoreState()


def gerar_relatorio_simulacao(
    tenant_id,
    envio_data,
    simulation_id,
    k_clusters_testados,
    simulation_db,
    base_dir=None,
    grafico_custo_path=None
):

    # 🔥 FIX DEFINITIVO
    envio_data_local = str(envio_data)

    styles = getSampleStyleSheet()
    style_h1 = styles["Heading1"]
    style_h2 = styles["Heading2"]
    style_normal = styles["Normal"]

    if base_dir is None:
        raise ValueError("base_dir deve ser informado no novo padrão!")

    relatorio_dir = os.path.join(
        base_dir,
        "relatorios",
        tenant_id,
        envio_data_local
    )

    os.makedirs(relatorio_dir, exist_ok=True)

    relatorio_path = os.path.join(
        relatorio_dir,
        f"relatorio_simulation_{envio_data_local}.pdf"
    )

    # 🔹 Geração de gráfico consolidado
    if not grafico_custo_path or not os.path.exists(grafico_custo_path):
        try:
            query = """
                SELECT k_clusters, custo_transferencia, custo_last_mile, custo_cluster
                FROM resultados_simulacao
                WHERE tenant_id = %s AND envio_data = %s
                ORDER BY k_clusters
            """

            df = pd.read_sql(query, simulation_db, params=(tenant_id, envio_data_local))

            if not df.empty:
                df["custo_total"] = (
                    df["custo_transferencia"].fillna(0) +
                    df["custo_last_mile"].fillna(0) +
                    df["custo_cluster"].fillna(0)
                )

                fig, ax = plt.subplots(figsize=(8, 5))

                ax.bar(df["k_clusters"], df["custo_transferencia"], label="Transferência")
                ax.bar(
                    df["k_clusters"],
                    df["custo_last_mile"],
                    bottom=df["custo_transferencia"],
                    label="Last-mile"
                )
                ax.bar(
                    df["k_clusters"],
                    df["custo_cluster"],
                    bottom=df["custo_transferencia"] + df["custo_last_mile"],
                    label="Cluster"
                )

                ax.plot(
                    df["k_clusters"],
                    df["custo_total"],
                    color="black",
                    marker="o",
                    label="Custo Total"
                )

                ax.set_title(f"Custo Total por k_clusters — {envio_data_local}")
                ax.set_xlabel("k_clusters")
                ax.set_ylabel("Custo (R$)")
                ax.legend()
                ax.grid(True)

                grafico_dir = os.path.join(base_dir, "graphs", tenant_id, envio_data_local)
                os.makedirs(grafico_dir, exist_ok=True)

                grafico_custo_path = os.path.join(
                    grafico_dir,
                    f"grafico_simulacao_{tenant_id}_{envio_data_local}.png"
                )

                plt.tight_layout()
                plt.savefig(grafico_custo_path)
                plt.close()

                print(f"✅ Gráfico consolidado salvo: {grafico_custo_path}")

        except Exception as e:
            print(f"⚠️ Não foi possível gerar gráfico consolidado: {e}")
            grafico_custo_path = None

    # Documento PDF
    doc = SimpleDocTemplate(relatorio_path, pagesize=A4)
    doc.build([], onFirstPage=rodape, onLaterPages=rodape)

    elements = []

    # Capa
    elements.append(Paragraph("Relatório de Simulação Logística", style_h1))
    elements.append(Spacer(1, 20))
    elements.append(Paragraph(f"Tenant: {tenant_id}", style_normal))
    elements.append(Paragraph(f"Data de envio: {envio_data_local}", style_normal))
    elements.append(Paragraph(f"ID da simulação: {simulation_id}", style_normal))
    elements.append(PageBreak())

    # Mapas
    maps_dir = os.path.join(base_dir, "maps", tenant_id, envio_data_local)

    for k in sorted(k_clusters_testados):
        elements.append(Paragraph(f"🔢 Simulação com k = {k} clusters", style_h2))
        elements.append(Spacer(1, 10))

        for tipo, titulo in [
            ("clusterizacao", "🧩 Clusterização"),
            ("transferencias", "🚛 Transferências"),
            ("lastmile", "📦 Last-mile"),
        ]:
            elements.append(Paragraph(titulo, style_normal))

            if k == 0 and tipo in ["clusterizacao", "transferencias"]:
                texto = (
                    "Clusterização não aplicável."
                    if tipo == "clusterizacao"
                    else "Sem transferências no baseline."
                )
                elements.append(Paragraph(texto, style_normal))
                continue

            if tipo == "lastmile":
                img_path = os.path.join(
                    maps_dir,
                    f"{tenant_id}_mapa_lastmile_{envio_data_local}_k{k}.png"
                )
            else:
                img_path = None

            if img_path and os.path.exists(img_path):
                elements.append(Image(img_path, width=480, height=280))
            else:
                elements.append(Paragraph("Mapa disponível em HTML.", style_normal))

            # Transferências
            if tipo == "transferencias":
                try:
                    df = carregar_resumo_transferencias(
                        simulation_db, tenant_id, envio_data_local, k
                    )
                    if not df.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(Paragraph("Resumo Transferências", style_normal))
                        elements.append(criar_tabela_transferencias_pdf(df))
                except Exception as e:
                    elements.append(Paragraph(f"Erro: {e}", style_normal))

            # Clusterização
            if tipo == "clusterizacao":
                try:
                    df = gerar_resumo_clusterizacao(
                        simulation_db, tenant_id, envio_data_local, k
                    )
                    if not df.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(Paragraph("Resumo Cluster", style_normal))
                        elements.append(criar_tabela_resumo_pdf(df))
                except Exception as e:
                    elements.append(Paragraph(f"Erro: {e}", style_normal))

            # Last-mile
            if tipo == "lastmile":
                try:
                    df = carregar_resumo_lastmile(
                        simulation_db, tenant_id, envio_data_local, k
                    )
                    if not df.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(Paragraph("Resumo Last-mile", style_normal))
                        elements.append(criar_tabela_lastmile_pdf(df))
                except Exception as e:
                    elements.append(Paragraph(f"Erro: {e}", style_normal))

        elements.append(PageBreak())

    # Gráfico final
    if grafico_custo_path and os.path.exists(grafico_custo_path):
        elements.append(Paragraph("📊 Custos", style_h2))
        elements.append(Image(grafico_custo_path, width=480, height=280))
    else:
        elements.append(Paragraph("⚠️ Gráfico não encontrado.", style_normal))

    doc.build(elements, onFirstPage=rodape, onLaterPages=rodape)

    return relatorio_path