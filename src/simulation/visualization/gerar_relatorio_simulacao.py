#hub_router_1.0.1/src/simulation/visualization/gerar_relatorio_simulacao.py

# hub_router_1.0.1/src/simulation/visualization/gerar_relatorio_simulacao.py

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

from simulation.visualization.gerar_resumo_clusterizacao import gerar_resumo_clusterizacao
from simulation.utils.criar_tabela_resumo_pdf import criar_tabela_resumo_pdf
from simulation.utils.criar_tabela_transferencias_pdf import criar_tabela_transferencias_pdf
from simulation.infrastructure.simulation_database_reader import (
    carregar_resumo_transferencias,
    carregar_resumo_lastmile
)
from simulation.utils.criar_tabela_lastmile_pdf import criar_tabela_lastmile_pdf
from simulation.utils.path_builder import build_output_path


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
    base_dir="exports/simulation",
    grafico_custo_path=None,
):

    envio_data = str(envio_data)

    styles = getSampleStyleSheet()
    style_h1 = styles["Heading1"]
    style_h2 = styles["Heading2"]
    style_normal = styles["Normal"]

    # 🔥 PADRÃO NOVO
    reports_dir = build_output_path(
        base_dir,
        tenant_id,
        envio_data,
        "reports"
    )

    maps_dir = build_output_path(
        base_dir,
        tenant_id,
        envio_data,
        "maps"
    )

    relatorio_path = os.path.join(
        reports_dir,
        f"relatorio_simulation_{envio_data}.pdf"
    )

    # =============================
    # 🔹 Documento
    # =============================
    doc = SimpleDocTemplate(relatorio_path, pagesize=A4)
    elements = []

    # =============================
    # 🔹 CAPA
    # =============================
    elements.append(Paragraph("Relatório de Simulação Logística", style_h1))
    elements.append(Spacer(1, 20))
    elements.append(Paragraph(f"Tenant: {tenant_id}", style_normal))
    elements.append(Paragraph(f"Data: {envio_data}", style_normal))
    elements.append(Paragraph(f"Simulation ID: {simulation_id}", style_normal))
    elements.append(PageBreak())

    # =============================
    # 🔹 POR CENÁRIO (k)
    # =============================
    for k in sorted(k_clusters_testados):

        elements.append(Paragraph(f"Simulação k = {k}", style_h2))
        elements.append(Spacer(1, 10))

        for tipo, titulo in [
            ("clusterizacao", "Clusterização"),
            ("transferencias", "Transferências"),
            ("lastmile", "Last-mile"),
        ]:

            elements.append(Paragraph(titulo, style_normal))

            # 🔴 lógica de mapa simplificada
            img_path = os.path.join(
                maps_dir,
                f"{tenant_id}_mapa_{tipo}_{envio_data}_k{k}.png"
            )

            if os.path.exists(img_path):
                elements.append(Image(img_path, width=480, height=280))
            else:
                elements.append(Paragraph("Mapa disponível em HTML.", style_normal))

            # =============================
            # 🔹 TABELAS
            # =============================
            try:
                if tipo == "transferencias":
                    df = carregar_resumo_transferencias(
                        simulation_db, tenant_id, envio_data, k
                    )
                    if not df.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(criar_tabela_transferencias_pdf(df))

                elif tipo == "clusterizacao":
                    df = gerar_resumo_clusterizacao(
                        simulation_db, tenant_id, envio_data, k
                    )
                    if not df.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(criar_tabela_resumo_pdf(df))

                elif tipo == "lastmile":
                    df = carregar_resumo_lastmile(
                        simulation_db, tenant_id, envio_data, k
                    )
                    if not df.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(criar_tabela_lastmile_pdf(df))

            except Exception as e:
                elements.append(Paragraph(f"Erro: {e}", style_normal))

        elements.append(PageBreak())

    # =============================
    # 🔹 GRÁFICO FINAL
    # =============================
    if grafico_custo_path and os.path.exists(grafico_custo_path):
        elements.append(Paragraph("Custos consolidados", style_h2))
        elements.append(Image(grafico_custo_path, width=480, height=280))
    else:
        elements.append(Paragraph("Gráfico não disponível.", style_normal))

    # =============================
    # 🔹 BUILD FINAL
    # =============================
    doc.build(elements, onFirstPage=rodape, onLaterPages=rodape)

    print(f"✅ Relatório gerado: {relatorio_path}")

    return relatorio_path