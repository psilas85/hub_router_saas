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
    base_dir="output",
    grafico_custo_path=None
):
    styles = getSampleStyleSheet()
    style_h1 = styles["Heading1"]
    style_h2 = styles["Heading2"]
    style_normal = styles["Normal"]

    # Caminho do PDF de saída
    relatorio_dir = os.path.join(base_dir, "relatorios", tenant_id)
    os.makedirs(relatorio_dir, exist_ok=True)
    relatorio_path = os.path.join(relatorio_dir, f"relatorio_simulation_{envio_data}.pdf")

    # 🔹 Garante que o gráfico consolidado exista
    if not grafico_custo_path or not os.path.exists(grafico_custo_path):
        try:
            query = """
                SELECT k_clusters, custo_transferencia, custo_last_mile, custo_cluster
                FROM resultados_simulacao
                WHERE tenant_id = %s AND envio_data = %s
                ORDER BY k_clusters
            """
            df = pd.read_sql(query, simulation_db, params=(tenant_id, envio_data))
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
                    label="Last-mile",
                )
                ax.bar(
                    df["k_clusters"],
                    df["custo_cluster"],
                    bottom=df["custo_transferencia"] + df["custo_last_mile"],
                    label="Cluster",
                )

                ax.plot(
                    df["k_clusters"],
                    df["custo_total"],
                    color="black",
                    marker="o",
                    label="Custo Total",
                )
                ax.set_title(f"Custo Total por k_clusters — {envio_data}")
                ax.set_xlabel("k_clusters")
                ax.set_ylabel("Custo (R$)")
                ax.legend()
                ax.grid(True)

                grafico_dir = os.path.join(base_dir, "graphs", tenant_id)
                os.makedirs(grafico_dir, exist_ok=True)
                grafico_custo_path = os.path.join(
                    grafico_dir, f"grafico_simulacao_{tenant_id}_{envio_data}.png"
                )
                plt.tight_layout()
                plt.savefig(grafico_custo_path)
                plt.close()
                print(f"✅ Gráfico consolidado salvo: {grafico_custo_path}")
        except Exception as e:
            print(f"⚠️ Não foi possível gerar gráfico consolidado: {e}")
            grafico_custo_path = None

    # Documento com rodapé personalizado
    doc = SimpleDocTemplate(relatorio_path, pagesize=A4)
    doc.build([], onFirstPage=rodape, onLaterPages=rodape)
    elements = []

    # Capa
    elements.append(Paragraph("Relatório de Simulação Logística", style_h1))
    elements.append(Spacer(1, 20))
    elements.append(Paragraph(f"Tenant: {tenant_id}", style_normal))
    elements.append(Paragraph(f"Data de envio: {envio_data}", style_normal))
    elements.append(Paragraph(f"ID da simulação: {simulation_id}", style_normal))
    elements.append(PageBreak())

    # Seção de Mapas por k
    maps_dir = os.path.join(base_dir, "maps", tenant_id)
    for k in sorted(k_clusters_testados):
        elements.append(Paragraph(f"🔢 Simulação com k = {k} clusters", style_h2))
        elements.append(Spacer(1, 10))

        for tipo, titulo in [
            ("clusterizacao", "🧩 Clusterização"),
            ("transferencias", "🚛 Transferências"),
            ("lastmile", "📦 Last-mile"),
        ]:
            elements.append(Paragraph(titulo, style_normal))

            # ✅ Tratamento especial para k = 1 (hub central)
            if k == 1 and tipo in ["clusterizacao", "transferencias"]:
                if tipo == "clusterizacao":
                    elements.append(
                        Paragraph(
                            "Simulação especial partindo do hub central. Clusterização não aplicável.",
                            style_normal,
                        )
                    )
                elif tipo == "transferencias":
                    elements.append(
                        Paragraph(
                            "Não há rotas de transferência nesta simulação (k = 1).",
                            style_normal,
                        )
                    )
                continue

            img_path = os.path.join(
                maps_dir, f"{tenant_id}_mapa_{tipo}_{envio_data}_k{k}.png"
            )
            if os.path.exists(img_path):
                elements.append(Image(img_path, width=480, height=280, kind="proportional"))
            else:
                elements.append(Paragraph("⚠️ Mapa não encontrado.", style_normal))

            # ➕ Tabelas de transferências
            if tipo == "transferencias":
                try:
                    df_resumo_transf = carregar_resumo_transferencias(
                        simulation_db, tenant_id, envio_data, k
                    )
                    if not df_resumo_transf.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(Paragraph("📋 Resumo das Transferências", style_normal))
                        elements.append(Spacer(1, 6))
                        elements.append(criar_tabela_transferencias_pdf(df_resumo_transf))
                except Exception as e:
                    elements.append(
                        Paragraph(f"⚠️ Erro ao gerar resumo das transferências: {e}", style_normal)
                    )

            # ➕ Resumo clusterização
            if tipo == "clusterizacao":
                try:
                    df_resumo = gerar_resumo_clusterizacao(
                        simulation_db, tenant_id, envio_data, k
                    )
                    if not df_resumo.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(Paragraph("📋 Resumo por Cluster", style_normal))
                        elements.append(Spacer(1, 6))
                        elements.append(criar_tabela_resumo_pdf(df_resumo))
                except Exception as e:
                    elements.append(
                        Paragraph(f"⚠️ Erro ao gerar resumo por cluster: {e}", style_normal)
                    )

            # ➕ Resumo last-mile
            if tipo == "lastmile":
                try:
                    df_resumo_lastmile = carregar_resumo_lastmile(
                        simulation_db, tenant_id, envio_data, k
                    )
                    if not df_resumo_lastmile.empty:
                        elements.append(Spacer(1, 10))
                        elements.append(Paragraph("📋 Resumo Last-mile", style_normal))
                        elements.append(Spacer(1, 6))
                        elements.append(criar_tabela_lastmile_pdf(df_resumo_lastmile))
                except Exception as e:
                    elements.append(
                        Paragraph(f"⚠️ Erro ao gerar resumo last-mile: {e}", style_normal)
                    )

            # Separador visual
            separator = Drawing(480, 1)
            separator.add(Line(0, 0, 480, 0))
            elements.append(Spacer(1, 4))
            elements.append(separator)
            elements.append(Spacer(1, 12))

        elements.append(PageBreak())

    # Gráfico de Custo Consolidado
    if grafico_custo_path and os.path.exists(grafico_custo_path):
        elements.append(Paragraph("📊 Gráfico de Custos por Heurística", style_h2))
        elements.append(Image(grafico_custo_path, width=480, height=280))
    else:
        elements.append(Paragraph("⚠️ Gráfico de custos não encontrado.", style_normal))

    # Gera o PDF com rodapé
    doc.build(elements, onFirstPage=rodape, onLaterPages=rodape)
    return relatorio_path
