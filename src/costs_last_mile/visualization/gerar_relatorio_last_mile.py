#costs_last_mile/visualization/gerar_relatorio_last_mile.py

import os
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

from costs_last_mile.visualization.logging_factory import get_logger

logger = get_logger(__name__)

def gerar_relatorio_last_mile(envio_data: str, tenant_id: str, df_detalhes, df_resumo, modo_forcar: bool = False, output_path: str = "exports/costs_last_mile"):

    # 🔁 Diretórios
    PDF_DIR = f"{output_path}/relatorios/{tenant_id}"
    IMG_DIR = PDF_DIR
    os.makedirs(PDF_DIR, exist_ok=True)

    # 📄 Nome fixo do arquivo PDF (sempre sobrescreve o anterior)
    nome_pdf = f"{PDF_DIR}/costs_last_mile_{envio_data}.pdf"
    grafico_pizza_path = f"{IMG_DIR}/grafico_pizza_{envio_data}.png"
    grafico_barra_path = f"{IMG_DIR}/grafico_barra_{envio_data}.png"

    # 📊 Gráfico pizza
    df_resumo.groupby("veiculo")["custo_entrega_total"].sum().plot.pie(
        autopct="%1.1f%%", startangle=90, counterclock=False, figsize=(6, 6), ylabel=""
    )
    plt.title("Distribuição de Custos por Veículo")
    plt.tight_layout()
    plt.savefig(grafico_pizza_path)
    plt.close()
    logger.info(f"🧩 Gráfico de pizza salvo em: {grafico_pizza_path}")

    # 📊 Gráfico barra com ordenação
    df_plot = df_resumo.groupby("veiculo")[["custo_entrega_total", "cte_frete_total"]].sum()
    df_plot = df_plot.sort_values(by="custo_entrega_total", ascending=False)
    df_plot.plot.bar(figsize=(8, 5))
    plt.title("Custo vs Frete por Veículo")
    plt.ylabel("R$")
    plt.tight_layout()
    plt.savefig(grafico_barra_path)
    plt.close()
    logger.info(f"🧱 Gráfico de barras salvo em: {grafico_barra_path}")

    # 📄 Criar PDF
    styles = getSampleStyleSheet()
    doc = SimpleDocTemplate(nome_pdf, pagesize=A4)
    story = []

    # Seção 1: Detalhes por rota
    story.append(Paragraph(f"Relatório de Custos Last Mile - {envio_data}", styles["Title"]))
    story.append(Paragraph(f"Tenant: {tenant_id}", styles["Normal"]))
    story.append(Spacer(1, 12))
    story.append(Paragraph("🔍 Detalhamento por Rota (Subcluster)", styles["Heading2"]))

    # ➕ Calcular totais e adicionar ao final
    totais_detalhes = pd.DataFrame([{
        "data_envio": "",
        "cluster": None,
        "sub_cluster": None,
        "veiculo": "TOTAL",
        "quantidade_entregas": df_detalhes["quantidade_entregas"].sum(),
        "peso_total_kg": df_detalhes["peso_total_kg"].sum(),
        "distancia_total_km": df_detalhes["distancia_total_km"].sum(),
        "custo_entrega_total": df_detalhes["custo_entrega_total"].sum(),
        "cte_frete_total": df_detalhes["cte_frete_total"].sum(),
        "percentual_custo": None
    }])

    df_detalhes = pd.concat([df_detalhes, totais_detalhes], ignore_index=True)

    # ➕ Montagem da tabela de detalhes
    dados_detalhes = [["Data", "Cluster", "SubCluster", "Veículo", "Entregas", "Peso (kg)", "Distância", "Custo", "Frete", "% Custo"]]
    for _, row in df_detalhes.iterrows():
        dados_detalhes.append([
            str(row.get("data_envio") or ""),
            int(row["cluster"]) if pd.notnull(row["cluster"]) else "-",
            int(row["sub_cluster"]) if pd.notnull(row["sub_cluster"]) else "-",
            row.get("veiculo", ""),
            int(row["quantidade_entregas"]) if pd.notnull(row["quantidade_entregas"]) else "-",
            f"{row['peso_total_kg']:.1f}" if pd.notnull(row["peso_total_kg"]) else "-",
            f"{row['distancia_total_km']:.1f}" if pd.notnull(row["distancia_total_km"]) else "-",
            f"R$ {row['custo_entrega_total']:.2f}" if pd.notnull(row["custo_entrega_total"]) else "-",
            f"R$ {row['cte_frete_total']:.2f}" if pd.notnull(row["cte_frete_total"]) else "-",
            f"{row['percentual_custo']:.1f} %" if pd.notnull(row["percentual_custo"]) else "-"
        ])

    col_widths_detalhes = [60, 40, 50, 60, 50, 60, 60, 65, 60, 50]
    tabela_detalhes = Table(dados_detalhes, colWidths=col_widths_detalhes, hAlign="CENTER", repeatRows=1)
    tabela_detalhes.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#404040")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.black),
    ]))
    # ➕ Destacar a linha TOTAL
    total_row_index = len(dados_detalhes) - 1
    tabela_detalhes.setStyle(TableStyle([
        ('FONTNAME', (0, total_row_index), (-1, total_row_index), 'Helvetica-Bold'),
        ('BACKGROUND', (0, total_row_index), (-1, total_row_index), colors.lightgrey),
    ]))

    story.append(tabela_detalhes)

    # Seção 2: Resumo
    story.append(PageBreak())
    story.append(Paragraph("📊 Resumo por Veículo", styles["Heading2"]))

    # ➕ Calcular totais e adicionar ao DataFrame
    totais = pd.DataFrame([{
        "veiculo": "TOTAL",
        "quantidade_entregas": df_resumo["quantidade_entregas"].sum(),
        "peso_total_kg": df_resumo["peso_total_kg"].sum(),
        "distancia_total_km": df_resumo["distancia_total_km"].sum(),
        "custo_entrega_total": df_resumo["custo_entrega_total"].sum(),
        "cte_frete_total": df_resumo["cte_frete_total"].sum(),
        "percentual_custo": None
    }])

    df_resumo = pd.concat([df_resumo, totais], ignore_index=True)

    # ➕ Montagem da tabela de resumo
    dados_resumo = [["Veículo", "Entregas", "Peso (kg)", "Distância", "Custo", "Frete", "% Custo"]]
    for _, row in df_resumo.iterrows():
        dados_resumo.append([
            row.get("veiculo", ""),
            int(row["quantidade_entregas"]) if pd.notnull(row["quantidade_entregas"]) else "-",
            f"{row['peso_total_kg']:.1f}" if pd.notnull(row["peso_total_kg"]) else "-",
            f"{row['distancia_total_km']:.1f}" if pd.notnull(row["distancia_total_km"]) else "-",
            f"R$ {row['custo_entrega_total']:.2f}" if pd.notnull(row["custo_entrega_total"]) else "-",
            f"R$ {row['cte_frete_total']:.2f}" if pd.notnull(row["cte_frete_total"]) else "-",
            f"{row['percentual_custo']:.1f} %" if pd.notnull(row["percentual_custo"]) else "-"
        ])

    col_widths_resumo = [70, 55, 60, 60, 70, 60, 50]
    tabela_resumo = Table(dados_resumo, colWidths=col_widths_resumo, hAlign="CENTER", repeatRows=1)
    tabela_resumo.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#404040")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
        ('GRID', (0, 0), (-1, -1), 0.3, colors.black),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
    ]))

    story.append(tabela_resumo)
    story.append(Spacer(1, 12))

    # Inserir gráficos no PDF
    for img in [grafico_pizza_path, grafico_barra_path]:
        story.append(Image(img, width=400, height=250))
        story.append(Spacer(1, 12))

    doc.build(story)
    logger.info(f"📄 Relatório PDF gerado: {nome_pdf}")
