# clusterization/visualization/gerador_relatorio_clusterizacao.py

import logging
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, PageBreak, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from datetime import datetime
import pandas as pd
import os

logger = logging.getLogger(__name__)

BASE_OUTPUT_DIR = "/app/output"

def gerar_relatorio_clusterizacao(
    caminho_mapa_html: str,
    caminhos_graficos: list[str],
    envio_data: str,
    tenant_id: str,
    df_resumo: pd.DataFrame,
    output_path: str = None,
    caminho_mapa_png: str | None = None
):
    output_path = output_path or os.path.join(BASE_OUTPUT_DIR, "relatorios", tenant_id)
    output_path = os.path.abspath(output_path)
    os.makedirs(output_path, exist_ok=True)

    nome_arquivo = os.path.join(output_path, "relatorio_clusterizacao.pdf")

    doc = SimpleDocTemplate(nome_arquivo, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"Relatório de Clusterização – {envio_data}", styles["Title"]))
    story.append(Paragraph(f"Tenant: <b>{tenant_id}</b>", styles["Normal"]))
    story.append(Paragraph(f"Data de geração: {datetime.today().strftime('%d/%m/%Y %H:%M')}", styles["Normal"]))
    story.append(Paragraph("📎 Este relatório foi gerado automaticamente pelo Cluster Router", styles["Normal"]))
    story.append(Spacer(1, 20))

    story.append(Paragraph("📍 <b>Mapa dos clusters</b>", styles["Heading2"]))
    story.append(Paragraph(f"O mapa interativo pode ser visualizado em:<br/><b>{caminho_mapa_html}</b>", styles["Normal"]))
    story.append(Spacer(1, 8))

    if caminho_mapa_png and os.path.exists(caminho_mapa_png):
        story.append(Image(caminho_mapa_png, width=480, height=360))
        story.append(Spacer(1, 20))
    else:
        logger.warning(f"⚠️ PNG do mapa não encontrado: {caminho_mapa_png}")

    story.append(PageBreak())
    story.append(Paragraph("📊 <b>Resumo por Cluster</b>", styles["Heading2"]))

    df = df_resumo.copy().sort_values("cluster")
    df["cluster"] = df["cluster"].astype(str)

    df.rename(columns={
        "cluster": "cluster",
        "quantidade_entregas": "qtd_entregas",
        "peso_total_kg": "peso_total_kg",
        "quantidade_volumes": "volumes_total",
        "cte_valor_nf_total": "valor_nf_total",
        "cte_valor_frete_total": "valor_frete_total"
    }, inplace=True)

    if "valor_nf_total" not in df.columns:
        logger.warning("⚠️ Coluna 'valor_nf_total' ausente. Criando com zero.")
        df["valor_nf_total"] = 0.0
    if "valor_frete_total" not in df.columns:
        logger.warning("⚠️ Coluna 'valor_frete_total' ausente. Criando com zero.")
        df["valor_frete_total"] = 0.0

    df["valor_nf"] = df["valor_nf_total"].astype(float).apply(lambda x: f"R$ {x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    df["valor_frete"] = df["valor_frete_total"].astype(float).apply(lambda x: f"R$ {x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))
    df["peso_total"] = df["peso_total_kg"].astype(float).round(2).apply(lambda x: f"{x:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','))

    total_nf = df["valor_nf_total"].sum()
    total_frete = df["valor_frete_total"].sum()
    total_peso = df["peso_total_kg"].sum()

    linha_total = {
        "cluster": "Total",
        "qtd_entregas": df["qtd_entregas"].sum(),
        "peso_total": f"{total_peso:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','),
        "volumes_total": df["volumes_total"].sum(),
        "valor_nf": f"R$ {total_nf:,.2f}".replace('.', '#').replace(',', '.').replace('#', ','),
        "valor_frete": f"R$ {total_frete:,.2f}".replace('.', '#').replace(',', '.').replace('#', ',')
    }

    df_display = df[["cluster", "qtd_entregas", "peso_total", "volumes_total", "valor_nf", "valor_frete"]]
    df_display = pd.concat([df_display, pd.DataFrame([linha_total])], ignore_index=True)

    dados = [df_display.columns.tolist()] + df_display.fillna("").values.tolist()

    tabela = Table(dados, hAlign='LEFT')
    tabela.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (1, 1), (-1, -1), 'CENTER'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
    ]))
    story.append(tabela)
    story.append(Spacer(1, 20))

    for caminho in caminhos_graficos:
        if os.path.exists(caminho):
            story.append(Paragraph(os.path.basename(caminho).replace("_", " ").title(), styles["Heading2"]))
            story.append(Image(caminho, width=480, height=280))
            story.append(Spacer(1, 20))

    doc.build(story)
    print(f"✅ Relatório PDF gerado em: {nome_arquivo}")
    return nome_arquivo
