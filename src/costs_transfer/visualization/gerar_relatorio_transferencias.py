# transfer_routing/visualization/gerar_relatorio_transferencias.py

import os
from datetime import datetime
import pandas as pd
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
)
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.units import cm
import logging

logger = logging.getLogger(__name__)

def formatar_valor_br(valor: float) -> str:
    return format(valor, ",.2f").replace(",", "X").replace(".", ",").replace("X", ".")

def gerar_relatorio_transferencias(
    envio_data: str,
    tenant_id: str,
    df_detalhado: pd.DataFrame,
    output_path: str = "exports/costs_transfer/pdf"
):
    os.makedirs(f"{output_path}/{tenant_id}", exist_ok=True)

    # 🔥 Nome fixo sem timestamp, para alinhar com o endpoint /visualizar
    nome_arquivo = f"{output_path}/{tenant_id}/relatorio_transfer_costs_detalhado_{envio_data}.pdf"

    try:
        doc = SimpleDocTemplate(
            nome_arquivo, pagesize=A4,
            rightMargin=0.6*cm, leftMargin=0.6*cm,
            topMargin=1.2*cm, bottomMargin=1.2*cm
        )

        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(name='Small', fontSize=7.5, leading=9))
        small_style = styles['Small']

        story = []
        titulo = f"Relatório de Custos de Transferência - {envio_data}"
        story.append(Paragraph(titulo, styles["Title"]))
        story.append(Spacer(1, 0.4 * cm))

        headers = [
            "Rota", "Hub", "Veículo", "Peso (kg)",
            "Frete (R$)", "Distância (km)", "Custo Transf. (R$)", "% Custo/Frete"
        ]
        dados = [headers]

        for _, row in df_detalhado.iterrows():
            dados.append([
                str(row["rota_transf"]),
                Paragraph(str(row["hub_central_nome"]), small_style),
                Paragraph(str(row["tipo_veiculo"]), small_style),
                formatar_valor_br(row["cte_peso"]),
                formatar_valor_br(row["cte_valor_frete"]),
                formatar_valor_br(row["distancia_total"]),
                formatar_valor_br(row["custo_transferencia_total"]),
                f'{(row["percentual_custo"] * 100):,.2f} %'
                  .replace(".", "#").replace(",", ".").replace("#", ",")
            ])

        total = df_detalhado[["cte_peso", "cte_valor_frete", "distancia_total", "custo_transferencia_total"]].sum()
        percentual_total = total["custo_transferencia_total"] / total["cte_valor_frete"] if total["cte_valor_frete"] else 0

        dados.append([
            "TOTAL", "", "",
            formatar_valor_br(total["cte_peso"]),
            formatar_valor_br(total["cte_valor_frete"]),
            formatar_valor_br(total["distancia_total"]),
            formatar_valor_br(total["custo_transferencia_total"]),
            f'{percentual_total * 100:,.2f} %'
              .replace(".", "#").replace(",", ".").replace("#", ",")
        ])

        tabela = Table(dados, repeatRows=1, colWidths=[
            1.2*cm, 4.0*cm, 2.0*cm, 2.0*cm,
            2.0*cm, 2.0*cm, 2.2*cm, 2.2*cm
        ])
        tabela.setStyle(TableStyle([
            ('FONTSIZE', (0, 0), (-1, -1), 7.5),
            ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('ALIGN', (3, 1), (-1, -1), 'RIGHT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('TOPPADDING', (0, 0), (-1, 0), 6),
        ]))

        story.append(tabela)
        doc.build(story)

        logger.info(f"✅ PDF final salvo em: {nome_arquivo}")
        return nome_arquivo

    except Exception as e:
        logger.error(f"❌ Erro ao gerar relatório PDF: {e}")
        return None
