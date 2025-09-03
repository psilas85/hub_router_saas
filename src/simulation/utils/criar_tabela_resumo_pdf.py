#utils/criar_tabela_resumo_pdf.py
import pandas as pd
from reportlab.platypus import Table, TableStyle
from reportlab.lib import colors

from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle


def criar_tabela_resumo_pdf(df):

    #Estilo para células com quebra de linha
    style_cell = ParagraphStyle(name='Normal', fontSize=8)
    
    # Conversão e totalizador
    df_numerico = df.copy()
    df_numerico["peso_total_kg"] = df_numerico["peso_total_kg"].astype(float)
    df_numerico["volumes_total"] = df_numerico["volumes_total"].astype(int)
    df_numerico["qtd_entregas"] = df_numerico["qtd_entregas"].astype(int)

    valor_nf_total = (
        df["valor_nf_total"]
        .str.replace(r"[^\d,]", "", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
        .sum()
    )

    valor_frete_total = (
        df["valor_frete_total"]
        .str.replace(r"[^\d,]", "", regex=True)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
        .sum()
    )

    total = {
        "cluster": "Total",
        "cluster_cidade": "",
        "qtd_entregas": df_numerico["qtd_entregas"].sum(),
        "peso_total_kg": round(df_numerico["peso_total_kg"].sum(), 2),
        "volumes_total": df_numerico["volumes_total"].sum(),
        "valor_nf_total": f"R$ {valor_nf_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        "valor_frete_total": f"R$ {valor_frete_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
    }

    df_totalizado = pd.concat([df, pd.DataFrame([total])], ignore_index=True)

    # Aplica quebra de linha para cluster_cidade
    df_totalizado["cluster_cidade"] = df_totalizado["cluster_cidade"].apply(
        lambda x: Paragraph(str(x), style_cell) if x else ""
    )

    # Tabela com largura controlada para A4
    data = [df_totalizado.columns.tolist()] + df_totalizado.values.tolist()
    col_widths = [35, 85, 55, 65, 65, 85, 85]
    tabela = Table(data, colWidths=col_widths, hAlign='LEFT')

    # Estilo
    tabela.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#f0f0f0")),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTSIZE', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, -1), (-1, -1), 6),
        ('BOTTOMPADDING', (0, -1), (-1, -1), 6),
    ]))

    return tabela
