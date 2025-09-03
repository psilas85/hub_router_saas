#visualization/gerar_tabela_transferencias.py

import pandas as pd
from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle

def criar_tabela_lastmile_pdf(df: pd.DataFrame):
    from reportlab.platypus import Table, TableStyle, Paragraph
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib import colors

    style_cell = ParagraphStyle(name='Normal', fontSize=7)

    df_numerico = df.copy()
    df_numerico["peso_total_kg"] = df_numerico["peso_total_kg"].astype(float).round(2)
    df_numerico["qde_volumes"] = df_numerico["qde_volumes"].astype(int)
    df_numerico["qde_entregas"] = df_numerico["qde_entregas"].astype(int)
    df_numerico["distancia_parcial_km"] = df_numerico["distancia_parcial_km"].astype(float).round(2)
    df_numerico["tempo_parcial_min"] = df_numerico["tempo_parcial_min"].astype(float).round(2)
    df_numerico["distancia_total_km"] = df_numerico["distancia_total_km"].astype(float).round(2)
    df_numerico["tempo_total_min"] = df_numerico["tempo_total_min"].astype(float).round(2)

    # Totalizador
    total = {
        "rota_id": "TOTAL",
        "tipo_veiculo": "",
        "peso_total_kg": round(df_numerico["peso_total_kg"].sum(), 2),
        "qde_volumes": df_numerico["qde_volumes"].sum(),
        "distancia_parcial_km": round(df_numerico["distancia_parcial_km"].sum(), 2),
        "tempo_parcial_min": round(df_numerico["tempo_parcial_min"].sum(), 2),
        "distancia_total_km": round(df_numerico["distancia_total_km"].sum(), 2),
        "tempo_total_min": round(df_numerico["tempo_total_min"].sum(), 2),
        "qde_entregas": df_numerico["qde_entregas"].sum()
    }

    df_total = pd.concat([df_numerico, pd.DataFrame([total])], ignore_index=True)

    df_total["tipo_veiculo"] = df_total["tipo_veiculo"].apply(
        lambda x: Paragraph(str(x), style_cell) if pd.notnull(x) and str(x).strip() else ""
    )

    colunas = [
        "rota_id", "tipo_veiculo", "peso_total_kg", "qde_volumes",
        "distancia_parcial_km", "tempo_parcial_min",
        "distancia_total_km", "tempo_total_min", "qde_entregas"
    ]

    headers = [
        "Rota", "Veículo", "Peso (kg)", "Volumes",
        "Dist. ida (km)", "Tempo ida (min)",
        "Dist. total (km)", "Tempo total (min)", "Entregas"
    ]

    data = [headers] + df_total[colunas].values.tolist()

    col_widths = [60, 50, 45, 45, 55, 55, 55, 55, 45]

    tabela = Table(data, colWidths=col_widths, hAlign='LEFT')

    tabela.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightblue),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#f0f0f0")),
        ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
        ('ALIGN', (0, 1), (1, -1), 'LEFT'),
        ('FONTSIZE', (0, 0), (-1, -1), 7),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
        ('TOPPADDING', (0, -1), (-1, -1), 6),
        ('BOTTOMPADDING', (0, -1), (-1, -1), 6),
    ]))

    return tabela


