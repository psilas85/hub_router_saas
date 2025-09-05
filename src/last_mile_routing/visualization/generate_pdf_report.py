#last_mile_routing/visualization/generate_pdf_report.py

import os
import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    Table,
    TableStyle
)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

from last_mile_routing.infrastructure.database_connection import (
    conectar_banco_routing,
    fechar_conexao
)
from last_mile_routing.visualization.mapa_estatico import gerar_mapa_estatico


# 🔧 Função para formatar números com separador de milhar e vírgula decimal
def formatar_numero(valor, inteiro=False):
    if inteiro:
        return f"{int(valor):,}".replace(",", ".")
    else:
        return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def generate_pdf_report(tenant_id, envio_data):
    # 🔧 Corrigido para /app/exports
    output_folder = f"/app/exports/last_mile_routing/relatorios/{tenant_id}"
    os.makedirs(output_folder, exist_ok=True)

    file_name = f"{output_folder}/relatorio_last_mile_{envio_data}.pdf"


    doc = SimpleDocTemplate(file_name, pagesize=A4)
    elements = []

    styles = getSampleStyleSheet()

    title = Paragraph(f"<b>Relatório de Last Mile - {envio_data}</b>", styles["Title"])
    elements.append(title)
    elements.append(Spacer(1, 12))

    # 🗺️ Gerar PNG diretamente pelo Matplotlib
    mapa_path = gerar_mapa_estatico(tenant_id, envio_data)

    if mapa_path and os.path.exists(mapa_path):
        elements.append(Paragraph("<b>🗺️ Mapa das Rotas:</b>", styles["Heading2"]))
        elements.append(Image(mapa_path, width=500, height=350))
        elements.append(Spacer(1, 12))
    else:
        elements.append(Paragraph(
            "⚠️ O mapa não foi encontrado. Verifique se ele foi gerado corretamente.",
            styles["Normal"]
        ))

    try:
        conn = conectar_banco_routing()

        query = """
            SELECT
                r.rota_id,
                r.veiculo,
                COUNT(d.cte_numero) AS qtde_entregas,
                r.peso_total_kg,
                r.tempo_parcial_min,
                r.tempo_total_min,
                r.distancia_parcial_km,
                r.distancia_total_km
            FROM last_mile_rotas r
            LEFT JOIN detalhes_rotas d
                ON r.tenant_id = d.tenant_id
                AND r.envio_data = d.envio_data
                AND r.rota_id = d.rota_id
            WHERE r.tenant_id = %s
            AND r.envio_data = %s
            GROUP BY
                r.rota_id,
                r.veiculo,
                r.peso_total_kg,
                r.tempo_parcial_min,
                r.tempo_total_min,
                r.distancia_parcial_km,
                r.distancia_total_km
            ORDER BY r.rota_id;
        """

        df = pd.read_sql(query, conn, params=(tenant_id, envio_data))

    except Exception as e:
        elements.append(Paragraph(f"<b>❌ Erro ao buscar dados no banco:</b> {e}", styles["Normal"]))
        doc.build(elements)
        print(f"✅ Relatório salvo em {file_name}")
        return

    finally:
        fechar_conexao(conn)

    if df.empty:
        elements.append(Paragraph("⚠️ Nenhuma rota encontrada para esse dia.", styles["Normal"]))
        doc.build(elements)
        print(f"✅ Relatório salvo em {file_name}")
        return

    # 🔥 Cabeçalho da tabela
    data = [
        [
            "Rota",
            "Veículo",
            "Qtde Entregas",
            "Peso (kg)",
            "Tempo Parcial (min)",
            "Tempo Total (min)",
            "Distância Parcial (km)",
            "Distância Total (km)"
        ]
    ]

    # 🔥 Dados linha a linha
    for _, row in df.iterrows():
        data.append([
            str(row["rota_id"]),
            row["veiculo"],
            formatar_numero(row["qtde_entregas"], inteiro=True),
            formatar_numero(row["peso_total_kg"]),
            formatar_numero(row["tempo_parcial_min"]),
            formatar_numero(row["tempo_total_min"]),
            formatar_numero(row["distancia_parcial_km"]),
            formatar_numero(row["distancia_total_km"])
        ])

    # 🔥 Linha totalizadora
    total_rotas = len(df)
    total_entregas = df["qtde_entregas"].sum()
    total_peso = df["peso_total_kg"].sum()
    total_t_parcial = df["tempo_parcial_min"].sum()
    total_t_total = df["tempo_total_min"].sum()
    total_d_parcial = df["distancia_parcial_km"].sum()
    total_d_total = df["distancia_total_km"].sum()

    data.append([
        f"TOTAL ({total_rotas} rotas)",
        "",
        formatar_numero(total_entregas, inteiro=True),
        formatar_numero(total_peso),
        formatar_numero(total_t_parcial),
        formatar_numero(total_t_total),
        formatar_numero(total_d_parcial),
        formatar_numero(total_d_total)
    ])

    table = Table(data, colWidths=[60, 60, 65, 65, 75, 75, 75, 75])
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("BACKGROUND", (0, -1), (-1, -1), colors.lightgrey),  # Totalizador
                ("FONTNAME", (0, -1), (-1, -1), "Helvetica-Bold"),
            ]
        )
    )

    elements.append(Paragraph("<b>Resumo de Rotas</b>", styles["Heading2"]))
    elements.append(table)

    doc.build(elements)

    print(f"✅ Relatório salvo em {file_name}")
