# transfer_routing/visualization/gerador_relatorio_transferencias.py

import os
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors

from transfer_routing.visualization.mapa_estatico import gerar_mapa_estatico_transferencias
from transfer_routing.infrastructure.database_connection import conectar_banco_routing, fechar_conexao
from transfer_routing.visualization.utils_output import caminho_output


def format_number(valor):
    try:
        return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(valor)


def gerar_relatorio_transferencias(
    tenant_id: str,
    envio_data: str,
    data_final: str = None,
    output_path: str = None,
    caminho_mapa_html: str = None,
    caminho_mapa_png: str = None,
    conn=None,
    logger=None
):
    """
    Gera relatório PDF de transferências.
    - Pode receber caminho de mapas já gerados (HTML/PNG).
    - Se não receber, gera o PNG automaticamente.
    - Se data_final não for informado, assume igual a envio_data.
    - Se conn não for informado, abre/fecha conexão automaticamente.
    """
    if data_final is None:
        data_final = envio_data

    # 📂 Define pasta de saída
    if output_path is None:
        output_path = caminho_output(tenant_id, "relatorios")

    os.makedirs(output_path, exist_ok=True)
    nome_arquivo = os.path.join(output_path, f"relatorio_transferencias_{envio_data}_{data_final}.pdf")

    # 🗺️ Usa mapa PNG já gerado ou cria um novo
    mapa_path = caminho_mapa_png
    if not mapa_path:
        mapa_path = gerar_mapa_estatico_transferencias(
            tenant_id, envio_data, data_final, output_path=output_path
        )

    # 🔎 Consulta dados
    query = """
        SELECT *
        FROM transferencias_resumo
        WHERE tenant_id = %s AND envio_data BETWEEN %s AND %s
    """

    auto_close = False
    if conn is None:
        conn = conectar_banco_routing()
        auto_close = True

    df = pd.read_sql(query, conn, params=(tenant_id, envio_data, data_final))

    if auto_close:
        fechar_conexao(conn)

    if df.empty:
        if logger:
            logger.warning(f"Nenhuma rota encontrada para {envio_data} até {data_final}.")
        else:
            print(f"Nenhuma rota encontrada para {envio_data} até {data_final}.")
        return None

    # 📑 Monta PDF
    nome_arquivo = os.path.join(output_path, f"relatorio_transferencias_{envio_data}_{data_final}.pdf")
    doc = SimpleDocTemplate(nome_arquivo, pagesize=A4)
    elementos = []

    styles = getSampleStyleSheet()
    elementos.append(Paragraph(f"Relatório de Transferências - {envio_data} até {data_final}", styles["Title"]))
    elementos.append(Spacer(1, 12))

    if mapa_path and os.path.exists(mapa_path):
        elementos.append(Image(mapa_path, width=500, height=300))
        elementos.append(Spacer(1, 20))
    else:
        elementos.append(Paragraph("⚠️ Mapa não encontrado.", styles["Normal"]))
        elementos.append(Spacer(1, 20))

    # 📊 Monta tabela
    data = [["Rota", "Entregas", "Paradas", "Peso (kg)", "Dist. Ida (km)",
             "Dist. Total (km)", "Tempo Ida (min)", "Tempo Total (min)"]]

    for _, row in df.iterrows():
        data.append([
            row["rota_transf"],
            f"{int(row['quantidade_entregas']):,}".replace(",", "."),
            f"{int(row['clusters_qde']):,}".replace(",", "."),
            format_number(row['cte_peso']),
            format_number(row['distancia_ida_km']),
            format_number(row['distancia_total_km']),
            format_number(row['tempo_ida_min']),
            format_number(row['tempo_total_min'])
        ])

    # 🔥 Totalizadores
    data.append([
        "TOTAL",
        f"{int(df['quantidade_entregas'].sum()):,}".replace(",", "."),
        f"{int(df['clusters_qde'].sum()):,}".replace(",", "."),
        format_number(df['cte_peso'].sum()),
        format_number(df['distancia_ida_km'].sum()),
        format_number(df['distancia_total_km'].sum()),
        format_number(df['tempo_ida_min'].sum()),
        format_number(df['tempo_total_min'].sum())
    ])

    tabela = Table(data, repeatRows=1)
    tabela.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.gray),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
        ('BACKGROUND', (0, -1), (-1, -1), colors.lightgrey),
    ]))

    elementos.append(tabela)
    doc.build(elementos)

    if logger:
        logger.info(f"Relatório salvo em {nome_arquivo}")
    else:
        print(f"Relatório salvo em {nome_arquivo}")

    return nome_arquivo
