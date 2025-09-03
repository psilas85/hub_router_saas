#simulation/visualization/gerador_relatorio_final.py

import os
from simulation.visualization.gerar_relatorio_simulacao import gerar_relatorio_simulacao

def executar_geracao_relatorio_final(tenant_id: str, envio_data: str, simulation_id: str, simulation_db, base_dir="exports/simulation"):
    """
    Executa automaticamente a conversão de mapas .html → .png e gera o relatório final em PDF da simulação.
    Mantém a estrutura: maps/{tenant_id}, graphs/{tenant_id}, relatorios/{tenant_id}
    """
    maps_dir = os.path.join(base_dir, "maps", tenant_id)

    # Buscar os k_clusters salvos
    cursor = simulation_db.cursor()
    cursor.execute("""
        SELECT DISTINCT k_clusters
        FROM resultados_simulacao
        WHERE tenant_id = %s AND envio_data = %s
        ORDER BY k_clusters
    """, (tenant_id, envio_data))
    k_clusters_testados = [row[0] for row in cursor.fetchall()]
    cursor.close()

    if not k_clusters_testados:
        print(f"❌ Nenhum resultado encontrado em resultados_simulacao para envio_data = {envio_data}")
        return

    # Garantir que os mapas HTML estejam convertidos em PNG
    for k in k_clusters_testados:
        for tipo in ["clusterizacao", "transferencias", "last_mile"]:
            html_path = os.path.join(maps_dir, f"{tenant_id}_mapa_{tipo}_{envio_data}_k{k}.html")
            png_path = html_path.replace(".html", ".png")
            if os.path.exists(html_path) and not os.path.exists(png_path):
                try:
                    from simulation.utils.html_to_png import converter_html_para_png
                    converter_html_para_png(html_path, png_path)
                except Exception as e:
                    print(f"⚠️ Erro ao converter {html_path} para PNG: {e}")

    # Caminho do gráfico consolidado
    grafico_custo_path = os.path.join(base_dir, "graphs", tenant_id, f"grafico_simulacao_{tenant_id}_{envio_data}.png")

    if not os.path.exists(grafico_custo_path):
        print(f"⚠️ Gráfico de custos não encontrado para {envio_data}. Relatório não será gerado.")
        return

    # PDF único consolidado
    relatorio_dir = os.path.join(base_dir, "relatorios", tenant_id)
    os.makedirs(relatorio_dir, exist_ok=True)
    relatorio_path = gerar_relatorio_simulacao(
        tenant_id=tenant_id,
        envio_data=envio_data,
        simulation_id=simulation_id,
        k_clusters_testados=k_clusters_testados,
        simulation_db=simulation_db,
        base_dir=base_dir,
        grafico_custo_path=grafico_custo_path
    )

    print(f"✅ Relatório consolidado gerado: {relatorio_path}")
    return relatorio_path
