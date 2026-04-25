#hub_router_1.0.1/src/simulation/utils/export_entregas_excel.py

import os
import pandas as pd


def exportar_entregas_com_rotas_excel(clusterization_db, simulation_db, tenant_id, envio_data, output_dir):
    # Campos desejados da tabela entregas
    campos_entregas = [
        "cte_numero", "cte_rua", "cte_numero_endereco", "cte_bairro", "cte_cidade", "cte_nf",
        "cte_peso", "cte_volumes", "cte_valor_nf", "cte_valor_frete", "cte_tempo_atendimento_min", "envio_data"
    ]
    query_entregas = f"""
        SELECT {', '.join(campos_entregas)}
        FROM entregas
        WHERE tenant_id = %s AND envio_data = %s
    """
    df_entregas = pd.read_sql(query_entregas, clusterization_db, params=(tenant_id, envio_data))

    # Campos desejados da tabela rotas_last_mile
    campos_rotas = [
        "cte_numero", "k_clusters", "rota_id", "ordem_entrega", "cluster"
    ]
    query_rotas = f"""
        SELECT {', '.join(campos_rotas)}
        FROM rotas_last_mile
        WHERE tenant_id = %s AND envio_data = %s
    """
    df_rotas = pd.read_sql(query_rotas, simulation_db, params=(tenant_id, envio_data))
    df_rotas = df_rotas.rename(columns={"k_clusters": "cenario_k"})

    # Merge pelo cte_numero
    df = pd.merge(df_entregas, df_rotas, on="cte_numero", how="left")

    # Ordena para facilitar leitura
    df = df.sort_values(["cenario_k", "rota_id", "ordem_entrega"]).reset_index(drop=True)

    # Salva Excel
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"entregas_simulacao_{envio_data}.xlsx")
    df.to_excel(output_path, index=False)
    return output_path
