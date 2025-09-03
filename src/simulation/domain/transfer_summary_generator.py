# simulation/domain/transfer_summary_generator.py

import pandas as pd
from datetime import datetime
from simulation.infrastructure.simulation_database_writer import persistir_resumo_transferencias
from simulation.domain.entities import TransferenciaResumo


def gerar_resumo_transferencias_from_detalhes(df_detalhes: pd.DataFrame, logger, simulation_db=None, coordenadas_por_rota=None) -> list[TransferenciaResumo]:
    logger.info("📊 Gerando resumo de transferências a partir de detalhes...")

    df_grouped = df_detalhes.groupby(['rota_id', 'tipo_veiculo']).agg({
        'tenant_id': 'first',
        'envio_data': 'first',
        'simulation_id': 'first',
        'k_clusters': 'first',
        'is_ponto_otimo': 'first',
        'cte_numero': 'count',
        'cte_peso': 'sum',
        'cte_volumes': 'sum',
        'cte_valor_nf': 'sum',
        'cluster': pd.Series.nunique
    }).reset_index()

    df_grouped.rename(columns={
        'cte_numero': 'qde_entregas',
        'cte_peso': 'peso_total_kg',
        'cte_volumes': 'qde_volumes',
        'cte_valor_nf': 'valor_total_nf',
        'cluster': 'qde_clusters_rota'
    }, inplace=True)

    capacidades = {}
    if simulation_db is not None:
        cursor = simulation_db.cursor()
        cursor.execute("SELECT tipo_veiculo, capacidade_kg_max FROM veiculos_transferencia")
        capacidades = {row[0]: float(row[1]) for row in cursor.fetchall()}
        cursor.close()

    lista_resumo = []
    for _, row in df_grouped.iterrows():
        capacidade = capacidades.get(row['tipo_veiculo'], None)
        aproveitamento = (row['peso_total_kg'] / capacidade * 100) if capacidade else None

        coordenadas_seq = None
        if coordenadas_por_rota:
            coords = coordenadas_por_rota.get(row['rota_id'], [])
            coordenadas_seq = coords if isinstance(coords, str) else None


        resumo = TransferenciaResumo(
            tenant_id=row['tenant_id'],
            envio_data=row['envio_data'],
            simulation_id=row['simulation_id'],
            k_clusters=row['k_clusters'],
            is_ponto_otimo=row['is_ponto_otimo'],
            rota_id=row['rota_id'],
            tipo_veiculo=row['tipo_veiculo'],
            qde_entregas=int(row['qde_entregas']),
            peso_total_kg=float(row['peso_total_kg']),
            volumes_total=int(row['qde_volumes']),
            valor_total_nf=float(row['valor_total_nf']),
            qde_clusters_rota=int(row['qde_clusters_rota']),
            distancia_total_km=0.0,
            tempo_total_min=0.0,
            distancia_parcial_km=0.0,
            aproveitamento_percentual=aproveitamento,
            coordenadas_seq=coordenadas_seq
        )
        lista_resumo.append(resumo)

    logger.info(f"📦 {len(lista_resumo)} resumos de transferência gerados.")
    return lista_resumo
