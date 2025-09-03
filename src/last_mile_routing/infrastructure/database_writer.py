#last_mile_routing/infrastructure/database_writer.py

import json
from datetime import datetime

from last_mile_routing.infrastructure.database_connection import fechar_conexao


import json


def salvar_last_mile_rota(conn, rota, tenant_id):
    cursor = conn.cursor()

    query = """
        INSERT INTO last_mile_rotas (
            tenant_id, envio_data, rota_id, cluster, sub_cluster, centro_lat, centro_lon,
            distancia_parcial_km, distancia_total_km, tempo_parcial_min, tempo_total_min,
            peso_total_kg, volumes_total, veiculo, dados_json, entregas, rota_coord
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    params = (
        tenant_id,
        rota["envio_data"],
        rota["rota_id"],
        rota["cluster"],
        rota["sub_cluster"],
        rota["centro_lat"],
        rota["centro_lon"],
        rota["distancia_parcial_km"],
        rota["distancia_total_km"],
        rota["tempo_parcial_min"],
        rota["tempo_total_min"],
        rota["peso_total_kg"],
        rota["volumes_total"],
        rota["veiculo"],
        json.dumps(rota),  # dados_json
        json.dumps(rota.get("entregas", [])),
        json.dumps(rota.get("rota_coord", []))
    )

    cursor.execute(query, params)
    conn.commit()
    cursor.close()



def excluir_roteirizacao_anterior(conexao, tenant_id, envio_data):
    cursor = conexao.cursor()

    try:
        tabelas = ["detalhes_rotas", "last_mile_rotas"]
        for tabela in tabelas:
            query = f"""
                DELETE FROM {tabela}
                WHERE tenant_id = %s AND envio_data = %s
            """
            cursor.execute(query, (tenant_id, envio_data))
            print(f"🗑️ Dados apagados da tabela {tabela} para envio_data={envio_data}")
        conexao.commit()

    except Exception as e:
        print(f"❌ Erro ao excluir roteirização anterior: {e}")
        conexao.rollback()

    finally:
        cursor.close()


def salvar_detalhe_rota(conexao, detalhe, tenant_id):
    cursor = conexao.cursor()
    try:
        query = """
            INSERT INTO detalhes_rotas (
                tenant_id, envio_data, rota_id, cluster, sub_cluster,
                cte_numero, ordem_entrega,
                centro_lat, centro_lon,
                destino_latitude, destino_longitude,
                coordenadas_seq, distancia_km,
                tempo_transito_min, tempo_total_min, veiculo,
                peso_kg, volumes, valor_nf, valor_frete,
                criado_em
            )
            VALUES (%s, %s, %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    now())
        """
        valores = (
            tenant_id,
            detalhe["envio_data"],
            detalhe["rota_id"],
            detalhe["cluster"],
            detalhe["sub_cluster"],
            detalhe["cte_numero"],
            detalhe["ordem_entrega"],
            detalhe["centro_lat"],
            detalhe["centro_lon"],
            detalhe["destino_latitude"],
            detalhe["destino_longitude"],
            json.dumps(detalhe["coordenadas_seq"]),
            detalhe["distancia_km"],
            detalhe["tempo_transito_min"],
            detalhe["tempo_total_min"],
            detalhe["veiculo"],
            detalhe["peso_kg"],
            detalhe["volumes"],
            detalhe["valor_nf"],
            detalhe["valor_frete"],
        )
        cursor.execute(query, valores)
        conexao.commit()
    except Exception as e:
        print(f"❌ Erro ao salvar detalhe da rota {detalhe['rota_id']}: {e}")
        conexao.rollback()
    finally:
        cursor.close()