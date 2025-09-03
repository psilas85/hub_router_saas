# transfer_routing/infrastructure/vehicle_selector.py

def obter_tipo_veiculo_por_peso(peso_kg: float, tenant_id: str, conn) -> dict:
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT tipo_veiculo, capacidade_kg_min, capacidade_kg_max, custo_por_km
                FROM transfer_costs
                WHERE %s BETWEEN capacidade_kg_min AND capacidade_kg_max
                AND tenant_id = %s
                ORDER BY capacidade_kg_min ASC
                LIMIT 1
            """, (peso_kg, tenant_id))

            resultado = cursor.fetchone()

            if resultado:
                return {
                    "tipo_veiculo": resultado[0],
                    "capacidade_min": resultado[1],
                    "capacidade_max": resultado[2],
                    "custo_por_km": float(resultado[3])
                }
            else:
                print(f"Nenhum veículo encontrado para peso {peso_kg} kg (tenant: {tenant_id})")
                return None
    except Exception as e:
        print(f"Erro ao buscar veículo para peso {peso_kg}: {e}")
        return None

