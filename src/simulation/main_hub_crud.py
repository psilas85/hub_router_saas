#hub_router_1.0.1/src/simulation/main_hub_crud.py

import argparse
from simulation.config import conectar_db_simulation, setup_logger

def adicionar_hub(cursor, tenant_id, nome, endereco, latitude, longitude):
    cursor.execute("""
        INSERT INTO hubs (tenant_id, nome, endereco, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (tenant_id, endereco) DO NOTHING
    """, (tenant_id, nome, endereco, latitude, longitude))
    print("✅ Hub adicionado com sucesso.")

def remover_hub(cursor, tenant_id, endereco):
    cursor.execute("""
        DELETE FROM hubs
        WHERE tenant_id = %s AND endereco = %s
    """, (tenant_id, endereco))
    print("🗑️ Hub removido com sucesso.")

def listar_hubs(cursor, tenant_id):
    cursor.execute("""
        SELECT id, nome, endereco, latitude, longitude
        FROM hubs
        WHERE tenant_id = %s
    """, (tenant_id,))
    hubs = cursor.fetchall()
    print("📋 Hubs cadastrados:")
    for hub in hubs:
        print(hub)

def main():
    parser = argparse.ArgumentParser(description="CRUD de Hubs da Simulação")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--acao", required=True, choices=["add", "remove", "list"], help="Ação: add, remove ou list")
    parser.add_argument("--nome", help="Nome do hub (obrigatório para add)")
    parser.add_argument("--endereco", help="Endereço completo do hub")
    parser.add_argument("--lat", type=float, help="Latitude (obrigatório para add)")
    parser.add_argument("--lon", type=float, help="Longitude (obrigatório para add)")

    args = parser.parse_args()
    logger = setup_logger()

    conn = conectar_db_simulation()
    cursor = conn.cursor()

    try:
        if args.acao == "add":
            if not (args.nome and args.endereco and args.lat and args.lon):
                print("❌ Para adicionar, informe --nome, --endereco, --lat e --lon")
            else:
                adicionar_hub(cursor, args.tenant, args.nome, args.endereco, args.lat, args.lon)
        elif args.acao == "remove":
            if not args.endereco:
                print("❌ Para remover, informe --endereco")
            else:
                remover_hub(cursor, args.tenant, args.endereco)
        elif args.acao == "list":
            listar_hubs(cursor, args.tenant)

        conn.commit()
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
