#hub_router_1.0.1/src/simulation/main_tarifas_crud.py

import argparse
from simulation.infrastructure.simulation_database_connection import conectar_simulation_db
from simulation.infrastructure.simulation_database_writer import (
    inserir_ou_atualizar_tarifa_last_mile,
    remover_tarifa_last_mile,
    inserir_ou_atualizar_tarifa_transferencia,
    remover_tarifa_transferencia,
)
from simulation.infrastructure.simulation_database_reader import (
    listar_tarifas_last_mile,
    listar_tarifas_transferencia,
)

def menu_principal():
    print("\n🛠️  GERENCIADOR DE TARIFAS")
    print("1 - Listar tarifas")
    print("2 - Inserir/Atualizar tarifa")
    print("3 - Remover tarifa")
    print("0 - Sair")
    return input("Escolha uma opção: ").strip()

def escolher_tipo():
    print("\n💡 Tipo de tarifa")
    print("1 - Last-mile")
    print("2 - Transferência")
    tipo_opcao = input("Escolha o tipo: ").strip()
    return "last_mile" if tipo_opcao == "1" else "transferencia"

def interativo(tenant_id):
    db = conectar_simulation_db()

    while True:
        opcao = menu_principal()

        if opcao == "0":
            print("👋 Encerrando...")
            break

        tipo = escolher_tipo()

        if opcao == "1":  # Listar
            if tipo == "last_mile":
                df = listar_tarifas_last_mile(db, tenant_id)
            else:
                df = listar_tarifas_transferencia(db, tenant_id)

            if df.empty:
                print("⚠️ Nenhuma tarifa cadastrada.")
            else:
                print("\n📋 Tarifas encontradas:\n")
                print(df.to_string(index=False))

        elif opcao == "2":  # Inserir/Atualizar
            veiculo = input("Tipo de veículo: ").strip().lower()
            kg_min = float(input("Capacidade mínima (kg): ").strip())
            kg_max = float(input("Capacidade máxima (kg): ").strip())
            tarifa_km = float(input("Tarifa por km: ").strip())

            if tipo == "last_mile":
                tarifa_entrega = float(input("Tarifa por entrega: ").strip())
                inserir_ou_atualizar_tarifa_last_mile(db, tenant_id, veiculo, kg_min, kg_max, tarifa_km, tarifa_entrega)
                print(f"✅ Tarifa last-mile para '{veiculo}' inserida/atualizada.")
            else:
                tarifa_fixa = float(input("Tarifa fixa: ").strip())
                inserir_ou_atualizar_tarifa_transferencia(db, tenant_id, veiculo, kg_min, kg_max, tarifa_km, tarifa_fixa)
                print(f"✅ Tarifa de transferência para '{veiculo}' inserida/atualizada.")

        elif opcao == "3":  # Remover
            veiculo = input("Tipo de veículo a remover: ").strip().lower()
            if tipo == "last_mile":
                sucesso = remover_tarifa_last_mile(db, veiculo)
                if sucesso:
                    print(f"🗑️ Tarifa last-mile para '{veiculo}' removida.")
                else:
                    print(f"⚠️ Nenhuma tarifa last-mile encontrada para '{veiculo}'.")
            else:
                sucesso = remover_tarifa_transferencia(db, veiculo)
                if sucesso:
                    print(f"🗑️ Tarifa de transferência para '{veiculo}' removida.")
                else:
                    print(f"⚠️ Nenhuma tarifa de transferência encontrada para '{veiculo}'.")

        else:
            print("❌ Opção inválida. Tente novamente.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tenant", required=True, help="Tenant ID para gerenciar tarifas")
    args = parser.parse_args()

    interativo(args.tenant)
