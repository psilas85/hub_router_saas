# costs_transfer/transfer_cost_crud.py

import logging
from costs_transfer.infrastructure.transfer_cost_repository import TransferCostRepository

class TransferCostCRUD:
    def __init__(self, tenant_id: str):
        self.repository = TransferCostRepository()
        self.tenant_id = tenant_id

    def menu(self):
        while True:
            print(f"\n🚚 **Gestão de Custos de Transferência | Tenant: {self.tenant_id}** 🚚")
            print("1️⃣ - Listar custos")
            print("2️⃣ - Adicionar novo custo")
            print("3️⃣ - Editar custo existente")
            print("4️⃣ - Remover custo")
            print("5️⃣ - Sair")
            
            opcao = input("Escolha uma opção: ")

            if opcao == "1":
                self.listar_custos()
            elif opcao == "2":
                self.adicionar_custo()
            elif opcao == "3":
                self.editar_custo()
            elif opcao == "4":
                self.remover_custo()
            elif opcao == "5":
                break
            else:
                print("❌ Opção inválida. Tente novamente.")

    def listar_custos(self):
        df = self.repository.listar_custos_transferencia(self.tenant_id)
        if df.empty:
            print("⚠️ Nenhum custo cadastrado.")
        else:
            print("\n📊 Tabela de Custos de Transferência:")
            print(df.to_string(index=False))

    def adicionar_custo(self):
        tipo_veiculo = input("🚗 Tipo de veículo: ")
        custo_km = float(input("💰 Custo por km (R$): "))
        capacidade_min = float(input("📦 Capacidade mínima (kg): "))
        capacidade_max = float(input("📦 Capacidade máxima (kg): "))
        self.repository.adicionar_custo_transferencia(
            tipo_veiculo, custo_km, capacidade_min, capacidade_max, self.tenant_id
        )
        print(f"✅ Custo para '{tipo_veiculo}' cadastrado com sucesso!")

    def editar_custo(self):
        tipo_veiculo = input("✏️ Tipo de veículo a editar: ")
        novo_custo_km = float(input("🔄 Novo custo por km (R$): "))
        nova_cap_min = float(input("📦 Nova capacidade mínima (kg): "))
        nova_cap_max = float(input("📦 Nova capacidade máxima (kg): "))
        self.repository.editar_custo_transferencia(
            tipo_veiculo, novo_custo_km, nova_cap_min, nova_cap_max, self.tenant_id
        )
        print(f"✅ Custo do veículo '{tipo_veiculo}' atualizado com sucesso!")

    def remover_custo(self):
        tipo_veiculo = input("🗑️ Tipo de veículo a remover: ")
        confirmacao = input(f"⚠️ Confirmar remoção do custo de '{tipo_veiculo}'? (s/n): ")
        if confirmacao.lower() == "s":
            self.repository.remover_custo_transferencia(tipo_veiculo, self.tenant_id)
            print(f"✅ Custo de '{tipo_veiculo}' removido com sucesso!")
        else:
            print("❌ Operação cancelada.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tenant_id = input("🏢 Informe o tenant_id: ").strip()
    crud = TransferCostCRUD(tenant_id)
    crud.menu()
