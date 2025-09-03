#costs_last_mile/cost_crud.py

import logging
from costs_last_mile.infrastructure.cost_repository_last_mile import CostRepository

class CostCRUD:
    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id
        self.repository = CostRepository()

    def menu(self):
        while True:
            print(f"\n🚚 **Gestão de Custos de Veículos | Tenant: {self.tenant_id}** 🚚")
            print("1️⃣ - Listar custos de veículos")
            print("2️⃣ - Adicionar novo custo")
            print("3️⃣ - Editar custo existente")
            print("4️⃣ - Remover custo")
            print("5️⃣ - Sair")

            opcao = input("Escolha uma opção: ").strip()

            if opcao == "1":
                self.listar_custos()
            elif opcao == "2":
                self.adicionar_custo()
            elif opcao == "3":
                self.editar_custo()
            elif opcao == "4":
                self.remover_custo()
            elif opcao == "5":
                print("👋 Encerrando menu de gestão de custos...")
                break
            else:
                print("❌ Opção inválida. Tente novamente.")

    def listar_custos(self):
        df = self.repository.buscar_custos_veiculo(self.tenant_id)
        if df.empty:
            print("⚠️ Nenhum custo cadastrado.")
        else:
            print("\n📊 Tabela de Custos:")
            print(df[["veiculo", "peso_minimo_kg", "peso_maximo_kg", "custo_por_km", "custo_por_entrega"]].to_string(index=False))

    def adicionar_custo(self):
        veiculo = input("🚗 Veículo: ").strip()
        try:
            peso_minimo_kg = float(input("⚖️ Peso mínimo (kg): "))
            peso_maximo_kg = float(input("⚖️ Peso máximo (kg): "))
            custo_km = float(input("💰 Custo por km (R$): "))
            custo_entrega = float(input("📦 Custo por entrega (R$): "))
            self.repository.adicionar_custo_veiculo(veiculo, custo_km, custo_entrega, peso_minimo_kg, peso_maximo_kg, self.tenant_id)
            print(f"✅ Custo para '{veiculo}' cadastrado com sucesso.")
        except ValueError:
            print("❌ Valor inválido. Operação cancelada.")

    def editar_custo(self):
        veiculo = input("🚗 Veículo a editar: ").strip()
        try:
            peso_min = float(input("⚖️ Novo peso mínimo (kg): "))
            peso_max = float(input("⚖️ Novo peso máximo (kg): "))
            novo_km = float(input("💰 Novo custo por km (R$): "))
            novo_entrega = float(input("📦 Novo custo por entrega (R$): "))
            self.repository.editar_custo_veiculo(veiculo, novo_km, novo_entrega, peso_min, peso_max, self.tenant_id)
            print(f"✅ Custo do veículo '{veiculo}' atualizado com sucesso.")
        except ValueError:
            print("❌ Valor inválido. Operação cancelada.")

    def remover_custo(self):
        veiculo = input("🚗 Veículo a remover: ").strip()
        confirmacao = input(f"⚠️ Confirmar remoção do custo de '{veiculo}'? (s/n): ").strip().lower()
        if confirmacao == "s":
            self.repository.remover_custo_veiculo(veiculo, self.tenant_id)
            print(f"✅ Custo de '{veiculo}' removido com sucesso.")
        else:
            print("❌ Operação cancelada.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    tenant_id = input("🏢 Informe o tenant_id: ").strip()
    crud = CostCRUD(tenant_id)
    crud.menu()
