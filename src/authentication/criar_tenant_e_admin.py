# src/authentication/criar_tenant_e_admin.py

import re
import uuid
from datetime import datetime

from authentication.utils.password_utils import gerar_hash_senha
from authentication.infrastructure.database_connection import (
    conectar_banco_cluster as conectar_banco,
)

PLANOS_PERMITIDOS = {"basic", "simulation"}


def _cnpj_limpo(v: str) -> str:
    return re.sub(r"\D", "", v or "")


def criar_tenant_e_admin_interativo():
    print("\n🚀 Criação de novo tenant e usuário administrador (cliente_admin)")

    # ---- Inputs
    nome_empresa = input("🏢 Nome da empresa: ").strip()
    cnpj = _cnpj_limpo(input("📄 CNPJ (com ou sem máscara): ").strip())

    while True:
        plano = input("📦 Plano [basic / simulation]: ").strip().lower()
        if plano in PLANOS_PERMITIDOS:
            break
        print("❌ Plano inválido. Digite 'basic' ou 'simulation'.")

    admin_nome = input("👤 Nome do administrador: ").strip()
    admin_email = input("📧 Email do administrador: ").strip()
    admin_senha = input("🔐 Senha do administrador: ").strip()

    # ---- Validações
    if len(cnpj) != 14:
        print("❌ CNPJ inválido. Deve conter 14 dígitos.")
        return
    if not nome_empresa or not admin_nome or not admin_email or not admin_senha:
        print("❌ Preencha todos os campos.")
        return

    tenant_id = str(uuid.uuid4())
    usuario_id = str(uuid.uuid4())
    senha_hash = gerar_hash_senha(admin_senha)
    agora = datetime.utcnow()

    conn = conectar_banco()
    try:
        with conn.cursor() as cur:
            # Duplicidades
            cur.execute("SELECT 1 FROM tenants WHERE cnpj = %s LIMIT 1", (cnpj,))
            if cur.fetchone():
                print("❌ Já existe um tenant com este CNPJ.")
                return

            cur.execute("SELECT 1 FROM usuarios WHERE email = %s LIMIT 1", (admin_email,))
            if cur.fetchone():
                print("❌ Já existe um usuário com este e-mail.")
                return

            # Inserir tenant
            cur.execute(
                """
                INSERT INTO tenants (id, nome, cnpj, plano, ativo, criado_em)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (tenant_id, nome_empresa, cnpj, plano, True, agora),
            )

            # Inserir admin do tenant (cliente_admin)
            cur.execute(
                """
                INSERT INTO usuarios (id, tenant_id, nome, email, senha_hash, role, ativo, criado_em)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    usuario_id,
                    tenant_id,
                    admin_nome,
                    admin_email,
                    senha_hash,
                    "cliente_admin",
                    True,
                    agora,
                ),
            )

        conn.commit()
        print("\n✅ Tenant e usuário administrador criados com sucesso!")
        print(f"   🆔 tenant_id: {tenant_id}")
        print(f"   👤 admin_id : {usuario_id}")
        print(f"   📧 e-mail   : {admin_email}")
        print("\nℹ️  O admin criado possui a role 'cliente_admin' (escopo do tenant).")
    except Exception as e:
        conn.rollback()
        print("❌ Erro ao criar tenant/admin:", e)
    finally:
        conn.close()


if __name__ == "__main__":
    criar_tenant_e_admin_interativo()
