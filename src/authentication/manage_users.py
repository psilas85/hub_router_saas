# hub_router_1.0.1/src/authentication/manage_users.py

import argparse
import uuid
from datetime import datetime
from authentication.utils.password_utils import gerar_hash_senha
from authentication.infrastructure.database_connection import conectar_banco, fechar_conexao

def criar_usuario(nome, email, senha, role, tenant_id=None):
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        usuario_id = str(uuid.uuid4())
        senha_hash = gerar_hash_senha(senha)
        agora = datetime.utcnow()

        cur.execute("""
            INSERT INTO usuarios (id, tenant_id, nome, email, senha_hash, role, ativo, criado_em)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (usuario_id, tenant_id, nome, email, senha_hash, role, True, agora))

        conn.commit()
        print(f"✅ Usuário {nome} ({role}) criado com sucesso. ID={usuario_id}")
    except Exception as e:
        conn.rollback()
        print("❌ Erro ao criar usuário:", e)
    finally:
        cur.close()
        fechar_conexao(conn)


def criar_tenant(nome_empresa, cnpj, plano):
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        tenant_id = str(uuid.uuid4())
        agora = datetime.utcnow()

        cur.execute("""
            INSERT INTO tenants (id, nome, cnpj, plano, ativo, criado_em)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (tenant_id, nome_empresa, cnpj, plano, True, agora))

        conn.commit()
        print(f"✅ Tenant {nome_empresa} criado com sucesso. ID={tenant_id}")
        return tenant_id
    except Exception as e:
        conn.rollback()
        print("❌ Erro ao criar tenant:", e)
    finally:
        cur.close()
        fechar_conexao(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gerenciamento de usuários e tenants HubRouter")

    subparsers = parser.add_subparsers(dest="command")

    # Criar Tenant
    tenant_parser = subparsers.add_parser("create-tenant", help="Criar novo tenant")
    tenant_parser.add_argument("--nome", required=True, help="Nome da empresa")
    tenant_parser.add_argument("--cnpj", required=True, help="CNPJ da empresa")
    tenant_parser.add_argument("--plano", required=True, choices=["basic", "simulation"], help="Plano")

    # Criar Usuário
    user_parser = subparsers.add_parser("create-user", help="Criar novo usuário")
    user_parser.add_argument("--nome", required=True, help="Nome do usuário")
    user_parser.add_argument("--email", required=True, help="Email do usuário")
    user_parser.add_argument("--senha", required=True, help="Senha do usuário")
    user_parser.add_argument("--role", required=True, choices=[
        "hub_admin", "hub_operacional", "cliente_admin", "cliente_operacional"
    ], help="Papel do usuário")
    user_parser.add_argument("--tenant_id", required=False, help="Tenant ID (necessário para cliente_admin e cliente_operacional)")

    args = parser.parse_args()

    if args.command == "create-tenant":
        criar_tenant(args.nome, args.cnpj, args.plano)

    elif args.command == "create-user":
        # 🔒 Valida coerência entre role e tenant_id
        if args.role in ["cliente_admin", "cliente_operacional"] and not args.tenant_id:
            print("❌ Para criar um usuário do tipo cliente_admin ou cliente_operacional é obrigatório informar --tenant_id")
            exit(1)

        if args.role in ["hub_admin", "hub_operacional"] and args.tenant_id:
            print("❌ Usuários do tipo hub_admin ou hub_operacional NÃO devem ter --tenant_id")
            exit(1)

        criar_usuario(args.nome, args.email, args.senha, args.role, args.tenant_id)

    else:
        parser.print_help()
