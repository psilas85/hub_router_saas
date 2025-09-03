import argparse
import logging
import pandas as pd
from tabulate import tabulate
from datetime import datetime

from clusterization.infrastructure.database_connection import (
    conectar_banco_routing,
    fechar_conexao
)


# === Configuração de log ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("main_crud_hubs")


# === Funções CRUD ===

def listar_hubs(conexao, tenant_id):
    query = """
    SELECT id, hub_central_nome, endereco, hub_central_latitude, hub_central_longitude,
           ativo, tenant_id, criado_em, atualizado_em
    FROM hubs_central
    WHERE tenant_id = %s
    ORDER BY id;
    """
    try:
        df = pd.read_sql(query, conexao, params=(tenant_id,))
        if df.empty:
            logger.info("⚠️ Nenhum hub cadastrado.")
        else:
            print(tabulate(df, headers='keys', tablefmt='psql'))
        return df
    except Exception as e:
        logger.error(f"❌ Erro ao listar hubs: {e}")


def inserir_hub(conexao, tenant_id, nome, endereco, latitude, longitude):
    query = """
    INSERT INTO hubs_central (
        hub_central_nome, endereco, hub_central_latitude, hub_central_longitude,
        tenant_id, ativo, criado_em, atualizado_em
    ) VALUES (%s, %s, %s, %s, %s, TRUE, NOW(), NOW())
    RETURNING id;
    """
    try:
        with conexao.cursor() as cursor:
            cursor.execute(query, (nome, endereco, latitude, longitude, tenant_id))
            hub_id = cursor.fetchone()[0]
            conexao.commit()
            logger.info(f"✅ Hub '{nome}' inserido com sucesso (ID={hub_id}).")
    except Exception as e:
        conexao.rollback()
        logger.error(f"❌ Erro ao inserir hub: {e}")


def atualizar_hub(conexao, hub_id, nome=None, endereco=None, latitude=None, longitude=None, ativo=None):
    campos = []
    valores = []

    if nome:
        campos.append("hub_central_nome = %s")
        valores.append(nome)
    if endereco:
        campos.append("endereco = %s")
        valores.append(endereco)
    if latitude:
        campos.append("hub_central_latitude = %s")
        valores.append(latitude)
    if longitude:
        campos.append("hub_central_longitude = %s")
        valores.append(longitude)
    if ativo is not None:
        campos.append("ativo = %s")
        valores.append(ativo)

    if not campos:
        logger.warning("⚠️ Nenhum campo para atualizar.")
        return

    campos.append("atualizado_em = NOW()")

    query = f"""
    UPDATE hubs_central
    SET {', '.join(campos)}
    WHERE id = %s;
    """
    valores.append(hub_id)

    try:
        with conexao.cursor() as cursor:
            cursor.execute(query, valores)
            conexao.commit()
            logger.info(f"✅ Hub ID={hub_id} atualizado com sucesso.")
    except Exception as e:
        conexao.rollback()
        logger.error(f"❌ Erro ao atualizar hub: {e}")


def deletar_hub(conexao, hub_id):
    query = "DELETE FROM hubs_central WHERE id = %s;"
    try:
        with conexao.cursor() as cursor:
            cursor.execute(query, (hub_id,))
            conexao.commit()
            logger.info(f"🗑️ Hub ID={hub_id} deletado com sucesso.")
    except Exception as e:
        conexao.rollback()
        logger.error(f"❌ Erro ao deletar hub: {e}")


# === Argumentos ===
parser = argparse.ArgumentParser(description="🗺️ Gerenciamento de hubs_central no banco routing.")

parser.add_argument("--tenant", required=True, help="Tenant ID")

subparsers = parser.add_subparsers(dest="comando", help="Comando CRUD")

# Listar
subparsers.add_parser("listar", help="Listar hubs existentes")

# Inserir
parser_inserir = subparsers.add_parser("inserir", help="Inserir um novo hub")
parser_inserir.add_argument("--nome", required=True, help="Nome do hub")
parser_inserir.add_argument("--endereco", required=True, help="Endereço completo")
parser_inserir.add_argument("--latitude", type=float, required=True, help="Latitude")
parser_inserir.add_argument("--longitude", type=float, required=True, help="Longitude")

# Atualizar
parser_atualizar = subparsers.add_parser("atualizar", help="Atualizar um hub existente")
parser_atualizar.add_argument("--id", type=int, required=True, help="ID do hub a ser atualizado")
parser_atualizar.add_argument("--nome", help="Novo nome")
parser_atualizar.add_argument("--endereco", help="Novo endereço")
parser_atualizar.add_argument("--latitude", type=float, help="Nova latitude")
parser_atualizar.add_argument("--longitude", type=float, help="Nova longitude")
parser_atualizar.add_argument("--ativo", type=int, choices=[0, 1], help="Ativo (1) ou Inativo (0)")

# Deletar
parser_deletar = subparsers.add_parser("deletar", help="Deletar um hub")
parser_deletar.add_argument("--id", type=int, required=True, help="ID do hub a ser deletado")

args = parser.parse_args()

# === Conexão ===
conexao = conectar_banco_routing()

# === Execução ===
if args.comando == "listar":
    listar_hubs(conexao, args.tenant)

elif args.comando == "inserir":
    inserir_hub(
        conexao, tenant_id=args.tenant, nome=args.nome, endereco=args.endereco,
        latitude=args.latitude, longitude=args.longitude
    )

elif args.comando == "atualizar":
    atualizar_hub(
        conexao, hub_id=args.id, nome=args.nome, endereco=args.endereco,
        latitude=args.latitude, longitude=args.longitude,
        ativo=bool(args.ativo) if args.ativo is not None else None
    )

elif args.comando == "deletar":
    deletar_hub(conexao, args.id)

else:
    parser.print_help()

fechar_conexao(conexao)
