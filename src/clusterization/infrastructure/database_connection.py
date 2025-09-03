#clusterization/infrastructure/database_connection.py

from clusterization.infrastructure.db import Database


def conectar_banco():
    db = Database()
    db.conectar()
    return db.conexao


def conectar_banco_routing():
    db = Database()
    db.conectar(database_env_key="DB_DATABASE_ROUTING")
    return db.conexao


def fechar_conexao(conexao):
    if conexao:
        conexao.close()
