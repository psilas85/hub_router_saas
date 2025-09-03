#data_input/infrastructure/database_connection.py

from data_input.infrastructure.db import Database

def conectar_banco():
    db = Database()
    db.conectar()
    return db.conexao

def fechar_conexao(conexao):
    if conexao:
        conexao.close()
