# simulation/main_cluster_cost_crud.py

import psycopg2
import argparse
from dotenv import load_dotenv
import os
from pathlib import Path

# Força o caminho absoluto para o .env dentro de simulation/
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

def conectar():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("SIMULATION_DB"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


def inserir(tenant_id, limite, fixo, variavel):
    conn = conectar()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cluster_costs (tenant_id, limite_qtd_entregas, custo_fixo_diario, custo_variavel_por_entrega)
        VALUES (%s, %s, %s, %s)
    """, (tenant_id, limite, fixo, variavel))
    conn.commit()
    conn.close()
    print("✅ Custo de cluster inserido com sucesso.")

def listar(tenant_id):
    conn = conectar()
    cur = conn.cursor()
    cur.execute("SELECT * FROM cluster_costs WHERE tenant_id = %s", (tenant_id,))
    for row in cur.fetchall():
        print(row)
    conn.close()

def apagar(id):
    conn = conectar()
    cur = conn.cursor()
    cur.execute("DELETE FROM cluster_costs WHERE id = %s", (id,))
    conn.commit()
    conn.close()
    print("🗑️ Registro removido.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--acao", choices=["inserir", "listar", "apagar"], required=True)
    parser.add_argument("--tenant_id")
    parser.add_argument("--limite", type=int)
    parser.add_argument("--fixo", type=float)
    parser.add_argument("--variavel", type=float)
    parser.add_argument("--id", type=int)

    args = parser.parse_args()

    if args.acao == "inserir":
        inserir(args.tenant_id, args.limite, args.fixo, args.variavel)
    elif args.acao == "listar":
        listar(args.tenant_id)
    elif args.acao == "apagar":
        apagar(args.id)
