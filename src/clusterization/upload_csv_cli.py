# upload_csv_cli.py

import os
import shutil
import logging
import argparse
from data_input.application.data_preprocessing import DataPreprocessing
from infrastructure.database_reader import DatabaseReader
from infrastructure.database_writer import DatabaseWriter
from infrastructure.db import Database

# Configura o log
logging.basicConfig(level=logging.INFO)

def criar_estrutura_pastas(tenant_id):
    base_path = os.path.join("clusterization", "tenants", tenant_id)
    input_path = os.path.join(base_path, "input")
    output_path = os.path.join(base_path, "output")
    maps_path = os.path.join(base_path, "maps")

    os.makedirs(input_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    os.makedirs(maps_path, exist_ok=True)

    logging.info(f"📁 Estrutura de pastas garantida em: {base_path}")
    return input_path, output_path

def copiar_csv_para_input(caminho_csv_origem, input_path):
    if not os.path.exists(caminho_csv_origem):
        logging.error(f"❌ Arquivo CSV não encontrado: {caminho_csv_origem}")
        return None

    destino = os.path.join(input_path, "dados_input.csv")
    shutil.copy(caminho_csv_origem, destino)
    logging.info(f"✅ CSV copiado para: {destino}")
    return destino

def executar_pipeline(filepath_csv, tenant_id, output_path):
    db = Database()
    db.conectar()

    try:
        reader = DatabaseReader(db.conexao)
        writer = DatabaseWriter(db.conexao)

        preprocessor = DataPreprocessing(
            writer, reader, tenant_id=tenant_id,
            path_saida_invalidos=os.path.join(output_path, "registros_invalidos.csv")
        )

        df = preprocessor.execute(filepath_csv)

        if df is not None and not df.empty:
            logging.info(f"✅ Upload e processamento concluído com sucesso. Registros válidos: {len(df)}")
        else:
            logging.warning("⚠ Nenhum registro válido foi processado.")

    except Exception as e:
        logging.error(f"❌ Erro durante o processamento: {e}")
    finally:
        db.fechar_conexao()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload de CSV + processamento para tenant no Cluster Router")
    parser.add_argument("--csv", required=True, help="Caminho completo do CSV a ser enviado")
    parser.add_argument("--tenant", required=True, help="ID do tenant (ex: cliente1, dev_tenant)")

    args = parser.parse_args()

    input_path, output_path = criar_estrutura_pastas(args.tenant)
    destino_csv = copiar_csv_para_input(args.csv, input_path)

    if destino_csv:
        executar_pipeline(destino_csv, args.tenant, output_path)
