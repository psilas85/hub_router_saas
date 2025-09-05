# hub_router_1.0.1/src/data_input/main_preprocessing.py

import argparse
import logging
import os
import json  # 👈 ADICIONADO para exportar resumo em JSON
from dotenv import load_dotenv

from data_input.application.data_preprocessing import DataPreprocessing
from data_input.infrastructure.database_writer import DatabaseWriter
from data_input.infrastructure.database_reader import DatabaseReader
from data_input.infrastructure.db import Database

# Configuração de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Carrega .env
load_dotenv()

def detectar_separador(caminho_arquivo):
    with open(caminho_arquivo, "r", encoding="utf-8-sig") as f:
        primeira_linha = f.readline()
        return ";" if ";" in primeira_linha else ","

def main():
    parser = argparse.ArgumentParser(description="Pré-processamento de dados por tenant.")
    parser.add_argument("--tenant", help="ID do tenant (pode vir do .env)")
    parser.add_argument("--limite-peso-kg", type=float, default=15_000.0, help="Peso máximo permitido por CTE (default: 15.000 kg)")
    parser.add_argument("--modo_forcar", action="store_true", help="Forçar reprocessamento mesmo sem arquivo")
    args = parser.parse_args()

    tenant_id = args.tenant or os.getenv("TENANT_ID")
    if not tenant_id:
        logging.error("❌ Tenant ID não informado via argumento ou .env")
        return

    limite_peso = args.limite_peso_kg
    modo_forcar = args.modo_forcar

    # Caminhos
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    base_path = os.path.join(project_root, "data_input", "tenants", tenant_id)
    input_path = os.path.join(base_path, "input", "dados_input.csv")
    output_invalidos_path = os.path.join(base_path, "output", "registros_invalidos.csv")
    output_acima_peso_path = os.path.join(base_path, "output", "ctes_acima_peso.csv")

    os.makedirs(os.path.dirname(output_invalidos_path), exist_ok=True)

    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo de entrada não encontrado: {input_path}")
        return

    logging.info(f"📂 Carregando dados do tenant '{tenant_id}' a partir de: {input_path}")

    try:
        db = Database()
        db.conectar()
        db_writer = DatabaseWriter(db.conexao)
        db_reader = DatabaseReader(db.conexao)

        preprocessor = DataPreprocessing(
            writer=db_writer,
            reader=db_reader,
            tenant_id=tenant_id,
            path_saida_invalidos=output_invalidos_path
        )

        # Detecta separador automaticamente
        sep = detectar_separador(input_path)
        logging.info(f"🔎 Separador detectado: '{sep}'")

        resultado = preprocessor.execute(input_path, sep=sep)


        if resultado is None:
            logging.warning("⚠ Nenhum registro válido foi processado.")
            return

        df, qtde_invalidos = resultado

        if not df.empty:
            acima_peso = df[df["cte_peso"] > limite_peso]
            if not acima_peso.empty:
                acima_peso.to_csv(output_acima_peso_path, index=False, encoding="utf-8-sig", sep=";")
                logging.warning(f"⚠️ {len(acima_peso)} CTE(s) com peso acima de {limite_peso:.0f} kg salvos em: {output_acima_peso_path}")
                for _, row in acima_peso.iterrows():
                    logging.warning(f"  🚛 CTE {row['cte_numero']} — Peso: {row['cte_peso']} kg")

            logging.info(f"✅ Processamento finalizado com {len(df)} registros válidos.")
            logging.info(f"📋 Resumo: {len(df)} válidos, {qtde_invalidos} inválidos.")
            print(f"Resumo: {len(df)} válidos, {qtde_invalidos} inválidos.")  # 👈 mantém o print humano

            # 👇 NOVO: saída em JSON para captura pelo Gateway
            total_processados = int(len(df) + (qtde_invalidos or 0))
            print(json.dumps({
                "total_processados": total_processados,
                "validos": int(len(df)),
                "invalidos": int(qtde_invalidos or 0)
            }))

            if hasattr(preprocessor, "registros_ignorados_uf") and preprocessor.registros_ignorados_uf > 0:
                logging.info(
                    f"📍 {preprocessor.registros_ignorados_uf} entrega(s) foram ignoradas por coordenadas fora da UF esperada "
                    f"e salvas em: {preprocessor.path_saida_coord_invalidas}"
                )
        else:
            logging.warning("⚠ Nenhum registro válido foi processado.")

    except Exception as e:
        logging.error(f"❌ Erro inesperado durante o pré-processamento: {e}", exc_info=True)

if __name__ == "__main__":
    main()
