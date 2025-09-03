# exploratory_analysis/main_eda.py

import argparse
from exploratory_analysis.infrastructure.database_reader import carregar_entregas
from exploratory_analysis.domain.eda_pre_clusterization import executar_analise
import sys

def parse_faixa_cores(faixa_str):
    faixa_dict = {}
    try:
        for item in faixa_str.split(","):
            min_v, max_v, cor = item.split(":")
            faixa_dict[(float(min_v), float(max_v))] = cor
    except Exception as e:
        print(f"❌ Erro ao interpretar faixa de cores: {e}")
        sys.exit(1)
    return faixa_dict

def main():
    parser = argparse.ArgumentParser(description="Executa análise exploratória de entregas")
    parser.add_argument("--tenant", required=True, help="ID do tenant (pasta de input)")
    parser.add_argument("--data-inicial", required=True, help="Data inicial no formato YYYY-MM-DD")
    parser.add_argument("--data-final", required=True, help="Data final no formato YYYY-MM-DD")
    parser.add_argument("--granularidade", choices=["diária", "mensal", "anual"], default="mensal", help="Granularidade da análise")
    parser.add_argument(
        "--faixa-cores",
        type=str,
        default="0:800:green,801:2000:orange,2001:999999:red",
        help="Faixas de valor da NF e cores no formato 'min:max:cor,...'"
    )
    parser.add_argument(
        "--incluir-outliers",
        action="store_true",
        help="Incluir análise de outliers estatísticos e geográficos"
    )

    args = parser.parse_args()

    print("🔍 Carregando dados...")
    df = carregar_entregas(args.data_inicial, args.data_final, args.tenant)

    if df.empty:
        print("⚠️ Nenhuma entrega encontrada no período informado.")
        return

    faixa_cores = parse_faixa_cores(args.faixa_cores)

    print(f"📊 Executando EDA com granularidade '{args.granularidade}'...")
    executar_analise(df, args.granularidade, faixa_cores, incluir_outliers=args.incluir_outliers)

    print("✅ Análise finalizada com sucesso.")

if __name__ == "__main__":
    main()
