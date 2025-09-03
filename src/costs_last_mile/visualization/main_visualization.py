#costs_last_mile/visualization/main_visualization.py

import argparse
from costs_last_mile.visualization.gerar_detalhes_last_mile import gerar_detalhes_last_mile
from costs_last_mile.visualization.gerar_resumo_last_mile import gerar_resumo_last_mile
from costs_last_mile.visualization.gerar_relatorio_last_mile import gerar_relatorio_last_mile
from costs_last_mile.visualization.logging_factory import get_logger

logger = get_logger("costs_last_mile")

def main():
    parser = argparse.ArgumentParser(description="Gerar relatórios e resumos de custos last mile")
    parser.add_argument("--tenant", required=True, help="Tenant ID")
    parser.add_argument("--data", required=True, help="Data de envio (YYYY-MM-DD)")
    args = parser.parse_args()

    logger.info(f"📦 Iniciando geração de relatórios para tenant '{args.tenant}' e data '{args.data}'")
    try:
        df_detalhes = gerar_detalhes_last_mile(args.tenant, args.data)
        df_resumo = gerar_resumo_last_mile(args.tenant, args.data)
        gerar_relatorio_last_mile(envio_data=args.data, tenant_id=args.tenant, df_detalhes=df_detalhes, df_resumo=df_resumo)
        logger.info("✅ Processamento finalizado com sucesso.")
    except Exception as e:
        logger.error(f"❌ Erro durante a geração dos relatórios: {e}")

if __name__ == "__main__":
    main()
