import logging
import psycopg2
from pathlib import Path
from simulation.domain.data_cleaner_service import DataCleanerService

# Configuração básica de logger para ver os prints
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_cleaner")

def main():
    tenant_id = "38ed0dcc-a92b-4c07-85e7-b59e5939b84c"
    envio_data = "2025-09-16"

    # Caminho base
    base_path = Path("/app/exports/simulation")

    # Simula conexão fake (não precisa DB se quiser só testar arquivos)
    class DummyConn:
        def cursor(self):
            class DummyCursor:
                def execute(self, *args, **kwargs): pass
                def close(self): pass
            return DummyCursor()
        def commit(self): pass
        def rollback(self): pass

    cleaner = DataCleanerService(
        db_conn=DummyConn(),
        tenant_id=tenant_id,
        envio_data=envio_data,
        logger=logger,
        output_dir=base_path
    )

    logger.info("📂 Antes da limpeza:")
    for sub in ["maps", "graphs"]:
        dir_path = base_path / sub / tenant_id
        if dir_path.exists():
            for f in dir_path.glob(f"*{envio_data}*"):
                logger.info(f"  - {f}")

    cleaner.limpar_artefatos()

    logger.info("📂 Depois da limpeza:")
    for sub in ["maps", "graphs"]:
        dir_path = base_path / sub / tenant_id
        if dir_path.exists():
            for f in dir_path.glob(f"*{envio_data}*"):
                logger.info(f"  - {f}")

if __name__ == "__main__":
    main()
