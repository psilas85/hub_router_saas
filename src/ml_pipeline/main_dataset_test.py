import os
from dotenv import load_dotenv
from ml_pipeline.infrastructure.dataset_repository import DatasetRepository

def main():
    load_dotenv()

    db_config = {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }

    repo = DatasetRepository(db_config)

    start_date = "2024-08-01"
    end_date = "2024-08-31"
    tenant_id = os.getenv("DEFAULT_TENANT_ID", "default-tenant")

    df = repo.load_simulation_dataset(start_date, end_date, tenant_id)

    print("✅ Dataset carregado!")
    print(df.head())
    print(f"Total linhas: {len(df)}")

if __name__ == "__main__":
    main()
