import requests
import datetime
import json
import pandas as pd

# Configurações
URL = "http://localhost:8011/predict"
TENANT_ID = "38ed0dcc-a92b-4c07-85e7-b59e5939b84c"
DATASET_NAME = "historico_2024_2025"
TARGET = "custo_total"
ALGORITHM = "RandomForestRegressor"

# Data de referência
today = datetime.date.today().strftime("%Y-%m-%d")

# Carregar cenários de entrada
with open("/app/src/scenarios.json", "r") as f:
    scenarios = json.load(f)


payload = {
    "tenant_id": TENANT_ID,
    "dataset_name": DATASET_NAME,
    "target_column": TARGET,
    "features": scenarios,
    "algorithm": ALGORITHM
}

print(f"📌 Rodando predições ({len(scenarios)} cenários) - {today}")

resp = requests.post(URL, json=payload)

if resp.status_code == 200:
    results = resp.json()["predictions"]
    # Junta cenários + resultados em um DataFrame
    df = pd.DataFrame(scenarios)
    df["prediction"] = [r["prediction"] for r in results]

    output_path = f"/app/src/predictions_{today}.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Predições salvas em {output_path}")
    print(df)
else:
    print("❌ Erro no predict:", resp.status_code, resp.text)
