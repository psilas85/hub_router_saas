import requests
import datetime

# Configurações fixas
URL = "http://localhost:8011/train"
TENANT_ID = "38ed0dcc-a92b-4c07-85e7-b59e5939b84c"
DATASET_NAME = "historico_2024_2025"
TARGET = "custo_total"
ALGORITHM = "RandomForestRegressor"

# Datas: início fixo e fim = ontem
start_date = "2024-08-01"
end_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

payload = {
    "tenant_id": TENANT_ID,
    "dataset_name": DATASET_NAME,
    "target_column": TARGET,
    "start_date": start_date,
    "end_date": end_date,
    "algorithm": ALGORITHM
}

print(f"📅 Treinando de {start_date} até {end_date}...")

resp = requests.post(URL, json=payload)

if resp.status_code == 200:
    print("✅ Treino concluído com sucesso!")
    print("Resposta:", resp.json())
else:
    print("❌ Erro no treino:", resp.status_code, resp.text)
