import requests
import os

# 🚨 Ajuste para o host/porta reais do serviço data_input
BASE_URL = os.getenv("DATA_INPUT_URL", "http://localhost:8008")

# 🔑 Usa o mesmo token que você já usou no curl
TOKEN = os.getenv("AUTH_TOKEN", "COLE_SEU_TOKEN_AQUI")

def test_historico():
    url = f"{BASE_URL}/data_input/historico?limit=5"
    headers = {"Authorization": f"Bearer {TOKEN}"}

    print(f"👉 Requisitando {url}")
    resp = requests.get(url, headers=headers)
    print("Status:", resp.status_code)

    try:
        print("Resposta JSON:", resp.json())
    except Exception:
        print("Resposta bruta:", resp.text)


if __name__ == "__main__":
    test_historico()
