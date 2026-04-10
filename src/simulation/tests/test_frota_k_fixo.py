import pytest
from fastapi.testclient import TestClient
from simulation.api.routes import router
from fastapi import FastAPI

# 🔹 monta app só com o router de simulação
app = FastAPI()
app.include_router(router)

client = TestClient(app)

@pytest.fixture
def token_header():
    # 🔹 aqui você pode colocar um mock fixo do token ou só um header vazio
    return {"Authorization": "Bearer mocktoken"}

def test_frota_k_fixo_k1_sem_transfer(token_header):
    resp = client.get(
        "/simulacao/frota_k_fixo",
        params={"data_inicial": "2025-09-01", "data_final": "2025-09-18", "k": 1},
        headers=token_header,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["transfer"] == [], "transfer deveria estar vazio quando k=0 (Hub unico)"
    assert "lastmile" in data
