# src/data_input/tests/test_decode_token.py
import jwt
import os

TOKEN = os.getenv("AUTH_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMmYwYmM1Yy1jYjgwLTQ4NzEtYmE4Yi1hODI5MGU1NTg4NWEiLCJyb2xlIjoiY2xpZW50ZV9hZG1pbiIsInBsYW5vIjoic2ltdWxhdGlvbiIsImV4cCI6MTc1ODU0MDI0NiwidGVuYW50IjoiMzhlZDBkY2MtYTkyYi00YzA3LTg1ZTctYjU5ZTU5MzliODRjIn0.AlffHFq_R2AUhgSLHLasHwgsoMACQ6hPfCj7fgdL5Uo")
SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")
ALGORITHM = os.getenv("ALGORITHM", "HS256")

try:
    payload = jwt.decode(TOKEN, SECRET_KEY, algorithms=[ALGORITHM])
    print("Payload decodificado:", payload)
except Exception as e:
    print("❌ Erro ao decodificar token:", e)
