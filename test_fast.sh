#!/bin/bash
set -e

# ============================
# Configurações
# ============================
API_GATEWAY="http://localhost:8010"
ML_SERVICE="http://localhost:8011"
EMAIL="paulo.silas@mercadolink.com.br"
SENHA="Link@0101"

# ============================
# 1. Login e captura do token
# ============================
echo "🔑 Autenticando..."
TOKEN=$(curl -s -X POST "$API_GATEWAY/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"$EMAIL\",\"senha\":\"$SENHA\"}" | jq -r .access_token)

if [ -z "$TOKEN" ] || [ "$TOKEN" == "null" ]; then
  echo "❌ Falha ao autenticar. Verifique usuário e senha."
  exit 1
fi
echo "✅ Token capturado"

# ============================
# 2. Rodar treino (modo fast)
# ============================
echo "🚀 Rodando treino rápido..."
curl -s -X POST "$ML_SERVICE/ml/train?fast=true" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_name": "simulacoes",
    "target_column": "custo_total",
    "start_date": "2025-06-01",
    "end_date": "2025-06-07",
    "algorithm": "linear"
  }' | jq .

# ============================
# 3. Rodar planning (modo fast)
# ============================
echo "📊 Rodando planning rápido..."
curl -s "$ML_SERVICE/ml/plan?start_date=2025-08-01&months=1&scenarios=base&fast=true" \
  -H "Authorization: Bearer $TOKEN" | jq .
