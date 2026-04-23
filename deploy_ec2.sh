#!/bin/bash
set -e

EC2_USER=ubuntu
EC2_HOST=3.87.205.115
PEM_PATH="$HOME/hub-router-server.pem"
REMOTE_DIR="~/hub_router_aws/hub_router_1.0.1"
SIMULATION_DATE_WORKERS="${SIMULATION_DATE_WORKERS:-2}"

echo "🚀 Conectando na EC2 (${EC2_HOST}) e atualizando serviços..."
ssh -o StrictHostKeyChecking=no -i "$PEM_PATH" ${EC2_USER}@${EC2_HOST} << EOF
  set -e
  cd ${REMOTE_DIR}

  echo "🛑 Parando containers..."
  docker compose down || true

  echo "📥 Baixando imagens atualizadas..."
  docker compose pull

  echo "▶️ Subindo aplicação com ${SIMULATION_DATE_WORKERS} workers de simulation por data..."
  docker compose up -d --scale simulation_date_worker=${SIMULATION_DATE_WORKERS}

  echo "📋 Status dos containers:"
  docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
EOF

echo "✅ Deploy concluído."
