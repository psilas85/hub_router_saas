#!/bin/bash
set -e

EC2_USER=ubuntu
EC2_HOST=3.87.205.115
PEM_PATH="$HOME/hub-router-server.pem"
REMOTE_DIR="~/hub_router_aws/hub_router_1.0.1"

echo "🚀 Conectando na EC2 (${EC2_HOST}) e atualizando serviços..."
ssh -o StrictHostKeyChecking=no -i "$PEM_PATH" ${EC2_USER}@${EC2_HOST} << EOF
  set -e
  cd ${REMOTE_DIR}

  echo "🛑 Parando containers..."
  docker compose down || true

  echo "📥 Baixando imagens atualizadas..."
  docker compose pull

  echo "▶️ Subindo aplicação..."
  docker compose up -d

  echo "📋 Status dos containers:"
  docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
EOF

echo "✅ Deploy concluído."
