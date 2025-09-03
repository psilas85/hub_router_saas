#!/bin/bash

echo "🧹 Limpando cache do apt..."
sudo apt-get clean

echo "🧼 Limpando cache temporário do Docker..."
sudo rm -rf /var/lib/docker/tmp/*

echo "🛑 Parando containers Docker..."
docker stop $(docker ps -aq)

echo "🗑️ Removendo containers parados..."
docker rm $(docker ps -aq)

echo "🗑️ Limpando imagens antigas..."
docker image prune -a -f

echo "🗑️ Limpando volumes..."
docker volume prune -f

echo "✅ Limpeza concluída. Verifique o espaço disponível com 'df -h'."

