#!/bin/bash
set -e

CONTAINER="hub_router_101-machine_learning_service"
SCRIPT_PATH="/app/src/machine_learning/validate_k_cluster.py"
EXPORT_DIR="/app/exports/machine_learning/validations"
LOG_FILE="./exports/machine_learning/validations/validation.log"

# Cria diretório de validações **dentro do container**
docker exec -it $CONTAINER mkdir -p $EXPORT_DIR

echo "🔎 Rodando validação do modelo K-Cluster dentro do container $CONTAINER..."
echo "📝 Log será salvo em $LOG_FILE"

# Executa dentro do container e salva log no host
docker exec -it $CONTAINER python $SCRIPT_PATH | tee "$LOG_FILE"

echo "✅ Validação concluída. Resultados disponíveis em:"
echo "   - $LOG_FILE"
