#!/bin/bash
set -e

echo "🔹 Iniciando build e push de todas as imagens HubRouter..."
docker login

# Lista de serviços
SERVICES=(
    "hub_router-base:Dockerfile.base:."
    "hub_router-authentication:src/authentication/Dockerfile.authentication:."
    "hub_router-clusterization:src/clusterization/Dockerfile.clusterization:."
    "hub_router-last_mile_routing:src/last_mile_routing/Dockerfile.last_mile_routing:."
    "hub_router-transfer_routing:src/transfer_routing/Dockerfile.transfer_routing:."
    "hub_router-costs_last_mile:src/costs_last_mile/Dockerfile.costs_last_mile:."
    "hub_router-costs_transfer:src/costs_transfer/Dockerfile.costs_transfer:."
    "hub_router-simulation:src/simulation/Dockerfile.simulation:."
    "hub_router-data_input:src/data_input/Dockerfile.data_input:."
    "hub_router-exploratory_api:src/exploratory_analysis/Dockerfile.exploratory_api:."
    "hub_router-exploratory_ui:src/exploratory_analysis/Dockerfile.exploratory_ui:."
)

for SERVICE in "${SERVICES[@]}"; do
    NAME="${SERVICE%%:*}"
    REST="${SERVICE#*:}"
    DOCKERFILE="${REST%%:*}"
    CONTEXT="${REST#*:}"

    echo "🚀 Buildando imagem: psilas85/${NAME}:latest"
    docker build -t "psilas85/${NAME}:latest" -f "${DOCKERFILE}" "${CONTEXT}"

    echo "📤 Fazendo push para Docker Hub..."
    docker push "psilas85/${NAME}:latest"

    echo "✅ Imagem ${NAME} enviada com sucesso!"
done

echo "🎯 Todas as imagens foram buildadas e enviadas com sucesso!"
