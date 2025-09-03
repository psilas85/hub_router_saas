#!/bin/bash

set -e

echo "🔧 Criando estrutura de .env para cada módulo..."

# Lista de módulos e seus respectivos conteúdos .env
declare -A envs

envs["data_input"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=clusterization_db"

envs["clusterization"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=clusterization_db"

envs["last_mile_routing"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=routing_db_utf8"

envs["costs_last_mile"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=simulation_db"

envs["costs_transfer"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=simulation_db"

envs["exploratory_analysis"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=clusterization_db"

envs["simulation"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=simulation_db"

envs["transfer_routing"]="DB_HOST=postgres
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=routing_db_utf8"

envs["authentication"]="# Este módulo não utiliza banco de dados por enquanto"

for module in "${!envs[@]}"; do
  echo "🔧 Verificando diretório: $module"
  mkdir -p "$module"

  echo "🔧 Criando .env em $module"
  echo "${envs[$module]}" > "$module/.env"
done

echo "✅ Todos os arquivos .env foram criados com sucesso."

