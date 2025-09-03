#!/bin/bash

# ========================
# Variáveis comuns AWS
# ========================
DB_HOST_AWS="postgres_db"
DB_PORT="5432"
DB_USER="postgres"
DB_PASSWORD="Psilas@85"
GMAPS_KEY="AIzaSyCXfVTF_C-dArM3wTJUOxhjNFKd5uEnRVs"

DATA_INPUT_PATH="/app/data_input"
OUTPUT_PATH="/app/output"
CACHE_PATH="/app/cache_routes"

# ========================
# authentication/.env
# ========================
cat > src/authentication/.env <<EOL
CLUSTER_DB=clusterization_db
ROUTING_DB=routing_db_utf8

DB_USER=$DB_USER
DB_PASS=$DB_PASSWORD
DB_HOST=$DB_HOST_AWS
DB_PORT=$DB_PORT

DB_DATABASE=clusterization_db

GMAPS_API_KEY=$GMAPS_KEY
EOL

# ========================
# clusterization/.env
# ========================
cat > src/clusterization/.env <<EOL
DB_HOST=$DB_HOST_AWS
DB_PORT=$DB_PORT
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD

DB_DATABASE=clusterization_db
DB_DATABASE_ROUTING=routing_db_utf8

GOOGLE_MAPS_API_KEY=$GMAPS_KEY
EOL

# ========================
# routing/.env
# ========================
cat > src/last_mile_routing/.env <<EOL
DB_HOST=$DB_HOST_AWS
DB_PORT=$DB_PORT
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
DB_DATABASE=routing_db_utf8
DB_NAME=routing_db_utf8
EOL

# ========================
# exploratory_analysis/.env
# ========================
cat > src/exploratory_analysis/.env <<EOL
DB_HOST=$DB_HOST_AWS
DB_PORT=$DB_PORT
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD
CLUSTERIZATION_DB=clusterization_db
EOL

# ========================
# simulation/.env
# ========================
cat > src/simulation/.env <<EOL
DB_HOST=$DB_HOST_AWS
DB_PORT=$DB_PORT
DB_USER=$DB_USER
DB_PASSWORD=$DB_PASSWORD

DB_DATABASE_SIMULATION=simulation_db
SIMULATION_DB=simulation_db

DB_DATABASE_CLUSTERIZATION=clusterization_db
CLUSTERIZATION_DB=clusterization_db

DB_DATABASE_ROUTING=routing_db_utf8
ROUTING_DB=routing_db_utf8

GOOGLE_API_KEY=$GMAPS_KEY

DATA_INPUT=$DATA_INPUT_PATH
OUTPUT=$OUTPUT_PATH
CACHE_ROUTES=$CACHE_PATH

DEFAULT_TENANT_ID=dev_tenant

OSRM_HOST=osrm_service
OSRM_PORT=5000
EOL

# ========================
# transfer_routing/.env
# ========================
cat > src/transfer_routing/.env <<EOL
CLUSTER_DB=clusterization_db
ROUTING_DB=routing_db_utf8

DB_USER=$DB_USER
DB_PASS=$DB_PASSWORD
DB_HOST=$DB_HOST_AWS
DB_PORT=$DB_PORT

GMAPS_API_KEY=$GMAPS_KEY

OSRM_URL=http://osrm_service:5000
EOL

echo "✅ Arquivos .env para AWS criados/atualizados com sucesso na estrutura src/"

