# ========================
# costs_last_mile/.env
# ========================
cat > src/costs_last_mile/.env <<EOL
DB_HOST=postgres_db
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=Psilas@85
DB_DATABASE=routing_db_utf8
DB_NAME=routing_db_utf8
EOL

# ========================
# costs_transfer/.env
# ========================
cat > src/costs_transfer/.env <<EOL
DB_HOST=postgres_db
DB_PORT=5432
DB_USER=postgres
DB_PASS=Psilas@85
DB_DATABASE=routing_db_utf8
EOL

