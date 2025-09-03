#!/bin/bash

# 🗂 Pasta temporária no container
TMP_DIR="/tmp/simulation_diag"

# 📦 Container e banco
CONTAINER="postgres_db"
DB="simulation_db"
USER="postgres"

# 🧹 Limpa diretório antigo no container
docker exec -i $CONTAINER bash -c "rm -rf $TMP_DIR && mkdir -p $TMP_DIR"

# 📜 Lista apenas tabelas úteis (sem sequences/views)
TABLES=$(docker exec -i $CONTAINER psql -U $USER -d $DB -t -c \
"SELECT table_name 
 FROM information_schema.tables 
 WHERE table_schema='public' 
   AND table_type='BASE TABLE'
 ORDER BY table_name")

# 📤 Exporta cada tabela
for table_name in $TABLES; do
  echo "📦 Exportando $table_name..."

  docker exec -i $CONTAINER psql -U $USER -d $DB \
    -c "\copy (SELECT column_name, data_type, is_nullable 
               FROM information_schema.columns 
               WHERE table_name='$table_name' 
               ORDER BY ordinal_position) 
         TO '$TMP_DIR/${table_name}_estrutura.csv' WITH CSV HEADER"

  docker exec -i $CONTAINER psql -U $USER -d $DB \
    -c "\copy (SELECT COUNT(*) AS total FROM \"$table_name\") 
         TO '$TMP_DIR/${table_name}_count.csv' WITH CSV HEADER"

  docker exec -i $CONTAINER psql -U $USER -d $DB \
    -c "\copy (SELECT * FROM \"$table_name\" LIMIT 10) 
         TO '$TMP_DIR/${table_name}_sample.csv' WITH CSV HEADER"
done

# 📥 Copia tudo para o host
mkdir -p simulation_diag
docker cp $CONTAINER:$TMP_DIR/. ./simulation_diag

# 📦 Compacta para envio
zip -r simulation_diag.zip simulation_diag/
echo "✅ Arquivo gerado: simulation_diag.zip"
