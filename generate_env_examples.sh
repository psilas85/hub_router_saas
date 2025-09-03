#!/usr/bin/env bash
set -euo pipefail

# percorre todos os .env existentes dentro de src/
find src -type f -name ".env" | while read -r env_file; do
    dir=$(dirname "$env_file")
    example_file="$dir/.env.example"

    echo "🔄 Gerando $example_file a partir de $env_file"

    # cria o .env.example substituindo valores por placeholders
    awk -F= '
    /^[#]/ {print $0; next}  # mantém comentários
    NF==2 {
        key=$1
        # substitui valor sensível por placeholder
        if (key ~ /KEY/ || key ~ /SECRET/ || key ~ /TOKEN/ || key ~ /PASS/ || key ~ /PWD/) {
            print key"=changeme"
        } else if (key ~ /PORT/) {
            print key"=5432"
        } else if (key ~ /HOST/) {
            print key"=localhost"
        } else if (key ~ /DB/) {
            print key"=changeme_db"
        } else if (key ~ /USER/) {
            print key"=changeme_user"
        } else {
            print key"="
        }
        next
    }
    NF==1 {print $1}   # mantém linhas sem "="
    ' "$env_file" > "$example_file"
done

echo "✅ Todos os .env.example foram gerados com placeholders."
