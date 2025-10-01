#!/bin/bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Uso: $0 <archivo_salida> <tipo=cantidad> [<tipo=cantidad> ...]"
  echo "Ej:  $0 docker-compose.yml filter-node-ano=2 group-by-node-q3=3"
  exit 1
fi

OUTPUT_FILE="$1"
shift

python3 scripts/generate_compose.py "$OUTPUT_FILE" "$@"

# ./generar_compose.sh docker-compose.yml filter-node-year=2 filter-node-hour=2 filter-node-amount=2 group-by-node-q2=2 group-by-node-q3=2 group-by-node-q4=2 aggregate-node-q2=1
