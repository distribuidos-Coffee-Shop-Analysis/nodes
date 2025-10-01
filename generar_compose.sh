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
