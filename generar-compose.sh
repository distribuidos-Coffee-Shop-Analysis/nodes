#!/bin/bash


if [ "$#" -ne 2 ]; then
  echo "Uso: $0 <archivo_salida> <cantidad_clientes>"
  exit 1
fi

echo "Nombre del archivo de salida: $1"
echo "Cantidad de clientes: $2"

python3 scripts/generate_compose.py "$1" "$2"
