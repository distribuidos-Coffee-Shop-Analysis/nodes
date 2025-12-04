# Coffee Shop Analysis - Nodes

Este es el repo de Nodes, donde definimos el comportamiento de todos los nodos del sistema. Para ver los nodos definidos actualmente se puede ver el `docker-compose.yaml`, generalmente los nodos que se levantan en una ejecucion normal son:

- `filter-node-year` => 4 nodos 
- `filter-node-hour` => 4 nodos
- `filter-node-amount` => 4 nodos
- `group-by-node-q2` => 4 nodos
- `group-by-node-q3` => 4 nodos
- `group-by-node-q4` => 4 nodos
- `aggregate-node-q2` => 3 nodos
- `aggregate-node-q3` => 3 nodos
- `aggregate-node-q4` => 3 nodos
- `joiner-node-q2` => 1 nodos
- `joiner-node-q3` => 1 nodos
- `joiner-node-q4-users` => 5 nodos
- `joiner-node-q4-stores` => 1 nodos

Si se quiere modificar alguna cantidad de nodos se puede hacer mediante el script `generate_compose.sh`. Un ejemplo de llamado a este, que genera el estado descripto arriba, puede ser asi:
```sh
./generar_compose.sh docker-compose.yml filter-node-year=4 filter-node-hour=4 filter-node-amount=4 group-by-node-q2=4 group-by-node-q3=4 group-by-node-q4=4 aggregate-node-q2=3 aggregate-node-q3=3 aggregate-node-q4=3 joiner-node-q2=1 joiner-node-q3=1 joiner-node-q4-users=5 joiner-node-q4-stores=1
```

## Correr los nodos

Para levantar todos los nodos:
```sh
make docker-compose-up
```

Para limpiar los contenedores:
```sh
make docker-compose-down
```
