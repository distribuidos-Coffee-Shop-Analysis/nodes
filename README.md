# Coffee Shop Analysis - Connection Node

Este es el nodo de conexión para el sistema de análisis de cafeterías. Actúa handler que recibe datasets de clientes y los rutea a las colas apropiadas para procesamiento, además de reenviar las respuestas de las queries de vuelta al cliente.

## Funcionalidad

### Recepción de Datasets

El connection node recibe 5 tipos de datasets del cliente:

- **Menu Items** (DatasetType: 1): Información de productos del menú
- **Stores** (DatasetType: 2): Información de tiendas
- **Transaction Items** (DatasetType: 3): Items individuales de transacciones
- **Transactions** (DatasetType: 4): Transacciones completas
- **Users** (DatasetType: 5): Información de usuarios

### Ruteo a Queues

Los datasets se rutean a diferentes colas según su tipo:

- **Stores** → `joiner_n_queue_stores`
- **Transaction Items** y **Transactions** → `transactions_queue`
- **Users** → `joiner_n_queue_users`
- **Menu Items** → `joiner_n_queue_menu_items`

### Respuestas de Queries

El nodo lee de la cola `replies_queue` las respuestas procesadas (Q1-Q4) y las reenvía al cliente:

- **Q1** (DatasetType: 10): transaction_id, final_amount
- **Q2** (DatasetType: 11): year_month_created_at, item_name, sellings_qty
- **Q3** (DatasetType: 12): year_half_created_at, store_name, tpv
- **Q4** (DatasetType: 13): store_name, birthdate

## Protocolo de Comunicación

### Formato de Mensaje

```
[MessageType][DatasetType][EOF][RecordCount][Records...]
```

### Tipos de Mensaje

- `MESSAGE_TYPE_BATCH = 2`: Mensajes de batch con datasets
- `MESSAGE_TYPE_RESPONSE = 3`: Respuestas del servidor

### Estructura de Records

Cada record se serializa como campos separados por pipes (`|`). Ejemplos:

**MenuItemRecord:**

```
item_id|item_name|category|price|is_seasonal|available_from|available_to
```

**StoreRecord:**

```
store_id|store_name|street|postal_code|city|state|latitude|longitude
```

## Arquitectura

### Componentes Principales

- **Server**: Coordina conexiones de clientes y manejo de handlers
- **Listener**: Acepta nuevas conexiones de clientes
- **ClientHandler**: Maneja comunicación individual con cada cliente
- **QueryRepliesHandler**: Consume respuestas de queries desde RabbitMQ
- **QueueManager**: Interfaz con RabbitMQ para manejo de colas

### Dependencias

- **RabbitMQ**: Sistema de colas para comunicación asíncrona
- **Python 3.9+**: Runtime del servidor
- **Pika**: Cliente de RabbitMQ para Python

## Configuración

### Variables de Entorno

```ini
SERVER_PORT=12345
SERVER_LISTEN_BACKLOG=5
LOGGING_LEVEL=INFO
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=admin
RABBITMQ_PASSWORD=admin
```

## Desarrollo

### Ejecutar Tests

```bash
python -m unittest tests/test_common.py
```

### Construcción con Docker

```bash
docker build -t connection-node .
```

### Ejecutar con Docker Compose

```bash
docker-compose -f docker-compose-dev.yaml up
```

## Flujo de Datos

1. **Cliente se conecta** al connection node
2. **Cliente envía datasets** por batches con diferentes DatasetTypes
3. **Connection node rutea** cada batch a la cola apropiada en RabbitMQ
4. **Procesadores downstream** consumen de las colas y procesan los datos
5. **Resultados de queries** se publican en `replies_queue`
6. **Connection node consume** de replies_queue y reenvía al cliente
7. **Cliente recibe respuestas** de las queries procesadas

## Logs

El sistema genera logs estructurados con el formato:

```
action: <acción> | result: <resultado> | <parámetros adicionales>
```

Ejemplos:

```
action: dataset_received | result: success | dataset_type: 1 | record_count: 100 | eof: false
action: reply_sent | result: success | dataset_type: 10 | record_count: 50 | eof: true
```
