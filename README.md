# Coffee Shop Analysis - Filter Node

Este es el nodo filtro. Se encarga de consumir transacciones desde una cola de entrada, filtrar los registros por fechas específicas (años 2024-2025), y rutear los datos filtrados a exchanges de salida según el tipo de dataset.

## Funcionalidad

### Filtrado de Transacciones

El filter node procesa transacciones con las siguientes reglas:

- **Filtro**: Solo deja las transacciones de los años **2024** y **2025**
- **Validación de dataset**: Verifica que el tipo de dataset sea `TRANSACTIONS` o `TRANSACTION_ITEMS`
- **Routing inteligente**: Envía cada tipo de dataset al exchange correspondiente

### Patrón de Comunicación

**Entrada:** Consume desde colas

```
transactions_queue ← Recibe todos los tipos de transacciones
```

**Salida:** Publica a exchanges

```
TRANSACTIONS → transactions_exchange
TRANSACTION_ITEMS → transaction_items_exchange
```

## Protocolo de Comunicación

### Formato de Mensaje JSON

Los mensajes se envían en formato JSON a través de RabbitMQ:

```json
{
  "dataset_type": "TRANSACTIONS",
  "records": ["record1_serialized", "record2_serialized", ...],
  "eof": false
}
```

### Estructura de Records

Cada record se serializa como campos separados por pipes (`|`). Ejemplos:

**TransactionRecord:**

```
transaction_id|user_id|store_id|created_at|final_amount|loyalty_discount|payment_method
```

**TransactionItemRecord:**

```
transaction_id|item_id|qty|discount_pct|item_price
```

## Arquitectura

### Componentes Principales

- **FilterNode**: Coordinador principal que gestiona el ciclo de vida del filtro
- **TransactionFilterHandler**: Worker thread que consume, filtra y rutea transacciones
- **QueueManager**: Interfaz con RabbitMQ que extiende MessageMiddleware de la cátedra

### Patrón de Threading

```
FilterNode (main thread)
    ├── Inicializa conexión RabbitMQ
    ├── Lanza TransactionFilterHandler (worker thread)
    └── Maneja shutdown graceful

TransactionFilterHandler (worker thread)
    ├── Consume mensajes de transactions_queue
    ├── Filtra por años 2024-2025
    ├── Valida tipo de dataset
    └── Publica a exchanges correspondientes
```

### MessageMiddleware Integration

El `QueueManager` implementa la interfaz `MessageMiddleware` requerida por la cátedra:

- **start_consuming(callback)**: Inicia consumo de mensajes
- **stop_consuming()**: Detiene consumo
- **send(message)**: Envía mensaje al exchange por defecto
- **close()**: Cierra conexión
- **delete(exchange_name)**: Elimina exchange

### Dependencias

- **RabbitMQ**: Sistema de colas para comunicación asíncrona
- **Python 3.9+**: Runtime del servidor
- **Pika**: Cliente de RabbitMQ para Python

## Configuración

### Archivo config.ini

```ini
[DEFAULT]
# Logging
LOGGING_LEVEL=INFO

# RabbitMQ Configuration
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=admin
RABBITMQ_PASSWORD=admin

# Filter Node Configuration
INPUT_QUEUE=transactions_queue
TRANSACTIONS_EXCHANGE=transactions_exchange
TRANSACTION_ITEMS_EXCHANGE=transaction_items_exchange
```

### Lógica de Filtrado

- **Filtro de años**: `2024-01-01` ≤ `created_at` ≤ `2025-12-31`
- **Routing por dataset**:
  - `DatasetType.TRANSACTIONS` → `transactions_exchange`
  - `DatasetType.TRANSACTION_ITEMS` → `transaction_items_exchange`

## Desarrollo

### Ejecutar Tests

```bash
python -m unittest tests/test_common.py
```

### Construcción con Docker

```bash
docker build -t filter-node .
```

### Ejecutar con Docker Compose

```bash
docker-compose -f docker-compose-dev.yaml up
```

### Ejecutar Localmente

```bash
python main.py
```

## Flujo de Datos

1. **Nodos upstream publican** transacciones a `transactions_queue`
2. **Filter node consume** mensajes de la cola de entrada
3. **TransactionFilterHandler procesa** cada batch:
   - Valida que el dataset sea `TRANSACTIONS` o `TRANSACTION_ITEMS`
   - Filtra registros por fecha (`created_at` en años 2024-2025)
   - Rutea a exchange correspondiente según tipo de dataset
4. **Exchanges distribuyen** los mensajes filtrados a colas bindeadas
5. **Nodos downstream consumen** de las colas bindeadas para procesamiento adicional

### Ejemplo de Procesamiento

```
Input: transactions_queue
├── Batch: TRANSACTIONS (100 records, años 2020-2025)
│   └── Filtro: Solo 60 records (años 2024-2025)
│       └── Output: transactions_exchange
└── Batch: TRANSACTION_ITEMS (200 records, años 2023-2025)
    └── Filtro: Solo 80 records (años 2024-2025)
        └── Output: transaction_items_exchange
```

## Logs

El sistema genera logs estructurados con el formato:

```
action: <acción> | result: <resultado> | <parámetros adicionales>
```

### Ejemplos de Logs

```bash
# Inicialización
action: filter_node_init | result: success
action: rabbitmq_connect | result: success | host: rabbitmq

# Procesamiento de transacciones
action: transaction_batch_received | result: success | dataset_type: TRANSACTIONS | record_count: 100 | eof: false
action: filter_by_year | result: success | original_count: 100 | filtered_count: 75 | years: 2024-2025
action: batch_routed | result: success | dataset_type: TRANSACTIONS | output_exchanges: ['transactions_exchange'] | eof: false

# Manejo de errores
action: filter_records_by_year | result: fail | error: Invalid date format
action: send_filtered_batch | result: fail | exchange: transactions_exchange | error: Connection closed
```

## Características Técnicas

### MessageMiddleware Integration

El `QueueManager` implementa la interfaz `MessageMiddleware` requerida por la cátedra:

```python
class QueueManager(MessageMiddleware):
    def start_consuming(self, callback):      # ✓ Delegación a start_consuming_transactions
    def stop_consuming(self):                 # ✓ Para consumo de mensajes
    def send(self, message):                  # ✓ Envío a exchange por defecto
    def close(self):                          # ✓ Cierre de conexión
    def delete(self, exchange_name):          # ✓ Eliminación de exchanges
```

### Funcionalidad Específica Mantenida

Además de la interfaz estándar, el QueueManager mantiene métodos específicos para el filtrado:

```python
# Métodos específicos de transacciones
queue_manager.send_filtered_batch(exchange_name, dataset_type, records, eof)
queue_manager.send_to_dataset_output_exchanges(dataset_type, records, eof)
queue_manager.get_output_exchanges_for_dataset(dataset_type)
```

### Manejo de Errores

- **Conexión perdida**: Reintentos automáticos de conexión a RabbitMQ
- **Mensajes malformados**: Logging detallado y rechazo con requeue
- **Fechas inválidas**: Filtrado seguro con manejo de excepciones
- **Shutdown graceful**: Cierre ordenado de threads y conexiones

### Performance

- **Threading asíncrono**: Worker thread dedicado para consumo de mensajes
- **Batch processing**: Procesa múltiples registros por mensaje
- **Acknowledge explícito**: Solo confirma mensajes procesados exitosamente
- **Persistent messages**: Mensajes durables para garantizar entrega

## Estado del Proyecto

### ✅ Completado

- Filtrado por años 2024-2025
- Routing por tipo de dataset a exchanges
- Integración con MessageMiddleware de la cátedra
- Configuración flexible via config.ini
- Logging estructurado
- Manejo de errores robusto
- Threading seguro
- Arquitectura simplificada (solo RabbitMQ, sin TCP)

### 🎯 Ready for Production

El Filter Node está completamente implementado y listo para ser desplegado como parte del pipeline de procesamiento de datos del sistema Coffee Shop Analysis.
