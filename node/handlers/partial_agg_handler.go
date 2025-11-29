package handlers

import (
	"log"
	"sync"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/aggregates"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

type PartialAggregateHandler struct {
	newAggregate func() aggregates.Aggregate
}

// Buffer por Cliente: map[ClientID] -> Aggregate (puede ser Q2 o Q3 o Q4)
type ClientBuffer struct {
	agg         aggregates.Aggregate
	pendingAcks []amqp.Delivery
}

func NewPartialAggregateHandler(factory func() aggregates.Aggregate) *PartialAggregateHandler {
	return &PartialAggregateHandler{
		newAggregate: factory,
	}
}

func (h *PartialAggregateHandler) Name() string {
	return "partial_" + h.newAggregate().Name()
}

// StartHandler lanza N workers transaccionales independientes
func (h *PartialAggregateHandler) StartHandler(qm *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	//cfg := common.GetConfig()
	// Usamos la misma config de cantidad de workers que los filtros/groups por ahora
	numWorkers := 5

	// Input Queue (Shared)
	inputQueue := qm.Wiring.SharedQueueName

	log.Printf("action: partial_start | workers: %d | queue: %s", numWorkers, inputQueue)

	for i := 0; i < numWorkers; i++ {
		clientWg.Add(1)
		go func(workerID int) {
			defer clientWg.Done()
			h.Handle(workerID, qm.Connection, inputQueue, qm.Wiring)
		}(i)
	}

	return nil
}

func (h *PartialAggregateHandler) Shutdown() error {
	// No hay estado persistente que guardar en shutdown para partial aggs
	// RabbitMQ se encarga de re-encolar lo no commiteado.
	return nil
}

// Lógica del Worker Transaccional
func (h *PartialAggregateHandler) Handle(id int, conn *amqp.Connection, inputQueue string, wiring *common.NodeWiring) {
	// 1. Canal Propio
	ch, err := conn.Channel()
	if err != nil {
		log.Printf("worker %d error create channel: %v", id, err)
		return
	}
	defer ch.Close()

	// 2. Modo Transaccional
	if err := ch.Tx(); err != nil {
		log.Printf("worker %d error tx: %v", id, err)
		return
	}

	// 3. QoS alto para batching eficiente en memoria
	ch.Qos(500, 0, false)

	msgs, err := ch.Consume(inputQueue, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("worker %d error consume: %v", id, err)
		return
	}

	buffers := make(map[string]*ClientBuffer)

	// Timer de 10 segundos
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return // Canal cerrado
			}

			// Parsear mensaje
			batch, err := protocol.BatchMessageFromData(msg.Body)
			if err != nil {
				msg.Nack(false, false) // Descartar mensaje corrupto
				continue
			}

			// Inicializar buffer si no existe
			if _, exists := buffers[batch.ClientID]; !exists {
				buffers[batch.ClientID] = &ClientBuffer{
					agg:         h.newAggregate(),
					pendingAcks: make([]amqp.Delivery, 0),
				}
			}

			// acumulamos los registros en memoria dentro d c/
			err = buffers[batch.ClientID].agg.Accumulate(batch.Records)
			if err != nil {
				log.Printf("aggregation error: %v", err)
				// hacer nack , requeue = false ?
			}

			// B. me lo guardo para el ACK posterior
			buffers[batch.ClientID].pendingAcks = append(buffers[batch.ClientID].pendingAcks, msg)

		case <-ticker.C:
			// C. FLUSH TIME (Ventana de 10s)
			if len(buffers) == 0 {
				continue
			}

			// Iteramos por cliente y commiteamos
			for clientID, buf := range buffers {
				if len(buf.pendingAcks) == 0 {
					continue
				}

				// 1. Obtener resultado parcial
				partialRecords, _ := buf.agg.GetPartialResult()

				// 2. Crear Batch de Salida
				// Nota: BatchIndex preservado del último mensaje o generado nuevo?
				// Para Partial Aggs, el índice exacto importa menos que el orden,
				// pero idealmente pasaríamos un índice coherente.
				lastMsgIndex := 0 // Simplificación

				outBatch := protocol.NewGroupByBatch(lastMsgIndex, partialRecords, false)
				outBatch.ClientID = clientID
				// Importante: El tipo de dataset debe coincidir con lo que espera el siguiente nodo
				// Ej: Q3 Partial devuelve Q3Groups, no Q3Agg.

				outBytes := common.EncodeToByteArray(outBatch)

				// 3. Publicar (Dentro de la TX)
				// Buscamos a dónde mandar según el tipo de dataset (usando wiring)
				route, ok := wiring.Outputs[outBatch.DatasetType]
				if ok {
					err := ch.Publish(route.Exchange, route.RoutingKey, false, false, amqp.Publishing{
						Body:         outBytes,
						DeliveryMode: amqp.Persistent,
					})
					if err != nil {
						ch.TxRollback()
						// Resetear todo o salir para reinicio limpio
						continue
					}
				}

				// 4. ACKs de los inputs (Dentro de la TX)
				for _, delivery := range buf.pendingAcks {
					delivery.Ack(false)
				}
			}

			// 5. COMMIT FINAL (Atómico)
			if err := ch.TxCommit(); err != nil {
				log.Printf("TxCommit fail: %v", err)
				ch.TxRollback()
			}

			// 6. Limpiar buffers (Ya se flushearon)
			buffers = make(map[string]*ClientBuffer)
		}
	}
}
