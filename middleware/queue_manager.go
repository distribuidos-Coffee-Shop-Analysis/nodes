package middleware

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

const HEARTBEAT_SECONDS = 3

// CleanableHandler defines the interface for handlers that support cleanup operations
type CleanableHandler interface {
	OnCleanup(clientID string)
}

// CleanupMessage represents the JSON structure of cleanup messages
type CleanupMessage struct {
	ClientID string `json:"client_id"`
}

// QueueManager manages RabbitMQ connections and queue operations
type QueueManager struct {
	host       string
	port       int
	username   string
	password   string
	Connection *amqp.Connection
	channel    *amqp.Channel

	// Pipeline components
	pipelines  []*Pipeline
	pipelineMu sync.Mutex

	Wiring *common.NodeWiring
}

func NewQueueManagerWithWiring(w *common.NodeWiring) *QueueManager {
	cfg := common.GetConfig()
	r := cfg.GetRabbitmqConfig()
	return &QueueManager{
		host:      r.Host,
		port:      r.Port,
		username:  r.Username,
		password:  r.Password,
		pipelines: make([]*Pipeline, 0),
		Wiring:    w,
	}
}

func (qm *QueueManager) Connect() error {
	var err error

	connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/?heartbeat=%d",
		qm.username, qm.password, qm.host, qm.port, HEARTBEAT_SECONDS)

	qm.Connection, err = amqp.Dial(connStr)
	if err != nil {
		log.Printf("action: rabbitmq_connect | result: fail | error: %v", err)
		return err
	}

	qm.channel, err = qm.Connection.Channel()
	if err != nil {
		log.Printf("action: rabbitmq_channel | result: fail | error: %v", err)
		return err
	}

	// exchanges
	for _, ex := range qm.Wiring.DeclareExchs {
		kind := "direct"
		if err := qm.channel.ExchangeDeclare(ex, kind, true, false, false, false, nil); err != nil {
			_ = qm.channel.Close()
			_ = qm.Connection.Close()
			return fmt.Errorf("declare exchange %s: %w", ex, err)
		}
	}

	// Declare and bind queues for each binding
	for i, b := range qm.Wiring.Bindings {
		var queueName string

		if b.UseSharedQueue {
			queueName = qm.Wiring.SharedQueueName
		} else {
			if len(qm.Wiring.Bindings) > 1 {
				queueName = fmt.Sprintf("%s_%d", qm.Wiring.IndividualQueueName, i)
			} else {
				queueName = qm.Wiring.IndividualQueueName
			}
		}

		q, err := qm.channel.QueueDeclare(queueName, true, false, false, false, nil)
		if err != nil {
			_ = qm.channel.Close()
			_ = qm.Connection.Close()
			return fmt.Errorf("declare queue %s: %w", queueName, err)
		}

		if err := qm.channel.QueueBind(q.Name, b.RoutingKey, b.Exchange, false, nil); err != nil {
			_ = qm.channel.Close()
			_ = qm.Connection.Close()
			return fmt.Errorf("bind queue %s to exchange %s: %w", q.Name, b.Exchange, err)
		}

		log.Printf("action: queue_setup | queue: %s | exchange: %s | routing_key: %s | binding_index: %d | shared: %v",
			queueName, b.Exchange, b.RoutingKey, i, b.UseSharedQueue)
	}

	return nil
}

func (qm *QueueManager) Disconnect() {
	if qm.Connection != nil && !qm.Connection.IsClosed() {
		qm.Connection.Close()
	}
	log.Println("action: rabbitmq_disconnect | result: success")
}

func (qm *QueueManager) getQueueName(bindingIndex int) string {
	b := qm.Wiring.Bindings[bindingIndex]

	if b.UseSharedQueue {
		return qm.Wiring.SharedQueueName
	}

	if len(qm.Wiring.Bindings) > 1 {
		return fmt.Sprintf("%s_%d", qm.Wiring.IndividualQueueName, bindingIndex)
	}

	return qm.Wiring.IndividualQueueName
}

// StartConsuming starts consuming from configured input queues.
func (qm *QueueManager) StartConsuming(callback func(batch *protocol.BatchMessage, delivery amqp.Delivery)) error {
	cfg := common.GetConfig()
	pipelineConfig := cfg.GetPipelineConfig(qm.Wiring.Role)

	log.Printf("action: start_consuming_pipeline | role: %s | consumers: %d | prefetch: %d | processors: %d | num_queues: %d",
		qm.Wiring.Role,
		pipelineConfig.NumConsumers,
		pipelineConfig.ConsumerPrefetch,
		pipelineConfig.NumProcessors,
		len(qm.Wiring.Bindings))

	var wg sync.WaitGroup

	// Start a pipeline for each queue
	for i := range qm.Wiring.Bindings {
		queueName := qm.getQueueName(i)

		config := PipelineConfig{
			NumConsumers:     pipelineConfig.NumConsumers,
			ConsumerPrefetch: pipelineConfig.ConsumerPrefetch,
			NumProcessors:    pipelineConfig.NumProcessors,
			InputBufferSize:  pipelineConfig.InputBufferSize,
		}

		pipeline := NewPipeline(config, qm.Connection)

		qm.pipelineMu.Lock()
		qm.pipelines = append(qm.pipelines, pipeline)
		qm.pipelineMu.Unlock()

		if err := pipeline.Start(queueName, callback); err != nil {
			qm.StopConsuming()
			return fmt.Errorf("failed to start pipeline for queue %s: %w", queueName, err)
		}

		wg.Add(1)
		go func(p *Pipeline) {
			defer wg.Done()
			p.Wait()
		}(pipeline)
	}

	wg.Wait()

	return nil
}

// StopConsuming stops all pipelines.
func (qm *QueueManager) StopConsuming() {
	log.Println("action: stop_consuming | status: stopping_pipelines")

	qm.pipelineMu.Lock()
	pipelines := qm.pipelines
	qm.pipelineMu.Unlock()

	for _, p := range pipelines {
		p.Stop()
	}

	log.Println("action: stop_consuming | result: success")
}

// Close closes the connection and stops all pipelines.
func (qm *QueueManager) Close() error {
	qm.StopConsuming()

	var err error
	if qm.Connection != nil && !qm.Connection.IsClosed() {
		err = qm.Connection.Close()
	}
	if err != nil {
		log.Printf("action: close | result: fail | error: %v", err)
	} else {
		log.Println("action: close | result: success")
	}
	return err
}

// StartCleanupListener starts a goroutine that listens for cleanup signals
// and calls the handler's OnCleanup method for each client that needs cleanup
func (qm *QueueManager) StartCleanupListener(handler CleanableHandler) error {
	// Declare cleanup exchange (fanout, durable)
	err := qm.channel.ExchangeDeclare(
		"cleanup_exchange", // name
		"fanout",           // type
		true,               // durable
		false,              // auto-deleted
		false,              // internal
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		log.Printf("action: cleanup_exchange_declare | result: fail | error: %v", err)
		return fmt.Errorf("failed to declare cleanup exchange: %w", err)
	}

	// Declare an exclusive, auto-delete queue for this node
	queue, err := qm.channel.QueueDeclare(
		"",    // name (empty = server generates unique name)
		false, // durable (false for exclusive queues)
		true,  // auto-delete (deleted when consumer disconnects)
		true,  // exclusive (only this connection can access)
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Printf("action: cleanup_queue_declare | result: fail | error: %v", err)
		return fmt.Errorf("failed to declare cleanup queue: %w", err)
	}

	// Bind queue to cleanup exchange
	err = qm.channel.QueueBind(
		queue.Name,         // queue name
		"",                 // routing key (ignored for fanout)
		"cleanup_exchange", // exchange
		false,              // no-wait
		nil,                // arguments
	)
	if err != nil {
		log.Printf("action: cleanup_queue_bind | result: fail | error: %v", err)
		return fmt.Errorf("failed to bind cleanup queue: %w", err)
	}

	log.Printf("action: cleanup_listener_setup | result: success | queue: %s", queue.Name)

	// Start consuming from cleanup queue
	msgs, err := qm.channel.Consume(
		queue.Name, // queue
		"",         // consumer tag (empty = server generates)
		false,      // auto-ack
		true,       // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		log.Printf("action: cleanup_consume | result: fail | error: %v", err)
		return fmt.Errorf("failed to start consuming cleanup messages: %w", err)
	}

	// Start goroutine to process cleanup messages
	go func() {
		log.Println("action: cleanup_listener_start | result: success | msg: listening for cleanup signals")

		for msg := range msgs {
			var cleanupMsg CleanupMessage
			if err := json.Unmarshal(msg.Body, &cleanupMsg); err != nil {
				log.Printf("action: cleanup_message_decode | result: fail | error: %v", err)
				msg.Ack(false)
				continue
			}

			log.Printf("action: cleanup_received | result: processing | client_id: %s", cleanupMsg.ClientID)

			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("action: cleanup_panic | client_id: %s | error: %v", cleanupMsg.ClientID, r)
						msg.Nack(false, true)
					}
				}()

				handler.OnCleanup(cleanupMsg.ClientID)

				if err := msg.Ack(false); err != nil {
					log.Printf("action: cleanup_ack | client_id: %s | result: fail | error: %v", cleanupMsg.ClientID, err)
				} else {
					log.Printf("action: cleanup_processed | result: success | client_id: %s", cleanupMsg.ClientID)
				}
			}()
		}

		log.Println("action: cleanup_listener_end | result: success | msg: cleanup listener stopped")
	}()

	return nil
}
