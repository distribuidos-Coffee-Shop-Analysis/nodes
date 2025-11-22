package middleware

import (
	"fmt"
	"log"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueManager manages RabbitMQ connections and queue operations for the filter node
type QueueManager struct {
	host       string
	port       int
	username   string
	password   string
	Connection *amqp.Connection
	channel    *amqp.Channel
	consuming  bool

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
		consuming: false,
		Wiring:    w,
	}
}

// Connect establishes connection to RabbitMQ
func (qm *QueueManager) Connect() error {
	var err error

	// Create connection string
	connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		qm.username, qm.password, qm.host, qm.port)

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
		if err := qm.channel.ExchangeDeclare(ex, kind, false, false, false, false, nil); err != nil {
			_ = qm.channel.Close()
			_ = qm.Connection.Close()
			return fmt.Errorf("declare exchange %s: %w", ex, err)
		}
	}

	// Declare and bind queues for each binding
	// - If UseSharedQueue=true: use SharedQueueName (all nodes consume from same queue)
	// - If UseSharedQueue=false and multiple bindings: use IndividualQueueName + suffix (e.g., role.nodeID_0, role.nodeID_1)
	// - If UseSharedQueue=false and single binding: use IndividualQueueName (e.g., role.nodeID)
	for i, b := range qm.Wiring.Bindings {
		var queueName string

		if b.UseSharedQueue {
			// Shared queue: all nodes consume from the same queue
			queueName = qm.Wiring.SharedQueueName
		} else {
			// Individual queue per node
			if len(qm.Wiring.Bindings) > 1 {
				// Multiple bindings without shared queue: add suffix to differentiate
				queueName = fmt.Sprintf("%s_%d", qm.Wiring.IndividualQueueName, i)
			} else {
				// Single binding: use base name
				queueName = qm.Wiring.IndividualQueueName
			}
		}

		// Declare queue
		q, err := qm.channel.QueueDeclare(queueName, false, false, false, false, nil)
		if err != nil {
			_ = qm.channel.Close()
			_ = qm.Connection.Close()
			return fmt.Errorf("declare queue %s: %w", queueName, err)
		}

		// Bind queue to exchange
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

// Disconnect closes RabbitMQ connection
func (qm *QueueManager) Disconnect() {
	if qm.Connection != nil && !qm.Connection.IsClosed() {
		qm.Connection.Close()
	}
	log.Println("action: rabbitmq_disconnect | result: success")
}

// StartConsuming starts consuming from configured input queue(s) and calls callback for each message
// For normal nodes: consumes from single queue
// For joiner nodes: consumes from multiple queues (one per input dataset)
func (qm *QueueManager) StartConsuming(callback func(batch *protocol.BatchMessage, delivery amqp.Delivery)) error {
	qm.consuming = true

	// Start consuming from each queue
	for i, b := range qm.Wiring.Bindings {
		var err error

		dedicatedChannel := qm.channel

		// Determine queue name based on binding configuration
		var queueName string
		if b.UseSharedQueue {
			// Shared queue: all nodes consume from the same queue
			queueName = qm.Wiring.SharedQueueName
		} else {
			// Individual queue per node
			if len(qm.Wiring.Bindings) > 1 {
				// Multiple bindings without shared queue: add suffix to differentiate
				queueName = fmt.Sprintf("%s_%d", qm.Wiring.IndividualQueueName, i)
			} else {
				// Single binding: use base name
				queueName = qm.Wiring.IndividualQueueName
			}
		}

		// Create a dedicated channel for each queue (joiner nodes with multiple bindings)
		if len(qm.Wiring.Bindings) > 1 {
			dedicatedChannel, err = qm.Connection.Channel()
			if err != nil {
				log.Printf("action: create_channel | result: fail | queue: %s | error: %v", queueName, err)
				return err
			}
		}

		// Configure QoS (prefetch) to limit in-flight messages
		// prefetchCount=100: RabbitMQ will send max 100 unacked messages to this consumer
		// This prevents thousands of messages being "in flight" during shutdown
		if err := dedicatedChannel.Qos(100, 0, false); err != nil {
			log.Printf("action: set_qos | result: fail | queue: %s | error: %v", queueName, err)
			return err
		}

		// Start consuming from this queue
		msgs, err := dedicatedChannel.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			log.Printf("action: start_consuming | result: fail | queue: %s | error: %v", queueName, err)
			return err
		}

		log.Printf("action: start_consuming | result: success | queue: %s | shared: %v", queueName, b.UseSharedQueue)

		// Process messages from this queue in a separate goroutine
		go func(qName string, msgChannel <-chan amqp.Delivery) {
			for msg := range msgChannel {
				if !qm.consuming {
					break
				}

				// Parse message outside goroutine
				var batchMessage *protocol.BatchMessage
				var err error

				// Check message type (first byte)
				if len(msg.Body) > 0 {
					msgType := msg.Body[0]
					if msgType == protocol.MessageTypeBatch {
						batchMessage, err = protocol.BatchMessageFromData(msg.Body)
						if err != nil {
							log.Printf("action: parse_batch | queue: %s | result: fail | error: %v", qName, err)
							msg.Ack(false) // Ack to remove bad message
							continue
						}
					} else {
						log.Printf("action: parse_message | queue: %s | result: fail | error: unknown message type: %d", qName, msgType)
						msg.Ack(false) // Ack to remove bad message
						continue
					}
				} else {
					log.Printf("action: parse_message | queue: %s | result: fail | error: empty message body", qName)
					msg.Ack(false) // Ack to remove bad message
					continue
				}

				// Launch callback in goroutine with panic recovery
				go func(batch *protocol.BatchMessage, delivery amqp.Delivery) {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("action: process_batch | result: fail | error: %v", r)
							delivery.Nack(false, true) // Reject and requeue
						}
					}()

					callback(batch, delivery)
				}(batchMessage, msg)
			}
		}(queueName, msgs)
	}

	// Keep the function running (blocking) - it will return when consuming stops
	select {}
}

// StopConsuming stops consuming messages
func (qm *QueueManager) StopConsuming() {
	qm.consuming = false
	log.Println("action: stop_consuming | result: success")
}

// MessageMiddleware interface methods

// Close closes the connection
func (qm *QueueManager) Close() error {
	qm.consuming = false
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
