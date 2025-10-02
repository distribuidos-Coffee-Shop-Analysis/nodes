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

	for i, b := range qm.Wiring.Bindings {
		// Determine queue name: single binding uses base name, multiple bindings add suffix
		queueName := qm.Wiring.QueueName
		if len(qm.Wiring.Bindings) > 1 {
			queueName = fmt.Sprintf("%s_%d", qm.Wiring.QueueName, i)
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

		log.Printf("action: queue_setup | queue: %s | exchange: %s | routing_key: %s | binding_index: %d",
			queueName, b.Exchange, b.RoutingKey, i)
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
	for i := range qm.Wiring.Bindings {
		var err error
	
		dedicatedChannel := qm.channel
		// Determine queue name: single binding uses base name, multiple bindings add suffix
		queueName := qm.Wiring.QueueName
		if len(qm.Wiring.Bindings) > 1 {
			queueName = fmt.Sprintf("%s_%d", qm.Wiring.QueueName, i)
			// Create a dedicated channel for this queue (joiner nodes)
			dedicatedChannel, err = qm.Connection.Channel()
			if err != nil {
				log.Printf("action: create_channel | result: fail | queue: %s | error: %v", queueName, err)
				return err
			}
		}

		// Start consuming from this queue
		msgs, err := dedicatedChannel.Consume(queueName, "", false, false, false, false, nil)
		if err != nil {
			log.Printf("action: start_consuming | result: fail | queue: %s | error: %v", queueName, err)
			return err
		}

		log.Printf("action: start_consuming | result: success | queue: %s", queueName)

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
							msg.Nack(false, true) // Reject and requeue
							continue
						}
					} else {
						log.Printf("action: parse_message | queue: %s | result: fail | error: unknown message type: %d", qName, msgType)
						msg.Nack(false, true)
						continue
					}
				} else {
					log.Printf("action: parse_message | queue: %s | result: fail | error: empty message body", qName)
					msg.Nack(false, true)
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

// Send sends message to default output exchange
func (qm *QueueManager) Send(message []byte) error {
	// Use the first available output exchange as default
	var exchange, routingKey string
	for _, route := range qm.Wiring.Outputs {
		exchange = route.Exchange
		routingKey = route.RoutingKey
		break
	}

	if exchange == "" {
		return fmt.Errorf("no output exchange configured")
	}

	err := qm.channel.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Make message persistent
			Body:         message,
		})

	if err != nil {
		log.Printf("action: send | result: fail | error: %v", err)
		return err
	}
	return nil
}

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

// DeleteExchanges deletes all declared exchanges
func (qm *QueueManager) DeleteExchanges() error {
	if qm.channel == nil {
		return fmt.Errorf("no channel available")
	}

	// Delete all declared exchanges
	for _, exchangeName := range qm.Wiring.DeclareExchs {
		err := qm.channel.ExchangeDelete(exchangeName, false, false)
		if err != nil {
			log.Printf("action: delete | result: fail | exchange: %s | error: %v", exchangeName, err)
			return err
		}
		log.Printf("action: delete | result: success | exchange: %s", exchangeName)
	}

	return nil
}

// RabbitMQMiddleware implements the MessageMiddleware interface using QueueManager
type RabbitMQMiddleware struct{}

// NewRabbitMQMiddleware creates a new instance of RabbitMQMiddleware
func NewRabbitMQMiddleware() *RabbitMQMiddleware {
	return &RabbitMQMiddleware{}
}

// StartConsuming implements MessageMiddleware interface
func (rmq *RabbitMQMiddleware) StartConsuming(m *QueueManager, onMessageCallback onMessageCallback) MessageMiddlewareError {
	if m.Connection == nil || m.Connection.IsClosed() {
		log.Printf("action: start_consuming | result: fail | error: no connection available")
		return MessageMiddlewareDisconnectedError
	}

	msgs, err := m.channel.Consume(m.Wiring.QueueName, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("action: start_consuming | result: fail | error: %v", err)
		return MessageMiddlewareMessageError
	}

	m.consuming = true
	log.Printf("action: start_consuming | result: success | queue: %s", m.Wiring.QueueName)

	// Create a channel for the messages and start the callback in a goroutine
	consumeChannel := ConsumeChannel(&msgs)
	done := make(chan error, 1)

	go onMessageCallback(consumeChannel, done)

	// Wait for completion or error
	err = <-done
	if err != nil {
		log.Printf("action: start_consuming | result: fail | error: %v", err)
		return MessageMiddlewareMessageError
	}

	return 0 // No error
}

// StopConsuming implements MessageMiddleware interface
func (rmq *RabbitMQMiddleware) StopConsuming(m *QueueManager) MessageMiddlewareError {
	m.consuming = false
	log.Println("action: stop_consuming | result: success")
	return 0 // No error
}

// Send implements MessageMiddleware interface
func (rmq *RabbitMQMiddleware) Send(m *QueueManager, message []byte) MessageMiddlewareError {
	if m.Connection == nil || m.Connection.IsClosed() {
		log.Printf("action: send | result: fail | error: no connection available")
		return MessageMiddlewareDisconnectedError
	}

	// Use the first available output exchange as default
	var exchange, routingKey string
	for _, route := range m.Wiring.Outputs {
		exchange = route.Exchange
		routingKey = route.RoutingKey
		break
	}

	if exchange == "" {
		log.Printf("action: send | result: fail | error: no output exchange configured")
		return MessageMiddlewareMessageError
	}

	err := m.channel.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Make message persistent
			Body:         message,
		})

	if err != nil {
		log.Printf("action: send | result: fail | error: %v", err)
		return MessageMiddlewareMessageError
	}
	return 0 // No error
}

// Close implements MessageMiddleware interface
func (rmq *RabbitMQMiddleware) Close(m *QueueManager) MessageMiddlewareError {
	m.consuming = false
	var err error
	if m.Connection != nil && !m.Connection.IsClosed() {
		err = m.Connection.Close()
	}
	if err != nil {
		log.Printf("action: close | result: fail | error: %v", err)
		return MessageMiddlewareCloseError
	} else {
		log.Println("action: close | result: success")
	}
	return 0 // No error
}

// Delete implements MessageMiddleware interface
func (rmq *RabbitMQMiddleware) Delete(m *QueueManager) MessageMiddlewareError {
	if m.channel == nil {
		log.Printf("action: delete | result: fail | error: no channel available")
		return MessageMiddlewareDeleteError
	}

	// Delete all declared exchanges
	for _, exchangeName := range m.Wiring.DeclareExchs {
		err := m.channel.ExchangeDelete(exchangeName, false, false)
		if err != nil {
			log.Printf("action: delete | result: fail | exchange: %s | error: %v", exchangeName, err)
			return MessageMiddlewareDeleteError
		}
		log.Printf("action: delete | result: success | exchange: %s", exchangeName)
	}

	return 0 // No error
}

// Ensure RabbitMQMiddleware implements MessageMiddleware interface
var _ MessageMiddleware[QueueManager] = (*RabbitMQMiddleware)(nil)
