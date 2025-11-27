package middleware

import (
	"log"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Worker represents an individual worker that consumes messages from a queue
// Each worker has its own RabbitMQ channel and processes messages sequentially
type Worker struct {
	id         int
	queueName  string
	connection *amqp.Connection
	channel    *amqp.Channel
	callback   func(batch *protocol.BatchMessage, delivery amqp.Delivery)
	shutdownCh chan struct{}
	doneCh     chan struct{}
}

// NewWorker creates a new worker with its own dedicated channel
func NewWorker(
	id int,
	queueName string,
	connection *amqp.Connection,
	callback func(batch *protocol.BatchMessage, delivery amqp.Delivery),
	shutdownCh chan struct{},
) (*Worker, error) {
	// Create dedicated channel for this worker
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	// QoS with prefetch=1 for fair dispatch
	if err := channel.Qos(1, 0, false); err != nil {
		channel.Close()
		return nil, err
	}

	return &Worker{
		id:         id,
		queueName:  queueName,
		connection: connection,
		channel:    channel,
		callback:   callback,
		shutdownCh: shutdownCh,
		doneCh:     make(chan struct{}),
	}, nil
}

// Start begins consuming messages from the queue
func (w *Worker) Start() {
	defer close(w.doneCh)
	defer w.cleanup()

	msgs, err := w.channel.Consume(
		w.queueName, // queue
		"",          // consumer tag (auto-generated)
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Printf("action: worker_start | worker_id: %d | queue: %s | result: fail | error: %v",
			w.id, w.queueName, err)
		return
	}

	// Process messages until shutdown
	for {
		select {
		case <-w.shutdownCh:
			log.Printf("action: worker_shutdown | worker_id: %d | queue: %s | result: received_shutdown_signal",
				w.id, w.queueName)
			return

		case msg, ok := <-msgs:
			if !ok {
				log.Printf("action: worker_channel_closed | worker_id: %d | queue: %s",
					w.id, w.queueName)
				return
			}

			w.processMessage(msg)
		}
	}
}

// processMessage handles a single message
func (w *Worker) processMessage(msg amqp.Delivery) {
	if len(msg.Body) == 0 {
		log.Printf("action: worker_parse | worker_id: %d | queue: %s | result: fail | error: empty message body",
			w.id, w.queueName)
		msg.Ack(false)
		return
	}

	msgType := msg.Body[0]
	if msgType != protocol.MessageTypeBatch {
		log.Printf("action: worker_parse | worker_id: %d | queue: %s | result: fail | error: unknown message type: %d",
			w.id, w.queueName, msgType)
		msg.Ack(false)
		return
	}

	batchMessage, err := protocol.BatchMessageFromData(msg.Body)
	if err != nil {
		log.Printf("action: worker_parse | worker_id: %d | queue: %s | result: fail | error: %v",
			w.id, w.queueName, err)
		msg.Ack(false)
		return
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("action: worker_process | worker_id: %d | queue: %s | result: panic | error: %v",
					w.id, w.queueName, r)
				msg.Nack(false, true)
			}
		}()

		w.callback(batchMessage, msg)
	}()
}

// Done returns a channel that is closed when the worker has finished
func (w *Worker) Done() <-chan struct{} {
	return w.doneCh
}

// cleanup closes the worker's channel
func (w *Worker) cleanup() {
	if w.channel != nil {
		if err := w.channel.Close(); err != nil {
			log.Printf("action: worker_cleanup | worker_id: %d | queue: %s | result: fail | error: %v",
				w.id, w.queueName, err)
		}
	}
	log.Printf("action: worker_cleanup | worker_id: %d | queue: %s | result: success",
		w.id, w.queueName)
}
