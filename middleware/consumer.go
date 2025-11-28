package middleware

import (
	"log"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer is a lightweight RabbitMQ consumer that only fetches messages
// and puts them into a Go channel for processing. It does NOT process messages.
// Use many Processor goroutines to handle the actual processing.
type Consumer struct {
	id         int
	queueName  string
	connection *amqp.Connection
	channel    *amqp.Channel
	outputChan chan<- MessagePacket
	prefetch   int
	shutdownCh <-chan struct{}
	doneCh     chan struct{}
}

// NewConsumer creates a consumer with its own dedicated channel and high prefetch
func NewConsumer(
	id int,
	queueName string,
	connection *amqp.Connection,
	outputChan chan<- MessagePacket,
	prefetch int,
	shutdownCh <-chan struct{},
) (*Consumer, error) {
	// Create dedicated channel for this consumer
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	// Set QoS with high prefetch for batch fetching
	// This allows us to fetch many messages at once, reducing network round-trips
	if err := channel.Qos(prefetch, 0, false); err != nil {
		channel.Close()
		return nil, err
	}

	return &Consumer{
		id:         id,
		queueName:  queueName,
		connection: connection,
		channel:    channel,
		outputChan: outputChan,
		prefetch:   prefetch,
		shutdownCh: shutdownCh,
		doneCh:     make(chan struct{}),
	}, nil
}

// Start begins consuming messages from RabbitMQ and forwarding to the output channel
func (c *Consumer) Start() {
	defer close(c.doneCh)
	defer c.cleanup()

	// Register for channel close notifications to capture the reason
	channelCloseCh := make(chan *amqp.Error, 1)
	c.channel.NotifyClose(channelCloseCh)

	// Register for connection close notifications
	connCloseCh := make(chan *amqp.Error, 1)
	c.connection.NotifyClose(connCloseCh)

	msgs, err := c.channel.Consume(
		c.queueName, // queue
		"",          // consumer tag (auto-generated)
		false,       // auto-ack (false - we'll ACK after processing)
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Printf("action: consumer_start | consumer_id: %d | queue: %s | result: fail | error: %v",
			c.id, c.queueName, err)
		return
	}

	log.Printf("action: consumer_started | consumer_id: %d | queue: %s | prefetch: %d",
		c.id, c.queueName, c.prefetch)

	// Consume messages until shutdown or error
	for {
		select {
		case <-c.shutdownCh:
			log.Printf("action: consumer_shutdown | consumer_id: %d | queue: %s | result: received_shutdown_signal",
				c.id, c.queueName)
			return

		case amqpErr, ok := <-channelCloseCh:
			if ok && amqpErr != nil {
				log.Printf("action: consumer_channel_error | consumer_id: %d | queue: %s | code: %d | reason: %s | server: %v | recover: %v",
					c.id, c.queueName, amqpErr.Code, amqpErr.Reason, amqpErr.Server, amqpErr.Recover)
			} else {
				log.Printf("action: consumer_channel_closed | consumer_id: %d | queue: %s | reason: channel_close_notification",
					c.id, c.queueName)
			}
			return

		case amqpErr, ok := <-connCloseCh:
			if ok && amqpErr != nil {
				log.Printf("action: consumer_connection_error | consumer_id: %d | queue: %s | code: %d | reason: %s | server: %v | recover: %v",
					c.id, c.queueName, amqpErr.Code, amqpErr.Reason, amqpErr.Server, amqpErr.Recover)
			} else {
				log.Printf("action: consumer_connection_closed | consumer_id: %d | queue: %s | reason: connection_close_notification",
					c.id, c.queueName)
			}
			return

		case msg, ok := <-msgs:
			if !ok {
				// Check if there's a pending error that explains the closure
				select {
				case amqpErr := <-channelCloseCh:
					if amqpErr != nil {
						log.Printf("action: consumer_channel_closed | consumer_id: %d | queue: %s | code: %d | reason: %s",
							c.id, c.queueName, amqpErr.Code, amqpErr.Reason)
					} else {
						log.Printf("action: consumer_channel_closed | consumer_id: %d | queue: %s | reason: msgs_channel_closed_no_error",
							c.id, c.queueName)
					}
				default:
					log.Printf("action: consumer_channel_closed | consumer_id: %d | queue: %s | reason: msgs_channel_closed_unexpectedly",
						c.id, c.queueName)
				}
				return
			}

			// Parse the message
			packet, ok := c.parseMessage(msg)
			if !ok {
				// Message was invalid, already ACK'd in parseMessage
				continue
			}

			// Forward to output channel (blocking if channel is full)
			// This provides natural backpressure
			select {
			case c.outputChan <- packet:
				// Message forwarded successfully
			case <-c.shutdownCh:
				// Shutdown while trying to send - NACK and requeue
				msg.Nack(false, true)
				return
			}
		}
	}
}

// parseMessage parses a RabbitMQ delivery into a MessagePacket
// Returns false if the message is invalid (will be ACK'd to discard)
func (c *Consumer) parseMessage(msg amqp.Delivery) (MessagePacket, bool) {
	if len(msg.Body) == 0 {
		log.Printf("action: consumer_parse | consumer_id: %d | queue: %s | result: fail | error: empty message body",
			c.id, c.queueName)
		msg.Ack(false) // Discard invalid message
		return MessagePacket{}, false
	}

	msgType := msg.Body[0]
	if msgType != protocol.MessageTypeBatch {
		log.Printf("action: consumer_parse | consumer_id: %d | queue: %s | result: fail | error: unknown message type: %d",
			c.id, c.queueName, msgType)
		msg.Ack(false) // Discard invalid message
		return MessagePacket{}, false
	}

	batchMessage, err := protocol.BatchMessageFromData(msg.Body)
	if err != nil {
		log.Printf("action: consumer_parse | consumer_id: %d | queue: %s | result: fail | error: %v",
			c.id, c.queueName, err)
		msg.Ack(false) // Discard invalid message
		return MessagePacket{}, false
	}

	return MessagePacket{
		Batch:    batchMessage,
		Delivery: msg,
	}, true
}

// Done returns a channel that is closed when the consumer has finished
func (c *Consumer) Done() <-chan struct{} {
	return c.doneCh
}

// cleanup closes the consumer's channel
func (c *Consumer) cleanup() {
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			// Only log if it's not already closed
			if err != amqp.ErrClosed {
				log.Printf("action: consumer_cleanup | consumer_id: %d | queue: %s | result: fail | error: %v",
					c.id, c.queueName, err)
			}
		}
	}
	log.Printf("action: consumer_cleanup | consumer_id: %d | queue: %s | result: success",
		c.id, c.queueName)
}
