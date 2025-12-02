package middleware

import (
	"log"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQ consumer that fetches messages
// and puts them into a Go channel for processing.
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

// NewConsumer creates a consumer with its own dedicated channel.
func NewConsumer(
	id int,
	queueName string,
	connection *amqp.Connection,
	outputChan chan<- MessagePacket,
	prefetch int,
	shutdownCh <-chan struct{},
) (*Consumer, error) {
	channel, err := connection.Channel()
	if err != nil {
		return nil, err
	}

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

// Start consuming messages from RabbitMQ and forwarding to the output channel.
func (c *Consumer) Start() {
	defer close(c.doneCh)
	defer c.cleanup()

	channelCloseCh := make(chan *amqp.Error, 1)
	c.channel.NotifyClose(channelCloseCh)

	connCloseCh := make(chan *amqp.Error, 1)
	c.connection.NotifyClose(connCloseCh)

	msgs, err := c.channel.Consume(
		c.queueName, // queue
		"",          // consumer tag
		false,       // auto-ack: false
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

			packet, ok := c.parseMessage(msg)
			if !ok {
				continue
			}

			// Forward to output channel
			select {
			case c.outputChan <- packet:
			case <-c.shutdownCh:
				msg.Nack(false, true)
				return
			}
		}
	}
}

// parseMessage parses a RabbitMQ delivery into a MessagePacket
// Returns false if the message is invalid
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
			if err != amqp.ErrClosed {
				log.Printf("action: consumer_cleanup | consumer_id: %d | queue: %s | result: fail | error: %v",
					c.id, c.queueName, err)
			}
		}
	}
	log.Printf("action: consumer_cleanup | consumer_id: %d | queue: %s | result: success",
		c.id, c.queueName)
}
