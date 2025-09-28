package middleware

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueManager manages RabbitMQ connections and queue operations for the filter node
type QueueManager struct {
	host                     string
	port                     int
	username                 string
	password                 string
	connection               *amqp.Connection
	channel                  *amqp.Channel
	consuming                bool
	consumerTag              string
	inputQueue               string
	transactionsExchange     string
	transactionItemsExchange string
	inputQueueMapping        map[protocol.DatasetType]string
}

// NewQueueManager creates a new QueueManager instance
func NewQueueManager() *QueueManager {
	config := common.GetConfig()
	rabbitmqConfig := config.GetRabbitmqConfig()
	filterConfig := config.GetFilterConfig()

	qm := &QueueManager{
		host:                     rabbitmqConfig.Host,
		port:                     rabbitmqConfig.Port,
		username:                 rabbitmqConfig.Username,
		password:                 rabbitmqConfig.Password,
		consuming:                false,
		inputQueue:               filterConfig.InputQueue,
		transactionsExchange:     filterConfig.TransactionsExchange,
		transactionItemsExchange: filterConfig.TransactionItemsExchange,
	}

	// Create input queue mapping
	qm.inputQueueMapping = map[protocol.DatasetType]string{
		protocol.DatasetTypeTransactions:     qm.inputQueue,
		protocol.DatasetTypeTransactionItems: qm.inputQueue,
	}

	log.Printf("action: queue_manager_init | input_queue: %s | "+
		"transactions_exchange: %s | transaction_items_exchange: %s",
		qm.inputQueue, qm.transactionsExchange, qm.transactionItemsExchange)

	return qm
}

// Connect establishes connection to RabbitMQ
func (qm *QueueManager) Connect() error {
	var err error

	// Create connection string
	connStr := fmt.Sprintf("amqp://%s:%s@%s:%d/",
		qm.username, qm.password, qm.host, qm.port)

	qm.connection, err = amqp.Dial(connStr)
	if err != nil {
		log.Printf("action: rabbitmq_connect | result: fail | error: %v", err)
		return err
	}

	qm.channel, err = qm.connection.Channel()
	if err != nil {
		log.Printf("action: rabbitmq_channel | result: fail | error: %v", err)
		return err
	}

	// Declare input queue
	_, err = qm.channel.QueueDeclare(
		qm.inputQueue, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Printf("action: queue_declare | result: fail | queue: %s | error: %v",
			qm.inputQueue, err)
		return err
	}

	// Declare output exchanges
	if qm.transactionsExchange != "" {
		err = qm.channel.ExchangeDeclare(
			qm.transactionsExchange, // name
			"direct",                // type
			true,                    // durable
			false,                   // auto-deleted
			false,                   // internal
			false,                   // no-wait
			nil,                     // arguments
		)
		if err != nil {
			log.Printf("action: exchange_declare | result: fail | exchange: %s | error: %v",
				qm.transactionsExchange, err)
			return err
		}
	}

	if qm.transactionItemsExchange != "" {
		err = qm.channel.ExchangeDeclare(
			qm.transactionItemsExchange, // name
			"direct",                    // type
			true,                        // durable
			false,                       // auto-deleted
			false,                       // internal
			false,                       // no-wait
			nil,                         // arguments
		)
		if err != nil {
			log.Printf("action: exchange_declare | result: fail | exchange: %s | error: %v",
				qm.transactionItemsExchange, err)
			return err
		}
	}

	log.Printf("action: rabbitmq_connect | result: success | host: %s | "+
		"input_queue: %s | transactions_exchange: %s | transaction_items_exchange: %s",
		qm.host, qm.inputQueue, qm.transactionsExchange, qm.transactionItemsExchange)

	return nil
}

// Disconnect closes RabbitMQ connection
func (qm *QueueManager) Disconnect() {
	if qm.connection != nil && !qm.connection.IsClosed() {
		qm.connection.Close()
	}
	log.Println("action: rabbitmq_disconnect | result: success")
}

// StartConsuming starts consuming from configured input queue and calls callback for each message
func (qm *QueueManager) StartConsuming(callback func(*protocol.BatchMessage)) error {
	msgs, err := qm.channel.Consume(
		qm.inputQueue, // queue
		"",            // consumer
		false,         // auto-ack (we'll ack manually)
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		log.Printf("action: start_consuming | result: fail | error: %v", err)
		return err
	}

	qm.consuming = true
	log.Printf("action: start_consuming | result: success | queue: %s", qm.inputQueue)

	// Process messages
	for msg := range msgs {
		if !qm.consuming {
			break
		}

		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("action: process_transaction | result: fail | error: %v", r)
					msg.Nack(false, true) // Reject and requeue
				}
			}()

			var batchMessage *protocol.BatchMessage
			var err error

			// Check message type (first byte)
			if len(msg.Body) > 0 {
				msgType := msg.Body[0]
				if msgType == protocol.MessageTypeBatch {
					batchMessage, err = protocol.BatchMessageFromData(msg.Body)
				} else {
					err = fmt.Errorf("unknown message type: %d", msgType)
				}
			} else {
				err = fmt.Errorf("empty message body")
			}

			if err != nil {
				log.Printf("action: process_transaction | result: fail | error: %v", err)
				msg.Nack(false, true) // Reject and requeue
				return
			}

			// Call the callback with the parsed data
			callback(batchMessage)

			// Acknowledge the message
			msg.Ack(false)
		}()
	}

	return nil
}

// StopConsuming stops consuming messages
func (qm *QueueManager) StopConsuming() {
	qm.consuming = false
	log.Println("action: stop_consuming | result: success")
}

// SendToDatasetOutputExchanges sends filtered batch to all appropriate output exchanges for a dataset type
func (qm *QueueManager) SendToDatasetOutputExchanges(batchMessage *protocol.BatchMessage) error {
	var exchange string

	switch batchMessage.DatasetType {
	case protocol.DatasetTypeTransactions:
		exchange = qm.transactionsExchange
	case protocol.DatasetTypeTransactionItems:
		exchange = qm.transactionItemsExchange
	default:
		log.Printf("action: send_to_dataset_output_exchanges | result: fail | "+
			"error: DATASET TYPE: %s NOT SUPPORTED", batchMessage.DatasetType)
		return fmt.Errorf("dataset type %s not supported", batchMessage.DatasetType)
	}

	success := qm.sendFilteredBatch(exchange, batchMessage)
	if success {
		log.Printf("action: send_to_dataset_output_exchanges | result: success | "+
			"dataset_type: %s", batchMessage.DatasetType)
		return nil
	} else {
		log.Printf("action: send_to_dataset_output_exchanges | result: partial_fail | "+
			"dataset_type: %s", batchMessage.DatasetType)
		return fmt.Errorf("failed to send to exchange %s", exchange)
	}
}

// sendFilteredBatch sends filtered batch to specific output exchange by name
func (qm *QueueManager) sendFilteredBatch(exchangeName string, batchMessage *protocol.BatchMessage) bool {
	byteArray := qm.encodeToByteArray(batchMessage)

	err := qm.channel.Publish(
		exchangeName, // exchange
		"",           // routing key (no specific routing key needed for direct exchange)
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Make message persistent
			Body:         byteArray,
		})

	if err != nil {
		log.Printf("action: send_filtered_batch | result: fail | error: %v", err)
		return false
	}

	log.Printf("action: send_filtered_batch | result: success | exchange: %s | "+
		"original_type: %s | record_count: %d | eof: %t",
		exchangeName, batchMessage.DatasetType, len(batchMessage.Records), batchMessage.EOF)

	return true
}

// encodeToByteArray encodes batch message to byte array
func (qm *QueueManager) encodeToByteArray(batchMessage *protocol.BatchMessage) []byte {
	// [MessageType][DatasetType][EOF][RecordCount][Records...]
	data := make([]byte, 0)
	data = append(data, protocol.MessageTypeBatch)
	data = append(data, byte(batchMessage.DatasetType))

	// Build content: EOF|RecordCount|Record1|Record2|...
	eofValue := "0"
	if batchMessage.EOF {
		eofValue = "1"
	}

	content := fmt.Sprintf("%s|%d", eofValue, len(batchMessage.Records))
	for _, record := range batchMessage.Records {
		content += "|" + record.Serialize()
	}

	data = append(data, []byte(content)...)
	return data
}

// MessageMiddleware interface methods

// Send sends message to default transactions exchange (MessageMiddleware interface)
func (qm *QueueManager) Send(message []byte) error {
	err := qm.channel.Publish(
		qm.transactionsExchange, // exchange
		"",                      // routing key
		false,                   // mandatory
		false,                   // immediate
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
	var err error
	if qm.connection != nil && !qm.connection.IsClosed() {
		err = qm.connection.Close()
	}
	if err != nil {
		log.Printf("action: close | result: fail | error: %v", err)
	} else {
		log.Println("action: close | result: success")
	}
	return err
}

// Delete deletes an exchange
func (qm *QueueManager) Delete(exchangeName string) error {
	if qm.channel == nil {
		return fmt.Errorf("no channel available")
	}

	// Use provided exchange or default to transactions_exchange
	targetExchange := exchangeName
	if targetExchange == "" {
		targetExchange = qm.transactionsExchange
	}

	err := qm.channel.ExchangeDelete(targetExchange, false, false)
	if err != nil {
		log.Printf("action: delete | result: fail | exchange: %s | error: %v",
			targetExchange, err)
		return err
	}

	log.Printf("action: delete | result: success | exchange: %s", targetExchange)
	return nil
}

// SendDatasetBatch routes dataset batch to appropriate input queue based on dataset type (for compatibility)
func (qm *QueueManager) SendDatasetBatch(batchMessage *protocol.BatchMessage) error {
	queueName, exists := qm.inputQueueMapping[batchMessage.DatasetType]
	if !exists {
		return fmt.Errorf("no input queue mapping for dataset type: %s", batchMessage.DatasetType)
	}

	// Serialize the batch message for the queue
	messageData := map[string]interface{}{
		"dataset_type": batchMessage.DatasetType,
		"records":      make([]string, len(batchMessage.Records)),
		"eof":          batchMessage.EOF,
	}

	// Serialize records
	for i, record := range batchMessage.Records {
		messageData["records"].([]string)[i] = record.Serialize()
	}

	messageBody, err := json.Marshal(messageData)
	if err != nil {
		log.Printf("action: send_batch | result: fail | error: %v", err)
		return err
	}

	err = qm.channel.Publish(
		"",        // exchange (default)
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // Make message persistent
			Body:         messageBody,
		})

	if err != nil {
		log.Printf("action: send_batch | result: fail | error: %v", err)
		return err
	}

	log.Printf("action: send_batch | result: success | queue: %s | "+
		"dataset_type: %s | record_count: %d | eof: %t",
		queueName, batchMessage.DatasetType, len(batchMessage.Records), batchMessage.EOF)

	return nil
}
