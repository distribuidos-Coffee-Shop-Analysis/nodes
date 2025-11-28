package middleware

import (
	"fmt"
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

const HEARTBEAT_SECONDS = 3

// QueueManager manages RabbitMQ connections and queue operations using a Pipeline architecture
// The pipeline separates network I/O (few consumers) from processing (many processors)
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

// Connect establishes connection to RabbitMQ
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

// Disconnect closes RabbitMQ connection
func (qm *QueueManager) Disconnect() {
	if qm.Connection != nil && !qm.Connection.IsClosed() {
		qm.Connection.Close()
	}
	log.Println("action: rabbitmq_disconnect | result: success")
}

// getQueueName returns the queue name for a given binding index
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

// StartConsuming starts consuming from configured input queue(s) using the Pipeline architecture
// The pipeline separates network I/O from processing for better performance:
// - Few RabbitMQ consumers with high prefetch (network efficient)
// - Many processing goroutines reading from Go channels (CPU efficient)
// The callback handles everything: processing, publishing, and ACK/NACK
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

	// Start a pipeline for each queue binding
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

		// The callback handles everything: processing, publishing, ACK/NACK
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

	// Wait for all pipelines to complete
	wg.Wait()

	return nil
}

// StopConsuming stops all pipelines gracefully
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

// Close closes the connection and stops all pipelines
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
