package middleware

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

// PipelineConfig holds configuration for the pipeline architecture
type PipelineConfig struct {
	// RabbitMQ consumer settings
	NumConsumers     int // Number of RabbitMQ consumer goroutines (recommended: 20-50)
	ConsumerPrefetch int // Prefetch count per consumer (recommended: 50-100)

	// Processing settings
	NumProcessors int // Number of processing goroutines (can be high: 1000-5000)

	// Buffer sizes
	InputBufferSize int // Size of the consumer->processor channel buffer
}

// DefaultPipelineConfig returns sensible defaults for the pipeline
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		NumConsumers:     20,
		ConsumerPrefetch: 50,
		NumProcessors:    1000,
		InputBufferSize:  1000,
	}
}

// MessagePacket wraps a batch message with its delivery for ACK tracking
// This travels through the pipeline from consumer -> processor
type MessagePacket struct {
	Batch    *protocol.BatchMessage
	Delivery amqp.Delivery
}

// Pipeline orchestrates the consumer -> processor flow
// Handlers are responsible for their own publishing and ACK/NACK
type Pipeline struct {
	config PipelineConfig

	// Channel for inter-stage communication
	inputChan chan MessagePacket // Consumer -> Processor

	// Components
	consumers  []*Consumer
	processors []*Processor

	// Lifecycle management
	shutdownCh chan struct{}
	wg         sync.WaitGroup

	// Connection
	connection *amqp.Connection
}

// NewPipeline creates a new pipeline with the given configuration
func NewPipeline(config PipelineConfig, connection *amqp.Connection) *Pipeline {
	return &Pipeline{
		config:     config,
		connection: connection,
		inputChan:  make(chan MessagePacket, config.InputBufferSize),
		shutdownCh: make(chan struct{}),
		consumers:  make([]*Consumer, 0, config.NumConsumers),
		processors: make([]*Processor, 0, config.NumProcessors),
	}
}

// ProcessorFunc is the function signature for processing a batch
// The callback should handle everything: processing, publishing, and ACK/NACK.
type ProcessorFunc func(batch *protocol.BatchMessage, delivery amqp.Delivery)

// Start initializes and starts all pipeline components
func (p *Pipeline) Start(queueName string, processorFunc ProcessorFunc) error {
	log.Printf("action: pipeline_start | consumers: %d | processors: %d | prefetch: %d | buffer: %d",
		p.config.NumConsumers, p.config.NumProcessors, p.config.ConsumerPrefetch, p.config.InputBufferSize)

	// Monitor connection for unexpected closures
	connCloseCh := make(chan *amqp.Error, 1)
	p.connection.NotifyClose(connCloseCh)
	go func() {
		select {
		case amqpErr, ok := <-connCloseCh:
			if ok && amqpErr != nil {
				log.Printf("action: pipeline_connection_error | code: %d | reason: %s | server: %v | recover: %v",
					amqpErr.Code, amqpErr.Reason, amqpErr.Server, amqpErr.Recover)
			} else if ok {
				log.Printf("action: pipeline_connection_closed | reason: graceful_close")
			}
		case <-p.shutdownCh:
			// Pipeline is shutting down normally
		}
	}()

	// Start processors first (they need to be ready to receive from inputChan)
	for i := 0; i < p.config.NumProcessors; i++ {
		proc := NewProcessor(i, p.inputChan, processorFunc, p.shutdownCh)
		p.processors = append(p.processors, proc)
		p.wg.Add(1)
		go func(pr *Processor) {
			defer p.wg.Done()
			pr.Start()
		}(proc)
	}
	log.Printf("action: pipeline_processors_started | count: %d", len(p.processors))

	// Start consumers (they will start producing to inputChan)
	for i := 0; i < p.config.NumConsumers; i++ {
		consumer, err := NewConsumer(i, queueName, p.connection, p.inputChan, p.config.ConsumerPrefetch, p.shutdownCh)
		if err != nil {
			log.Printf("action: pipeline_create_consumer | consumer_id: %d | result: fail | error: %v", i, err)
			p.Stop()
			return err
		}
		p.consumers = append(p.consumers, consumer)
		p.wg.Add(1)
		go func(c *Consumer) {
			defer p.wg.Done()
			c.Start()
		}(consumer)
	}
	log.Printf("action: pipeline_consumers_started | count: %d | queue: %s", len(p.consumers), queueName)

	return nil
}

// Stop gracefully shuts down the pipeline
func (p *Pipeline) Stop() {
	log.Println("action: pipeline_stop | status: initiating_shutdown")

	// Signal all components to stop
	close(p.shutdownCh)

	log.Println("action: pipeline_stop | status: waiting_for_all_components")
	p.wg.Wait()

	log.Println("action: pipeline_stop | result: success")
}

// Wait blocks until the pipeline finishes
func (p *Pipeline) Wait() {
	p.wg.Wait()
}
