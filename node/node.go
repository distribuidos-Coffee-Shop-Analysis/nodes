package node

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
)

// Node represents a simple filter node that processes transactions from RabbitMQ queues
type Node struct {
	queueManager *middleware.QueueManager
	handler      Handler
	clientWg     sync.WaitGroup // Track active goroutines
}

func NewNode(handler Handler, queueManager *middleware.QueueManager) *Node {
	fn := &Node{
		queueManager: queueManager,
		handler:      handler,
	}

	log.Printf("action: node_init | result: success |")
	return fn
}

// Run is the main filter node entry point - processes transactions from RabbitMQ
func (node *Node) Run() error {
	defer node.Shutdown()

	// Connect to RabbitMQ
	if err := node.queueManager.Connect(); err != nil {
		log.Printf("action: filter_node_startup | result: fail | error: Could not connect to RabbitMQ: %v", err)
		return err
	}

	// Start handler depending on its type
	if err := node.handler.StartHandler(node.queueManager, &node.clientWg); err != nil {
		log.Printf("action: start_transaction_filter_handler | result: fail | error: %v", err)
		return err
	}

	log.Println("action: filter_node_started | result: success | msg: processing transactions...")

	return nil
}

// Shutdown gracefully shuts down the node
func (n *Node) Shutdown() {
	log.Printf("action: node_shutdown | handler: %s | result: start", n.handler.Name())

	if n.queueManager != nil {
		// Step 1: Stop consuming new messages from RabbitMQ
		n.queueManager.StopConsuming()
		log.Printf("action: node_shutdown | step: stop_consuming | result: success")
	}

	// Step 2: Wait for all in-flight messages to finish processing
	// This ensures all goroutines complete their ACK/NACK before we persist
	n.clientWg.Wait()
	log.Printf("action: node_shutdown | step: wait_goroutines | result: success")

	// Step 3: Persist state for stateful handlers (aggregate, joiner)
	// At this point, all messages have been ACKed and we know the exact state
	if err := n.handler.Shutdown(); err != nil {
		log.Printf("action: node_shutdown | step: handler_shutdown | result: fail | error: %v", err)
	} else {
		log.Printf("action: node_shutdown | step: handler_shutdown | result: success")
	}

	// Step 4: Close connection to RabbitMQ
	if n.queueManager != nil {
		n.queueManager.Close()
	}

	log.Printf("action: node_shutdown | handler: %s | result: success", n.handler.Name())
}
