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

// Shutdown gracefully shuts down the filter node
func (n *Node) Shutdown() {
	log.Printf("action: node_shutdown | handler: %s | result: start", n.handler.Name())

	if n.queueManager != nil {
		n.queueManager.StopConsuming() // Stop consuming new messages
		n.queueManager.Close()         // Close connection to RabbitMQ
	}

	// Wait for all client goroutines to finish
	n.clientWg.Wait()

	log.Printf("action: node_shutdown | handler: %s | result: success", n.handler.Name())
}
