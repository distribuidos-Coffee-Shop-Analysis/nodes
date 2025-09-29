package node

import (
	"log"
	"sync"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/handlers"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Node represents a simple filter node that processes transactions from RabbitMQ queues
type Node struct {
	queueManager      *middleware.QueueManager
	handler           handlers.Handler
	clientWg   sync.WaitGroup // Track active goroutines
}

func NewNode(handler handlers.Handler, queueManager *middleware.QueueManager) *Node {
	fn := &Node{
		queueManager:      queueManager,
		handler:           handler,
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

	// Start transaction filter handler
	if err := node.startHandler(); err != nil {
		log.Printf("action: start_transaction_filter_handler | result: fail | error: %v", err)
		return err
	}

	log.Println("action: filter_node_started | result: success | msg: processing transactions...")

	
	return nil
}

// startTransactionFilterHandler starts the transaction filter handler
func (node *Node) startHandler() error {
	
	log.Println("action: transaction_filter_handler_started | result: success")

	// Consume with callback (blocks until StopConsuming or error)
	err := node.queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage) {
	
		go node.handler.Handle(batchMessage, node.queueManager.Connection, 
			node.queueManager.Wiring, &node.clientWg)
		
	})
	if err != nil {
		log.Printf("action: node_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the filter node
func (n *Node) Shutdown() {
	log.Printf("action: node_shutdown | handler: %s | result: start", n.handler.Name())

	if n.queueManager != nil {
		n.queueManager.StopConsuming() // Stop consuming new messages
		n.queueManager.Close() // Close connection to RabbitMQ
	}

	// Wait for all client goroutines to finish
	n.clientWg.Wait()

	log.Printf("action: node_shutdown | handler: %s | result: success", n.handler.Name())
}

