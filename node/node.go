package node

import (
	"log"
	"sync"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/handlers"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// Node represents a simple filter node that processes transactions from RabbitMQ queues
type Node struct {
	queueManager      *middleware.QueueManager
	handler           handlers.Handler
	handlersLock      sync.Mutex
	shutdownRequested bool
	shutdownChan      chan struct{}
}

func NewNode(handler handlers.Handler, queueManager *middleware.QueueManager) *Node {
	fn := &Node{
		queueManager:      queueManager,
		handler:           handler,
		shutdownRequested: false,
		shutdownChan:      make(chan struct{}),
	}

	log.Printf("action: node_init | result: success |")
	return fn
}

// Run is the main filter node entry point - processes transactions from RabbitMQ
func (node *Node) Run() error {
	defer node.shutdown()

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

	// Keep the main routine alive while the filter handler processes
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-node.shutdownChan:
			log.Println("action: filter_node_shutdown | result: requested")
			return nil
		case <-ticker.C:
			// Check if transaction filter handler is still alive
			if node.handler != nil && !node.handler.IsAlive() {
				log.Println("action: filter_node | result: handler_stopped")
				return nil
			}
		}
	}
}

// startTransactionFilterHandler starts the transaction filter handler
func (node *Node) startHandler() error {
	node.handlersLock.Lock()
	defer node.handlersLock.Unlock()

	if err := node.handler.Start(); err != nil {
		return err
	}

	log.Println("action: transaction_filter_handler_started | result: success")

	// Consume with callback (blocks until StopConsuming or error)
	err := node.queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage) {
		if !node.handler.Accept(batchMessage.DatasetType) {
			return
		}

		outs, err := node.handler.Handle(batchMessage)
		if err != nil {
			log.Printf("action: handler_handle | result: fail | error: %v", err)
			return
		}
		if len(outs) == 0 {
			log.Printf("action: every record filtered out | result: success ")
			return
		}
		for _, out := range outs {
			if err := node.queueManager.SendToDatasetOutputExchanges(out); err != nil {
				log.Printf("action: node_publish | result: fail | error: %v", err)
			}
		}
	})
	if err != nil {
		log.Printf("action: node_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

// Shutdown gracefully shuts down the filter node
func (node *Node) Shutdown() {
	close(node.shutdownChan)
	node.shutdown()
}

// shutdown performs the actual shutdown operations
func (n *Node) shutdown() {
	if n.shutdownRequested {
		return
	}
	n.shutdownRequested = true
	log.Printf("action: node_shutdown | handler: %s | result: start", n.handler.Name())

	if n.queueManager != nil {
		n.queueManager.StopConsuming()
	}

	done := make(chan struct{})
	go func() {
		_ = n.handler.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Printf("action: node_shutdown | handler: %s | result: timeout_on_handler_close", n.handler.Name())
	}

	if n.queueManager != nil {
		if err := n.queueManager.Close(); err != nil {
			log.Printf("action: node_shutdown | result: warn | msg: error_closing_rabbit | error: %v", err)
		}
	}
	log.Printf("action: node_shutdown | handler: %s | result: success", n.handler.Name())
}
