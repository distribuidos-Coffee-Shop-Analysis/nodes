package server

import (
	"log"
	"sync"
	"time"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
)

// FilterNode represents a simple filter node that processes transactions from RabbitMQ queues
type FilterNode struct {
	queueManager             *middleware.QueueManager
	transactionFilterHandler *TransactionFilterHandler
	handlersLock             sync.Mutex
	shutdownRequested        bool
	shutdownChan             chan struct{}
}

// NewFilterNode creates a new FilterNode instance
func NewFilterNode() *FilterNode {
	fn := &FilterNode{
		queueManager:      middleware.NewQueueManager(),
		shutdownRequested: false,
		shutdownChan:      make(chan struct{}),
	}

	log.Println("action: filter_node_init | result: success")
	return fn
}

// Run is the main filter node entry point - processes transactions from RabbitMQ
func (fn *FilterNode) Run() error {
	defer fn.shutdown()

	// Connect to RabbitMQ
	if err := fn.queueManager.Connect(); err != nil {
		log.Printf("action: filter_node_startup | result: fail | error: Could not connect to RabbitMQ: %v", err)
		return err
	}

	// Start transaction filter handler
	if err := fn.startTransactionFilterHandler(); err != nil {
		log.Printf("action: start_transaction_filter_handler | result: fail | error: %v", err)
		return err
	}

	log.Println("action: filter_node_started | result: success | msg: processing transactions...")

	// Keep the main routine alive while the filter handler processes
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fn.shutdownChan:
			log.Println("action: filter_node_shutdown | result: requested")
			return nil
		case <-ticker.C:
			// Check if transaction filter handler is still alive
			if fn.transactionFilterHandler != nil && !fn.transactionFilterHandler.IsAlive() {
				log.Println("action: filter_node | result: handler_stopped")
				return nil
			}
		}
	}
}

// startTransactionFilterHandler starts the transaction filter handler
func (fn *FilterNode) startTransactionFilterHandler() error {
	fn.handlersLock.Lock()
	defer fn.handlersLock.Unlock()

	fn.transactionFilterHandler = NewTransactionFilterHandler(fn.removeFilterHandler)

	if err := fn.transactionFilterHandler.Start(); err != nil {
		return err
	}

	log.Println("action: transaction_filter_handler_started | result: success")
	return nil
}

// removeFilterHandler removes the finished transaction filter handler
func (fn *FilterNode) removeFilterHandler(handler *TransactionFilterHandler) {
	fn.handlersLock.Lock()
	defer fn.handlersLock.Unlock()

	if fn.transactionFilterHandler == handler {
		fn.transactionFilterHandler = nil
	}
	log.Println("action: remove_filter_handler | result: success")
}

// Shutdown gracefully shuts down the filter node
func (fn *FilterNode) Shutdown() {
	close(fn.shutdownChan)
	fn.shutdown()
}

// shutdown performs the actual shutdown operations
func (fn *FilterNode) shutdown() {
	fn.shutdownRequested = true
	log.Println("action: filter_node_shutdown | result: in_progress | msg: starting graceful shutdown")

	// Shutdown transaction filter handler
	if fn.transactionFilterHandler != nil {
		fn.transactionFilterHandler.RequestShutdown()

		// Wait up to 5 seconds for graceful shutdown
		done := make(chan struct{})
		go func() {
			fn.transactionFilterHandler.Wait()
			close(done)
		}()

		select {
		case <-done:
			log.Println("action: filter_handler_shutdown | result: success")
		case <-time.After(5 * time.Second):
			log.Println("action: filter_node_shutdown | result: warning | msg: filter handler didn't stop gracefully")
		}
	}

	// Close RabbitMQ connection
	if fn.queueManager != nil {
		if err := fn.queueManager.Close(); err != nil {
			log.Printf("action: filter_node_shutdown | result: fail | msg: error closing RabbitMQ connection | error: %v", err)
		}
	}

	log.Println("action: filter_node_shutdown | result: success | msg: graceful shutdown completed")
}
