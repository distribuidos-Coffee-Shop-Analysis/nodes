package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/handlers"
)

func main() {
	// Initialize logging
	initializeLog()

	cfg := common.GetConfig()
	nodeConfig := cfg.GetNodeConfig()

	// Create wiring based on the node role
	var wiring *common.NodeWiring
	switch nodeConfig.Role {
	case common.RoleFilterYear:
		wiring = common.BuildWiringForFilterYear(nodeConfig.Role, nodeConfig.NodeID)
	default:
		// Default to filter year if role not specified
		wiring = common.BuildWiringForFilterYear(common.RoleFilterYear, nodeConfig.NodeID)
	}

	handler := handlers.NewTransactionFilterHandler()
	// Create filter node
	node := node.NewNode(handler, middleware.NewQueueManagerWithWiring(wiring))

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start filter node in a goroutine
	go func() {
		if err := node.Run(); err != nil {
			log.Printf("action: filter_node_main | result: fail | error: %v", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("action: filter_node_shutdown | result: in_progress | msg: received signal %v", sig)

	// Graceful shutdown
	node.Shutdown()
}

// initializeLog initializes the logging configuration similar to Python version
func initializeLog() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("")
}
