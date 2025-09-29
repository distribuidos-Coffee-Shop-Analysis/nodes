package main

import (
	"fmt"
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

	configPath := fmt.Sprintf("/app/config/%s.json", string(nodeConfig.Role))
	wiring, err := common.BuildWiringFromConfig(configPath, nodeConfig.NodeID)
	if err != nil {
		log.Printf("action: load_wiring_config | result: fail | error: %v", err)
		return
	}

	handler := handlers.NewHandler(nodeConfig.Role)
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
