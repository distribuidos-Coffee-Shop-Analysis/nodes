package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/server"
)

func main() {
	// Initialize logging
	initializeLog()

	// Create filter node
	filterNode := server.NewFilterNode()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start filter node in a goroutine
	go func() {
		if err := filterNode.Run(); err != nil {
			log.Printf("action: filter_node_main | result: fail | error: %v", err)
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	sig := <-sigChan
	log.Printf("action: filter_node_shutdown | result: in_progress | msg: received signal %v", sig)

	// Graceful shutdown
	filterNode.Shutdown()
}

// initializeLog initializes the logging configuration similar to Python version
func initializeLog() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("")
}
