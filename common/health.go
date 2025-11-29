package common

import (
	"io"
	"log"
	"net"
)

const (
	pingMessage = "PING"
	pongMessage = "PONG"
)

// StartHealthServer starts a TCP health check server on the specified port
// It listens for "PING" messages and responds with "PONG"
// This function should be called in a goroutine as it blocks indefinitely
func StartHealthServer(port string) {
	address := "0.0.0.0:" + port
	
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("action: start_health_server | result: fail | error: failed to bind to %s: %v", address, err)
	}
	defer listener.Close()

	log.Printf("action: start_health_server | result: success | address: %s", address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("action: accept_health_connection | result: fail | error: %v", err)
			continue
		}

		// Handle each connection in a separate goroutine
		go handleHealthCheck(conn)
	}
}

// handleHealthCheck handles a single health check connection
func handleHealthCheck(conn net.Conn) {
	defer conn.Close()

	// Read incoming data
	buffer := make([]byte, len(pingMessage))
	n, err := conn.Read(buffer)
	if err != nil {
		if err != io.EOF {
			log.Printf("action: read_health_check | result: fail | error: %v", err)
		}
		return
	}

	message := string(buffer[:n])
	
	// If message is PING, respond with PONG
	if message == pingMessage {
		_, err = conn.Write([]byte(pongMessage))
		if err != nil {
			log.Printf("action: write_health_response | result: fail | error: %v", err)
			return
		}
		log.Printf("action: health_check | result: success | msg: responded to PING with PONG")
	} else {
		log.Printf("action: health_check | result: fail | msg: unexpected message: %s", message)
	}
}

