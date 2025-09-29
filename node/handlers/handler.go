package handlers

import (
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

// Defines the contract that any handler must fulfill to plug into the generic node.
type Handler interface {
	// The name of the handler, used for logging.
	Name() string 
	// Start is called once before starting to consume.
	// The handler does not manage connections to Rabbit: it only prepares internal state.
	
	// Handle processes a batch WITHOUT side effects from IO (ideally pure).
	// It may mutate the batch or generate new ones; it returns the batch(es) to publish.
	// If there is nothing to emit, it may return nil, nil.
	Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection, wiring *common.NodeWiring, clientWG *sync.WaitGroup) (error)

}
