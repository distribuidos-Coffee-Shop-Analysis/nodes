package handlers

import "github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"

// Defines the contract that any handler must fulfill to plug into the generic node.
type Handler interface {
	// The name of the handler, used for logging.
	Name() string 
	// Start is called once before starting to consume.
	// The handler does not manage connections to Rabbit: it only prepares internal state.
	Start() error

	// Handle processes a batch WITHOUT side effects from IO (ideally pure).
	// It may mutate the batch or generate new ones; it returns the batch(es) to publish.
	// If there is nothing to emit, it may return nil, nil.
	Handle(batch *protocol.BatchMessage) ([]*protocol.BatchMessage, error)

	// Accept allows filtering which datasets this handler processes (optimizes early-drop).
	Accept(dataset protocol.DatasetType) bool

	// Close is called on shutdown to release internal resources.
	Close() error

	// IsAlive allows the node to expose health/liveness.
	IsAlive() bool
}
