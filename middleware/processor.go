package middleware

import (
	"log"
)

// Processor reads from an input Go channel, processes messages using a callback.
// The callback is responsible for processing, publishing, and ACK/NACK.
// You can create thousands of these since they don't hold network resources.
type Processor struct {
	id          int
	inputChan   <-chan MessagePacket
	processFunc ProcessorFunc
	shutdownCh  <-chan struct{}
	doneCh      chan struct{}
}

// NewProcessor creates a new processor
func NewProcessor(
	id int,
	inputChan <-chan MessagePacket,
	processFunc ProcessorFunc,
	shutdownCh <-chan struct{},
) *Processor {
	return &Processor{
		id:          id,
		inputChan:   inputChan,
		processFunc: processFunc,
		shutdownCh:  shutdownCh,
		doneCh:      make(chan struct{}),
	}
}

// Start begins processing messages from the input channel
func (p *Processor) Start() {
	defer close(p.doneCh)

	for {
		select {
		case <-p.shutdownCh:
			// Drain remaining messages in input channel before exiting
			p.drainAndProcess()
			return

		case packet, ok := <-p.inputChan:
			if !ok {
				// Input channel closed, we're done
				return
			}

			p.processPacket(packet)
		}
	}
}

// drainAndProcess processes any remaining messages in the input channel
func (p *Processor) drainAndProcess() {
	for {
		select {
		case packet, ok := <-p.inputChan:
			if !ok {
				return
			}
			p.processPacket(packet)
		default:
			// No more messages to drain
			return
		}
	}
}

// processPacket processes a single message packet with panic recovery
func (p *Processor) processPacket(packet MessagePacket) {
	// Process with panic recovery
	func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("action: processor_panic | processor_id: %d | error: %v | client_id: %s | batch_index: %d",
					p.id, r, packet.Batch.ClientID, packet.Batch.BatchIndex)
				// On panic, NACK and requeue
				packet.Delivery.Nack(false, true)
			}
		}()

		// The callback handles everything: processing, publishing, ACK/NACK
		p.processFunc(packet.Batch, packet.Delivery)
	}()
}

// Done returns a channel that is closed when the processor has finished
func (p *Processor) Done() <-chan struct{} {
	return p.doneCh
}
