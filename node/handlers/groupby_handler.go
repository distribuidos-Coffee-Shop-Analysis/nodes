package handlers

import (
	"log"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/groupbys"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

type GroupByHandler struct {
	groupby groupbys.RecordGroupBy
}

func NewGroupByHandler(groupby groupbys.RecordGroupBy) *GroupByHandler {
	return &GroupByHandler{
		groupby: groupby,
	}
}

func (h *GroupByHandler) Name() string {
	return "groupby_" + h.groupby.Name()
}

// StartHandler starts the groupby handler
func (h *GroupByHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	err := queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {
		h.Handle(batchMessage, queueManager.Connection,
			queueManager.Wiring, clientWg, delivery)
	})
	if err != nil {
		log.Printf("action: groupby_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

// Handle processes a transaction batch - groups records and routes to output exchanges
func (h *GroupByHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	log.Printf("action: groupby_batch_received | groupby: %s | result: success | "+
		"dataset_type: %s | record_count: %d | eof: %t",
		h.groupby.Name(), batchMessage.DatasetType, len(batchMessage.Records), batchMessage.EOF)

	groupedRecords, err := h.groupby.ProcessBatch(batchMessage.Records, batchMessage.EOF)
	if err != nil {
		log.Printf("action: groupby_process | groupby: %s | result: fail | error: %v", h.groupby.Name(), err)
		msg.Nack(false, true) // Reject and requeue
		return err
	}

	batchIndex := batchMessage.BatchIndex

	var groupByBatch *protocol.BatchMessage
	if h.groupby.Name() == "q2_groupby_year_month_item" {
		groupByBatch = protocol.NewQ2GroupByBatch(
			batchIndex,
			groupedRecords,
			batchMessage.EOF,
		)
	} else {
		groupByBatch = protocol.NewGroupByBatch(
			batchIndex,
			groupedRecords,
			batchMessage.EOF,
		)
	}

	publisher, err := middleware.NewPublisher(connection, wiring)
	if err != nil {
		log.Printf("action: create_publisher | groupby: %s | result: fail | error: %v", h.groupby.Name(), err)
		msg.Nack(false, true)
		return err
	}

	if err := publisher.SendToDatasetOutputExchanges(groupByBatch); err != nil {
		log.Printf("action: groupby_publish | groupby: %s | result: fail | error: %v", h.groupby.Name(), err)
		msg.Nack(false, true)
		return err
	}

	publisher.Close()

	log.Printf("action: groupby_publish | groupby: %s | result: success | "+
		"batch_index: %d | record_count: %d | eof: %t",
		h.groupby.Name(), batchIndex, len(groupedRecords), batchMessage.EOF)

	msg.Ack(false) // Acknowledge msg

	return nil
}
