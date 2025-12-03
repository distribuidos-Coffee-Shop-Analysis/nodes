package handlers

import (
	"log"
	"strings"
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/groupbys"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	"github.com/rabbitmq/amqp091-go"
)

type GroupByHandler struct {
	groupby        groupbys.GroupBy
	pub            *middleware.Publisher
	pubMu          sync.Mutex
	queryType      string
	aggregateCount int
}

func NewGroupByHandler(groupby groupbys.GroupBy) *GroupByHandler {
	cfg := common.GetConfig()
	queryType := getQueryType(groupby.Name())
	aggregateCount := getAggregateCountForQueryType(cfg, queryType)

	return &GroupByHandler{
		groupby:        groupby,
		queryType:      queryType,
		aggregateCount: aggregateCount,
	}
}

func getQueryType(name string) string {
	nameLower := strings.ToLower(name)
	if strings.Contains(nameLower, "q2") {
		return "q2"
	}
	if strings.Contains(nameLower, "q3") {
		return "q3"
	}
	if strings.Contains(nameLower, "q4") {
		return "q4"
	}
	return ""
}

// getAggregateCountForQueryType returns the aggregate count for the given query type
func getAggregateCountForQueryType(cfg *common.Config, queryType string) int {
	switch queryType {
	case "q2":
		return cfg.GetQ2AggregateCount()
	case "q3":
		return cfg.GetQ3AggregateCount()
	case "q4":
		return cfg.GetQ4AggregateCount()
	default:
		return 1
	}
}

func (h *GroupByHandler) Name() string {
	return "groupby_" + h.groupby.Name()
}

func (h *GroupByHandler) Shutdown() error {
	return nil
}

func (h *GroupByHandler) StartHandler(queueManager *middleware.QueueManager, clientWg *sync.WaitGroup) error {
	pub, err := middleware.NewPublisher(queueManager.Connection, queueManager.Wiring)
	if err != nil {
		log.Printf("action: create_publisher | result: fail | error: %v", err)
		return err
	}
	h.pub = pub
	log.Printf("action: create_publisher | result: success | handler: %s", h.Name())

	err = queueManager.StartConsuming(func(batchMessage *protocol.BatchMessage, delivery amqp091.Delivery) {
		h.Handle(batchMessage, queueManager.Connection,
			queueManager.Wiring, clientWg, delivery)
	})
	if err != nil {
		log.Printf("action: groupby_consume | result: fail | error: %v", err)
		return err
	}
	return nil
}

func (h *GroupByHandler) Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection,
	wiring *common.NodeWiring, clientWG *sync.WaitGroup, msg amqp091.Delivery) error {

	clientWG.Add(1)
	defer clientWG.Done()

	groupedRecords, err := h.groupby.ProcessBatch(batchMessage.Records, batchMessage.EOF)
	if err != nil {
		log.Printf("action: groupby_process | groupby: %s | result: fail | error: %v", h.groupby.Name(), err)
		msg.Nack(false, true)
		return err
	}

	batchIndex := batchMessage.BatchIndex
	clientID := batchMessage.ClientID

	groupByBatch := h.groupby.NewGroupByBatch(batchIndex, groupedRecords, batchMessage.EOF, clientID)

	partition := common.GetAggregatePartition(clientID, h.aggregateCount)
	routingKey := common.BuildAggregateRoutingKey(h.queryType, partition)

	h.pubMu.Lock()
	err = h.pub.SendToDatasetOutputExchangesWithRoutingKey(groupByBatch, routingKey)
	h.pubMu.Unlock()

	if err != nil {
		log.Printf("action: groupby_publish | groupby: %s | result: fail | error: %v", h.groupby.Name(), err)
		msg.Nack(false, true)
		return err
	}

	msg.Ack(false)

	return nil
}
