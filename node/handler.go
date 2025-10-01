package node

import (
	"sync"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/middleware"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/aggregates"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/filters"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/groupbys"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/node/handlers"
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
	Handle(batchMessage *protocol.BatchMessage, connection *amqp091.Connection, wiring *common.NodeWiring, clientWG *sync.WaitGroup, delivery amqp091.Delivery) error

	StartHandler(queueManager *middleware.QueueManager, clientWG *sync.WaitGroup) error
}

func NewHandler(role common.NodeRole) Handler {
	switch role {
	case common.RoleFilterYear:
		filter := filters.NewYearFilter(2024, 2025)
		return handlers.NewFilterHandler(filter)
	case common.RoleFilterHour:
		filter := filters.NewHourFilter(6, 23)
		return handlers.NewFilterHandler(filter)
	case common.RoleFilterAmount:
		filter := filters.NewAmountFilter(75)
		return handlers.NewFilterHandler(filter)
	case common.RoleGroupByQ4:
		groupby := groupbys.NewQ4GroupBy()
		return handlers.NewGroupByHandler(groupby)
	case common.RoleGroupByQ3:
		groupby := groupbys.NewQ3GroupBy()
		return handlers.NewGroupByHandler(groupby)
	case common.RoleGroupByQ2:
		groupby := groupbys.NewQ2GroupBy()
		return handlers.NewGroupByHandler(groupby)
	case common.RoleAggregateQ2:
		aggregate := aggregates.NewQ2Aggregate()
		return handlers.NewAggregateHandler(aggregate)
	case common.RoleAggregateQ3:
		aggregate := aggregates.NewQ3Aggregate()
		return handlers.NewAggregateHandler(aggregate)
	case common.RoleAggregateQ4:
		aggregate := aggregates.NewQ4Aggregate()
		return handlers.NewAggregateHandler(aggregate)
	default:
		panic("unknown role for handler")
	}
}
