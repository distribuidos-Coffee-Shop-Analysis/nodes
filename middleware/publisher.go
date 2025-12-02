package middleware

import (
	"fmt"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/common"
	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	channel *amqp.Channel
	wiring  *common.NodeWiring
}

func NewPublisher(connection *amqp.Connection, wiring *common.NodeWiring) (*Publisher, error) {

	ch, err := connection.Channel()
	if err != nil {
		return nil, err
	}

	return &Publisher{
		channel: ch,
		wiring:  wiring,
	}, nil
}

func (p *Publisher) SendToDatasetOutputExchanges(b *protocol.BatchMessage) error {
	route, ok := p.wiring.Outputs[b.DatasetType]
	if !ok {
		return fmt.Errorf("no output route for dataset %v in role %s", b.DatasetType, p.wiring.Role)
	}
	return p.publish(route.Exchange, route.RoutingKey, common.EncodeToByteArray(b))
}

func (p *Publisher) SendToDatasetOutputExchangesWithRoutingKey(b *protocol.BatchMessage, customRoutingKey string) error {
	route, ok := p.wiring.Outputs[b.DatasetType]
	if !ok {
		return fmt.Errorf("no output route for dataset %v in role %s", b.DatasetType, p.wiring.Role)
	}
	return p.publish(route.Exchange, customRoutingKey, common.EncodeToByteArray(b))
}

func (p *Publisher) publish(exchange, rk string, body []byte) error {
	return p.channel.Publish(exchange, rk, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
}

func (p *Publisher) Close() {
	p.channel.Close()
}
