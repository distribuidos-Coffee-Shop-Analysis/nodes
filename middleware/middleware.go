package middleware

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareChannel *amqp.Channel
type ConsumeChannel *<-chan amqp.Delivery

type MessageMiddlewareError int

const (
	MessageMiddlewareMessageError MessageMiddlewareError = iota + 1
	MessageMiddlewareDisconnectedError
	MessageMiddlewareCloseError
	MessageMiddlewareDeleteError
)

type MessageMiddlewareQueue struct {
	queueName      string
	channel        MiddlewareChannel
	consumeChannel ConsumeChannel
}

type MessageMiddlewareExchange struct {
	exchangeName   string
	routeKeys      []string
	amqpChannel    MiddlewareChannel
	consumeChannel ConsumeChannel
}

type onMessageCallback func(consumeChannel ConsumeChannel, done chan error)

// Puede especificarse un tipo más específico para T si se desea
type MessageMiddleware[T any] interface {
	/*
	   Comienza a escuchar a la cola/exchange e invoca a onMessageCallback tras
	   cada mensaje de datos o de control.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	StartConsuming(m *T, onMessageCallback onMessageCallback) (error MessageMiddlewareError)

	/*
	   Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	   no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	*/
	StopConsuming(m *T) (error MessageMiddlewareError)

	/*
	   Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	   Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	*/
	Send(m *T, message []byte) (error MessageMiddlewareError)

	/*
	   Se desconecta de la cola o exchange al que estaba conectado.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	*/
	Close(m *T) (error MessageMiddlewareError)

	/*
	   Se fuerza la eliminación remota de la cola o exchange.
	   Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	*/
	Delete(m *T) (error MessageMiddlewareError)
}
