package rabbitmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/thalesfsp/queue/queue"
)

//////
// Const, vars, and types.
//////

// SubscribeParams defines the parameters for subscribing to a queue.
type SubscribeParams struct {
	queue.SubscribeParams

	// Args are optional arguments that have specific semantics for the queue or
	// server.
	Args amqp.Table `json:"args"`

	// AutoAck acknowledges deliveries to this consumer prior to writing the
	// delivery to the network.
	AutoAck bool `default:"false" json:"autoAck"`

	// AutoDelete deletes the queue when the last consumer unsubscribes.
	AutoDelete bool `default:"false" json:"autoDelete"`

	// Consumer is a unique string that identifies the consumer.
	Consumer string `json:"consumer"`

	// Durability ensures the queue survives a broker restart.
	Durable bool `default:"true" json:"durable"`

	// Exclusive ensures that this is the sole consumer from this queue.
	Exclusive bool `default:"false" json:"exclusive"`

	// NoLocal is not supported by RabbitMQ.
	NoLocal bool `default:"false" json:"noLocal"`

	// NoWait does not wait for the server to confirm the request and immediately
	// begins deliveries.
	NoWait bool `default:"false" json:"noWait"`
}

// NewSubscribeParams creates a new SubscribeParams.
func NewSubscribeParams() *SubscribeParams {
	return &SubscribeParams{
		SubscribeParams: queue.SubscribeParams{
			ContextTimeout: 5 * time.Second,
		},
		AutoAck:    false,
		AutoDelete: false,
		Consumer:   "",
		Durable:    true,
		Exclusive:  false,
		NoLocal:    false,
		NoWait:     false,
	}
}

// PublishParams defines the parameters for publishing a message to a queue.
type PublishParams struct {
	queue.PublishParams

	// ContentType specifies the content type of the message.
	ContentType string `default:"application/json" json:"contentType"`

	// Confirm enables publisher confirmation mode for this message.
	// Only works if publisher confirms are enabled at the channel level.
	Confirm bool `default:"false" json:"confirm"`

	// ConfirmCh is the channel where confirmation will be sent after publishing.
	// This channel must be initialized by the caller if Confirm is true.
	// The confirmation will contain:
	// - Ack: true if message was confirmed, false if nacked
	// - DeliveryTag: the sequence number of this delivery
	ConfirmCh chan amqp.Confirmation `json:"-"`

	// DeliveryMode. Transient means higher throughput but messages will not be
	// restored on broker restart. The delivery mode of publishings is unrelated
	// to the durability of the queues they reside on. Transient messages will
	// not be restored to durable queues, persistent messages will be restored
	// to durable queues and lost on non-durable queues during server restart.
	// This remains typed as uint8 to match Publishing.DeliveryMode. Other
	// delivery modes specific to custom queue implementations are not
	// enumerated here.
	//
	// - Transient  uint8 = 1
	// - Persistent uint8 = 2
	DeliveryMode uint8 `default:"2" json:"deliveryMode"`

	// Exchange specifies the exchange to publish to.
	Exchange string `json:"exchange"`

	// Immediate delivers the message to the first available consumer immediately.
	Immediate bool `default:"false" json:"immediate"`

	// Mandatory ensures the message is delivered to at least one consumer.
	Mandatory bool `default:"false" json:"mandatory"`
}

// NewPublishParams returns a default PublishParams.
func NewPublishParams() *PublishParams {
	return &PublishParams{
		Confirm:      false,
		ContentType:  "application/json",
		DeliveryMode: 2,
		Exchange:     "",
		Immediate:    false,
		Mandatory:    false,
	}
}
