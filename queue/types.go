package queue

import (
	"time"

	"github.com/thalesfsp/queue/internal/shared"
)

//////
// Const, vars, and types.
//////

// SubscribeParams defines the parameters for subscribing to a queue.
type SubscribeParams struct {
	// GroupID identifies a group of consumers that work as one unit.
	// Example: Consumer Group in Kafka/SQS, Consumer Tag prefix in RabbitMQ, etc.
	GroupID string

	// Tags filter which messages this subscriber receives.
	// Example: Message Filtering in SQS, Binding Keys in RabbitMQ, Topic Subscriptions in Kafka, etc.
	Tags []string

	// Route specifies which route/topic to subscribe to.
	// Example: Binding Pattern in RabbitMQ, Topic in Kafka, unused in standard SQS, etc.
	Route string

	// BatchSize defines how many messages to process in a single batch.
	// This helps optimize throughput vs. latency across all queue systems.
	BatchSize int

	// MaxMessages sets the maximum number of messages to receive in total.
	// Useful for controlling message flow and preventing overwhelming consumers.
	MaxMessages int

	// WaitTimeout specifies how long to wait for messages when none are immediately available.
	// Example: Long Polling in SQS, Consumer Timeout in Kafka, etc.
	WaitTimeout time.Duration

	// Metadata holds queue-specific attributes that don't fit into standard fields.
	// Example: Message Attributes in SQS, Headers in RabbitMQ/Kafka, etc.
	Metadata map[string]interface{}

	// ContextTimeout specifies how long to wait for the callback to finish.
	ContextTimeout time.Duration
}

// PublishParams defines the parameters for publishing a message to a queue.
type PublishParams struct {
	// Tags attach categorization metadata to the message.
	// Example: Message Tags in SQS, Message Properties in RabbitMQ, Record Headers in Kafka, etc.
	Tags []string

	// Route specifies where to publish the message.
	// Example: Routing Key in RabbitMQ, Topic in Kafka, unused in standard SQS, etc.
	Route string

	// DelaySeconds postpones message delivery by specified seconds.
	// Example: Message Timer in SQS, Delayed Exchange in RabbitMQ, etc.
	DelaySeconds int

	// MessageGroupID ensures ordering for related messages.
	// Example: Group ID in FIFO queues (SQS), Partition Key in Kafka, etc.
	MessageGroupID string

	// Priority influences message delivery order where supported.
	// Example: Message Priority in RabbitMQ, custom implementation in other queues, etc.
	Priority int

	// Metadata holds queue-specific attributes for publishing.
	// Example: System Attributes in SQS, Headers in RabbitMQ/Kafka, etc.
	Metadata map[string]interface{}
}

// Message represents a generic queue message
type Message struct {
	// Body is the raw message payload in bytes.
	Body []byte

	// MessageID uniquely identifies the message across a queue system.
	// Example: Message ID in SQS, Delivery Tag in RabbitMQ, Offset in Kafka, etc.
	MessageID string

	// Metadata holds specific attributes that don't fit into standard fields.
	Metadata map[string]interface{}

	// Timestamp indicates when the message was published to the queue.
	Timestamp time.Time
}

// NewMessage returns a new Message with a unique ID and current timestamp.
func NewMessage(body []byte) *Message {
	return &Message{
		Body:      body,
		MessageID: shared.GenerateUUID(),
		Timestamp: time.Now(),
	}
}

// NewMessageFromStruct creates a new Message from a struct.
func NewMessageFromStruct(body any) (*Message, error) {
	b, err := shared.Marshal(body)
	if err != nil {
		return nil, err
	}

	return NewMessage(b), nil
}

// NewMustMessageFromStruct creates a new Message from a struct, panicking on
// error.
func NewMustMessageFromStruct(body any) *Message {
	m, err := NewMessageFromStruct(body)
	if err != nil {
		panic(err)
	}

	return m
}
