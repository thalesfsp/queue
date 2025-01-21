package rabbitmq

import (
	"context"
	"fmt"

	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/queue/internal/customapm"
	"github.com/thalesfsp/queue/internal/logging"
	"github.com/thalesfsp/queue/queue"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl"
	"github.com/thalesfsp/sypl/fields"
	"github.com/thalesfsp/sypl/level"
	"github.com/thalesfsp/validation"

	amqp "github.com/rabbitmq/amqp091-go"
)

//////
// Const, vars, and types.
//////

// Name of the queue.
const Name = "rabbitmq"

// Singleton.
var singleton queue.IQueue[PublishParams, SubscribeParams]

// Config is the RabbitMQ configuration.
type Config struct {
	Args          amqp.Table
	AutoDelete    bool
	Durable       bool
	Exclusive     bool
	Global        bool
	NoWait        bool
	PrefetchCount int
	PrefetchSize  int
}

// NewConfig returns a default RabbitMQ configuration.
func NewConfig() *Config {
	return &Config{
		Args:          nil,
		AutoDelete:    false,
		Durable:       true,
		Exclusive:     false,
		Global:        false,
		NoWait:        false,
		PrefetchCount: 1,
		PrefetchSize:  0,
	}
}

// RabbitMQ queue definition.
type RabbitMQ struct {
	*queue.Queue

	// Client is the RabbitMQ client.
	Client *amqp.Channel `json:"-" validate:"required"`

	// Config is the RabbitMQ configuration.
	Config *Config `json:"-" validate:"required"`

	amqpConn *amqp.Connection

	amqpQueue amqp.Queue
}

//////
// Helpers.
//////

//////
// Implements the IQueue interface.
//////

// Subscribe to channel.
func (m *RabbitMQ) Subscribe(ctx context.Context, queueName string, cb queue.CallbackFunc, prm *SubscribeParams, options ...queue.OptionsFunc[PublishParams, SubscribeParams]) error {
	//////
	// Validation.
	//////

	if queueName == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("queueName"),
			m.GetLogger(),
			m.GetCounterSubscribedFailed(),
		)
	}

	if cb == nil {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("callback"),
			m.GetLogger(),
			m.GetCounterSubscribedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Subscribed.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := queue.NewOptions[PublishParams, SubscribeParams]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterSubscribedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterSubscribedFailed())
		}
	}

	// Queue name was defined when the `New` was called. RabbitMQ doesn't allow
	// to change that later.
	o.QueueName = m.amqpQueue.Name

	queueName = o.QueueName

	//////
	// Params handling.
	//////

	if prm == nil {
		prm = NewSubscribeParams()
	}

	//////
	// Subscribing.
	//////

	msgs, err := m.Client.Consume(
		queueName,
		prm.Consumer,
		prm.AutoAck,
		prm.Exclusive,
		prm.NoLocal,
		prm.NoWait,
		prm.Args,
	)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(queue.OperationSubscribed.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterSubscribedFailed(),
		)
	}

	go func() {
		for d := range msgs {
			ctxTimeout, cancel := context.WithTimeout(ctx, prm.ContextTimeout)
			defer cancel()

			if err := cb(ctxTimeout, &queue.Message{
				Body: d.Body,
			}); err != nil {
				// Doesn't acknowledge the message.
				d.Nack(false, true)

				// Observability.
				customapm.TraceError(
					ctx,
					customerror.NewFailedToError(queue.OperationReceived.String(), customerror.WithError(err)),
					m.GetLogger(),
					m.GetCounterReceivedFailed(),
				)

				// Do nothing, just continue to the next message.
				continue
			}

			// Observability.
			m.GetCounterReceived().Add(1)

			// Acknowledges the message.
			d.Ack(false)
		}
	}()

	//////
	// Observability.
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Subscribed.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	m.GetCounterSubscribed().Add(1)

	return nil
}

// Publish data.
func (m *RabbitMQ) Publish(ctx context.Context, queueName string, msg *queue.Message, prm *PublishParams, options ...queue.OptionsFunc[PublishParams, SubscribeParams]) error {
	//////
	// Validation.
	//////

	if queueName == "" {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("queueName"),
			m.GetLogger(),
			m.GetCounterPublishedFailed(),
		)
	}

	if msg == nil {
		return customapm.TraceError(
			ctx,
			customerror.NewRequiredError("data"),
			m.GetLogger(),
			m.GetCounterPublishedFailed(),
		)
	}

	//////
	// APM Tracing.
	//////

	ctx, span := customapm.Trace(
		ctx,
		m.GetType(),
		Name,
		status.Published.String(),
	)
	defer span.End()

	//////
	// Options initialization.
	//////

	o, err := queue.NewOptions[PublishParams, SubscribeParams]()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterPublishedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterPublishedFailed())
		}
	}

	// In the context of RabbitMQ, the queue name was already defined when the
	// `New` was called.
	o.QueueName = m.amqpQueue.Name

	queueName = o.QueueName

	//////
	// Params handling.
	//////

	if prm == nil {
		prm = NewPublishParams()
	}

	//////
	// Publish.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, queueName, msg); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterPublishedFailed())
		}
	}

	// Actually publish the data.
	if err := m.Client.PublishWithContext(ctx,
		prm.Exchange,
		queueName,
		prm.Mandatory,
		prm.Immediate,
		amqp.Publishing{
			DeliveryMode: prm.DeliveryMode,
			ContentType:  prm.ContentType,
			Body:         msg.Body,
		}); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(queue.OperationPublished.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterSubscribedFailed(),
		)
	}

	if o.PostHookFunc != nil {
		if err := o.PostHookFunc(ctx, m, queueName, msg); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterPublishedFailed())
		}
	}

	//////
	// Observability
	//////

	// Correlates the transaction, span and log, and logs it.
	m.GetLogger().PrintlnWithOptions(
		level.Debug,
		status.Published.String(),
		sypl.WithFields(logging.ToAPM(ctx, make(fields.Fields))),
	)

	//////
	// Metrics.
	//////

	m.GetCounterPublished().Add(1)

	return nil
}

// GetClient returns the client.
func (m *RabbitMQ) GetClient() any {
	return m.Client
}

//////
// Factory.
//////

// New creates a new RabbitMQ Queue.
func New(ctx context.Context, url string, queueName string, cfg *Config) (*RabbitMQ, error) {
	// Enforces IQueue interface implementation.
	var _ queue.IQueue[PublishParams, SubscribeParams] = (*RabbitMQ)(nil)

	iQ, err := queue.New(ctx, Name)
	if err != nil {
		return nil, err
	}

	//////
	// Pre-validation.
	//////

	if url == "" {
		return nil, customerror.NewRequiredError("url")
	}

	if queueName == "" {
		return nil, customerror.NewRequiredError("queueName")
	}

	if cfg == nil {
		// Sets the default configuration.
		cfg = NewConfig()
	}

	//////
	// Client setup starts here.
	//////

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, customerror.NewFailedToError("dial", customerror.WithError(err))
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, customerror.NewFailedToError("open channel", customerror.WithError(err))
	}

	q, err := ch.QueueDeclare(
		queueName,
		cfg.Durable,
		cfg.AutoDelete,
		cfg.Exclusive,
		cfg.NoWait,
		cfg.Args,
	)
	if err != nil {
		return nil, customerror.NewFailedToError("connect to queue", customerror.WithError(err))
	}

	if err := ch.Qos(
		cfg.PrefetchCount,
		cfg.PrefetchSize,
		false,
	); err != nil {
		return nil, customerror.NewFailedToError("setup QoS", customerror.WithError(err))
	}

	queue := &RabbitMQ{
		Queue: iQ,

		Client: ch,
		Config: cfg,

		amqpQueue: q,
		amqpConn:  conn,
	}

	//////
	// Queue validation.
	//////

	if err := validation.Validate(queue); err != nil {
		return nil, err
	}

	// Singleton setup.
	singleton = queue

	return queue, nil
}

//////
// Exported functionalities.
//////

// Get returns a setup Queue, or set it up.
func Get() queue.IQueue[PublishParams, SubscribeParams] {
	if singleton == nil {
		panic(fmt.Sprintf("%s %s not %s", Name, queue.Type, status.Initialized))
	}

	return singleton
}

// Set the Queue, primarily used for testing.
func Set(s queue.IQueue[PublishParams, SubscribeParams]) {
	singleton = s
}
