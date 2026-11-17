package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/thalesfsp/customerror"
	"github.com/thalesfsp/queue/internal/customapm"
	"github.com/thalesfsp/queue/internal/logging"
	"github.com/thalesfsp/queue/queue"
	"github.com/thalesfsp/status"
	"github.com/thalesfsp/sypl/v2"
	"github.com/thalesfsp/sypl/v2/fields"
	"github.com/thalesfsp/sypl/v2/level"
	"github.com/thalesfsp/validation"
	"go.elastic.co/apm"
)

//////
// Const, vars, and types.
//////

// Name of the queue.
const Name = "rabbitmq"

// subscriptionClient is the AMQP surface used to register a consumer.
type subscriptionClient interface {
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
}

// Singleton.
var singleton queue.IQueue

// Config is the RabbitMQ configuration.
type Config struct {
	EnableConfirms bool `json:"enableConfirms"`
	Global         bool `json:"global"`
	PrefetchCount  int  `json:"prefetchCount"`
	PrefetchSize   int  `json:"prefetchSize"`
}

// NewConfig returns a default RabbitMQ configuration.
func NewConfig() *Config {
	return &Config{
		EnableConfirms: false,
		Global:         false,
		PrefetchCount:  1,
		PrefetchSize:   0,
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

	// A nil subscriber uses Client. Tests supply a hermetic AMQP implementation.
	subscriber subscriptionClient
}

//////
// Helpers.
//////

// handleDelivery owns its tracing lifetime independently of Subscribe and its caller.
func (m *RabbitMQ) handleDelivery(ctx context.Context, d amqp.Delivery, cb queue.CallbackFunc, timeout time.Duration) {
	tx := apm.DefaultTracer.StartTransaction(Name, queue.Type)
	defer tx.End()

	// Replace inherited tracing values without detaching cancellation or other values.
	ctx = apm.ContextWithSpan(apm.ContextWithTransaction(ctx, tx), nil)
	ctx, span := customapm.Trace(ctx, m.GetType(), Name, status.Received.String())
	defer span.End()

	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var message queue.Message
	message.Body = d.Body

	if err := cb(ctxTimeout, &message); err != nil {
		if nackErr := d.Nack(false, true); nackErr != nil {
			_ = customapm.TraceError(ctx,
				customerror.NewFailedToError(queue.OperationReceived.String(), customerror.WithError(nackErr)),
				m.GetLogger(), m.GetCounterReceivedFailed())
		}

		_ = customapm.TraceError(ctx,
			customerror.NewFailedToError(queue.OperationReceived.String(), customerror.WithError(err)),
			m.GetLogger(), m.GetCounterReceivedFailed())

		return
	}

	m.GetCounterReceived().Add(1)

	if err := d.Ack(false); err != nil {
		_ = customapm.TraceError(ctx,
			customerror.NewFailedToError(queue.OperationReceived.String(), customerror.WithError(err)),
			m.GetLogger(), m.GetCounterReceivedFailed())
	}
}

//////
// Implements the IQueue interface.
//////

// Subscribe to channel.
//
// NOTE: Use `prm.Any` to set implementation-specific parameters.
func (m *RabbitMQ) Subscribe(ctx context.Context, queueName string, cb queue.CallbackFunc, prm *queue.SubscribeParams, options ...queue.OptionsFunc) error {
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

	consumerCtx := ctx

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

	o, err := queue.NewOptions()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterSubscribedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterSubscribedFailed())
		}
	}

	if o.QueueName != "" {
		queueName = o.QueueName
	}

	//////
	// Params handling.
	//////

	// Handle cases where prm is not set.
	if prm == nil {
		prm = NewSubscribeParams()
	}

	// Handle cases where Any isn't set.
	if prm.Any == nil {
		prm.Any = NewDefaultSubscribeParams()
	}

	// At this point, prm.Any is something. If `Any` was empty, for sure it's
	// a `PublishParams`, otherwise tries to cast to it, if fails, returns an
	// error.
	castedParams, ok := prm.Any.(*SubscribeParams)
	if !ok {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("cast to PublishParams"),
			m.GetLogger(),
			m.GetCounterPublishedFailed(),
		)
	}

	//////
	// Subscribing.
	//////

	client := m.subscriber
	if client == nil {
		client = m.Client
	}

	if _, err := client.QueueDeclare(
		queueName,
		castedParams.Durable,
		castedParams.AutoDelete,
		castedParams.Exclusive,
		castedParams.NoWait,
		castedParams.Args,
	); err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(
				"declare queue",
				customerror.WithError(err),
			),
			m.GetLogger(),
			m.GetCounterSubscribedFailed(),
		)
	}

	msgs, err := client.Consume(
		queueName,
		castedParams.Consumer,
		castedParams.AutoAck,
		castedParams.Exclusive,
		castedParams.NoLocal,
		castedParams.NoWait,
		castedParams.Args,
	)
	if err != nil {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError(queue.OperationSubscribed.String(), customerror.WithError(err)),
			m.GetLogger(),
			m.GetCounterSubscribedFailed(),
		)
	}

	timeout := subscribeTimeout(prm, castedParams)

	go func() {
		for d := range msgs {
			m.handleDelivery(consumerCtx, d, cb, timeout)
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
//
// NOTE: Use `prm.Any` to set implementation-specific parameters.
func (m *RabbitMQ) Publish(ctx context.Context, queueName string, msg *queue.Message, prm *queue.PublishParams, options ...queue.OptionsFunc) error {
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

	o, err := queue.NewOptions()
	if err != nil {
		return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterPublishedFailed())
	}

	// Iterate over the options and apply them against params.
	for _, option := range options {
		if err := option(o); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterPublishedFailed())
		}
	}

	if o.QueueName != "" {
		queueName = o.QueueName
	}

	//////
	// Params handling.
	//////

	// Handle cases where prm is not set.
	if prm == nil {
		prm = NewPublishParams()
	}

	// Handle cases where Any isn't set.
	if prm.Any == nil {
		prm.Any = NewDefaultPublishParams()
	}

	// At this point, prm.Any is something. If `Any` was empty, for sure it's
	// a `PublishParams`, otherwise tries to cast to it, if fails, returns an
	// error.
	castedParams, ok := prm.Any.(*PublishParams)
	if !ok {
		return customapm.TraceError(
			ctx,
			customerror.NewFailedToError("cast to PublishParams"),
			m.GetLogger(),
			m.GetCounterPublishedFailed(),
		)
	}

	//////
	// Publish.
	//////

	if o.PreHookFunc != nil {
		if err := o.PreHookFunc(ctx, m, queueName, msg); err != nil {
			return customapm.TraceError(ctx, err, m.GetLogger(), m.GetCounterPublishedFailed())
		}
	}

	// Register confirmation channel if provided and confirms are enabled
	if m.Config.EnableConfirms && castedParams.Confirm && castedParams.ConfirmCh != nil {
		m.Client.NotifyPublish(castedParams.ConfirmCh)
	}

	// Actually publish the data.
	var publishing amqp.Publishing
	publishing.DeliveryMode = castedParams.DeliveryMode
	publishing.ContentType = castedParams.ContentType
	publishing.Body = msg.Body

	if err := m.Client.PublishWithContext(ctx,
		castedParams.Exchange,
		queueName,
		castedParams.Mandatory,
		castedParams.Immediate,
		publishing); err != nil {
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
func New(ctx context.Context, url string, cfg *Config) (*RabbitMQ, error) {
	// Enforces IQueue interface implementation.
	var _ queue.IQueue = (*RabbitMQ)(nil)

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

	if err := ch.Qos(
		cfg.PrefetchCount,
		cfg.PrefetchSize,
		false,
	); err != nil {
		return nil, customerror.NewFailedToError("setup QoS", customerror.WithError(err))
	}

	// Call ping to RabbitMQ.
	if err := ch.Confirm(false); err != nil {
		return nil, customerror.NewFailedToError("ping", customerror.WithError(err))
	}

	queue := &RabbitMQ{
		Queue: iQ,

		Client: ch,
		Config: cfg,

		amqpConn:   conn,
		subscriber: nil,
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
func Get() queue.IQueue {
	if singleton == nil {
		panic(fmt.Sprintf("%s %s not %s", Name, queue.Type, status.Initialized))
	}

	return singleton
}

// Set the Queue, primarily used for testing.
func Set(s queue.IQueue) {
	singleton = s
}
