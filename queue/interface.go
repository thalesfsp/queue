package queue

import (
	"context"
	"expvar"

	"github.com/thalesfsp/sypl"
)

//////
// Const, vars, and types.
//////

// CallbackFunc is the function that will be once a message is received on a
// subscribed channel.
type CallbackFunc func(ctx context.Context, msg *Message) error

// IQueue defines the queue abstraction layer - interface.
type IQueue interface {
	// Publish data.
	//
	// NOTE: Use `prm.Any` to set implementation-specific parameters.
	Publish(ctx context.Context, queueName string, msg *Message, prm *PublishParams, options ...OptionsFunc) error

	// Subscribe to channel.
	//
	// NOTE: Use `prm.Any` to set implementation-specific parameters.
	Subscribe(ctx context.Context, queueName string, cb CallbackFunc, prm *SubscribeParams, options ...OptionsFunc) error

	//////
	// Meta.
	//////

	// GetType returns its type.
	GetType() string

	// GetClient returns the queue client. Use that to interact with the
	// underlying queue client.
	GetClient() any

	// GetLogger returns the logger.
	GetLogger() sypl.ISypl

	// GetName returns the queue name.
	GetName() string

	//////
	// Metrics.
	//////

	// GetCounterPublished returns the metric.
	GetCounterPublished() *expvar.Int

	// GetCounterPublishedFailed returns the metric.
	GetCounterPublishedFailed() *expvar.Int

	// GetCounterReceived returns the metric.
	GetCounterReceived() *expvar.Int

	// GetCounterReceivedFailed returns the metric.
	GetCounterReceivedFailed() *expvar.Int

	// GetCounterSubscribed returns the metric.
	GetCounterSubscribed() *expvar.Int

	// GetCounterSubscribedFailed returns the metric.
	GetCounterSubscribedFailed() *expvar.Int
}

//////
// Generic functions.
//////

// Publish data.
//
// NOTE: Use `prm.Any` to set implementation-specific parameters.
func Publish(ctx context.Context, s IQueue, queueName string, msg *Message, prm *PublishParams, options ...OptionsFunc) error {
	return s.Publish(ctx, queueName, msg, prm, options...)
}
