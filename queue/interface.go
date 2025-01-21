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
type IQueue[P, S any] interface {
	// Publish data.
	Publish(ctx context.Context, queueName string, msg *Message, prm *P, options ...OptionsFunc[P, S]) error

	// Subscribe to channel.
	Subscribe(ctx context.Context, queueName string, cb CallbackFunc, prm *S, options ...OptionsFunc[P, S]) error

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
func Publish[P any, S any](ctx context.Context, s IQueue[P, S], queueName string, msg *Message, prm *P, options ...OptionsFunc[P, S]) error {
	return s.Publish(ctx, queueName, msg, prm, options...)
}

// Subscribe data.
func Subscribe[P any, S any](ctx context.Context, s IQueue[P, S], queueName string, cb CallbackFunc, prm *S, options ...OptionsFunc[P, S]) error {
	return Subscribe(ctx, s, queueName, cb, prm, options...)
}
