package queue

import (
	"context"
	"expvar"

	"github.com/thalesfsp/sypl"
)

//////
// Creates the a struct which satisfies the queue.IQueue interface.
//////

// Mock is a struct which satisfies the queue.IQueue interface.
//
//nolint:dupl
type Mock[P, S any] struct {
	//////
	// Allows to set the returned value of each method.
	//////

	// Publish data.
	MockPublish func(ctx context.Context, queueName string, msg *Message, prm *P, options ...OptionsFunc[P, S]) error

	// Subscribe to channel.
	MockSubscribe func(ctx context.Context, queueName string, cb CallbackFunc, prm *S, options ...OptionsFunc[P, S]) error

	// GetType returns its type.
	MockGetType func() string

	// GetClient returns the queue client. Use that to interact with the underlying queue client.
	MockGetClient func() any

	// GetLogger returns the logger.
	MockGetLogger func() sypl.ISypl

	// GetName returns the queue name.
	MockGetName func() string

	// GetCounterPublished returns the metric.
	MockGetCounterPublished func() *expvar.Int

	// GetCounterPublishedFailed returns the metric.
	MockGetCounterPublishedFailed func() *expvar.Int

	// GetCounterReceived returns the metric.
	MockGetCounterReceived func() *expvar.Int

	// GetCounterReceivedFailed returns the metric.
	MockGetCounterReceivedFailed func() *expvar.Int

	// GetCounterSubscribed returns the metric.
	MockGetCounterSubscribed func() *expvar.Int

	// GetCounterSubscribedFailed returns the metric.
	MockGetCounterSubscribedFailed func() *expvar.Int
}

//////
// When the methods are called, it will call the corresponding method in the
// Mock struct returning the desired value. This implements the IQueue
// interface.
//////

// Subscribe to channel.
func (m *Mock[PublishParams, SubscribeParams]) Subscribe(ctx context.Context, queueName string, cb CallbackFunc, prm *SubscribeParams, options ...OptionsFunc[PublishParams, SubscribeParams]) error {
	return m.MockSubscribe(ctx, queueName, cb, prm, options...)
}

// Publish data.
func (m *Mock[PublishParams, SubscribeParams]) Publish(ctx context.Context, queueName string, msg *Message, prm *PublishParams, options ...OptionsFunc[PublishParams, SubscribeParams]) error {
	return m.MockPublish(ctx, queueName, msg, prm, options...)
}

// GetType returns its type.
func (m *Mock[PublishParams, SubscribeParams]) GetType() string {
	return m.MockGetType()
}

// GetClient returns the queue client. Use that to interact with the underlying queue client.
func (m *Mock[PublishParams, SubscribeParams]) GetClient() any {
	return m.MockGetClient()
}

// GetLogger returns the logger.
func (m *Mock[PublishParams, SubscribeParams]) GetLogger() sypl.ISypl {
	return m.MockGetLogger()
}

// GetName returns the queue name.
func (m *Mock[PublishParams, SubscribeParams]) GetName() string {
	return m.MockGetName()
}

// GetCounterPublished returns the metric.
func (m *Mock[PublishParams, SubscribeParams]) GetCounterPublished() *expvar.Int {
	return m.MockGetCounterPublished()
}

// GetCounterPublishedFailed returns the metric.
func (m *Mock[PublishParams, SubscribeParams]) GetCounterPublishedFailed() *expvar.Int {
	return m.MockGetCounterPublishedFailed()
}

// GetCounterReceived returns the metric.
func (m *Mock[PublishParams, SubscribeParams]) GetCounterReceived() *expvar.Int {
	return m.MockGetCounterReceived()
}

// GetCounterReceivedFailed returns the metric.
func (m *Mock[PublishParams, SubscribeParams]) GetCounterReceivedFailed() *expvar.Int {
	return m.MockGetCounterReceivedFailed()
}

// GetCounterSubscribed returns the metric.
func (m *Mock[PublishParams, SubscribeParams]) GetCounterSubscribed() *expvar.Int {
	return m.MockGetCounterSubscribed()
}

// GetCounterSubscribedFailed returns the metric.
func (m *Mock[PublishParams, SubscribeParams]) GetCounterSubscribedFailed() *expvar.Int {
	return m.MockGetCounterSubscribedFailed()
}
