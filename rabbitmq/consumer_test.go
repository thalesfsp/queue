package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/thalesfsp/queue/queue"
	"go.elastic.co/apm"
	"go.elastic.co/apm/apmtest"
)

// The real Subscribe path registers a consumer on this in-memory AMQP boundary.
type consumerClient struct {
	deliveries chan amqp.Delivery
	declared   string
	consumed   string
}

func (c *consumerClient) QueueDeclare(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	c.declared = name
	var q amqp.Queue
	q.Name = name
	return q, nil
}

func (c *consumerClient) Consume(name, _ string, _, _, _, _ bool, _ amqp.Table) (<-chan amqp.Delivery, error) {
	c.consumed = name
	return c.deliveries, nil
}

type acknowledger struct {
	ackCalls  atomic.Int32
	nackCalls atomic.Int32
	requeue   atomic.Bool
	multiple  atomic.Bool
	ackErr    error
	nackErr   error
}

func (a *acknowledger) Ack(_ uint64, multiple bool) error {
	a.multiple.Store(multiple)
	a.ackCalls.Add(1)
	return a.ackErr
}

func (a *acknowledger) Nack(_ uint64, multiple, requeue bool) error {
	a.multiple.Store(multiple)
	a.requeue.Store(requeue)
	a.nackCalls.Add(1)
	return a.nackErr
}

func (a *acknowledger) Reject(_ uint64, _ bool) error {
	return errors.New("unexpected reject")
}

var consumerSequence atomic.Int64

func newConsumer(t *testing.T) (*RabbitMQ, *consumerClient, *apmtest.RecordingTracer) {
	t.Helper()
	tracer := apmtest.NewRecordingTracer()
	previous := apm.DefaultTracer
	apm.DefaultTracer = tracer.Tracer
	t.Cleanup(func() { apm.DefaultTracer = previous; tracer.Close() })
	base, err := queue.New(t.Context(), fmt.Sprintf("consumer-%d", consumerSequence.Add(1)))
	require.NoError(t, err)
	client := new(consumerClient)
	client.deliveries = make(chan amqp.Delivery, 2)
	q := new(RabbitMQ)
	q.Queue = base
	q.subscriber = client
	return q, client, tracer
}

func receivedSpanCount(tracer *apmtest.RecordingTracer) int {
	tracer.Flush(nil)
	count := 0
	for _, span := range tracer.Payloads().Spans {
		if span.Name == "rabbitmq.received" {
			count++
		}
	}
	return count
}

func TestSubscribeDeliveryLifetime(t *testing.T) {
	for _, scenario := range []string{"success", "callback error", "ack error", "nack error", "inactive APM", "cancelled caller", "no parent trace", "ended delivery span"} {
		t.Run(scenario, func(t *testing.T) {
			if scenario == "inactive APM" {
				t.Setenv("ELASTIC_APM_ACTIVE", "false")
			} else {
				t.Setenv("ELASTIC_APM_ACTIVE", "true")
			}
			t.Setenv("ELASTIC_APM_TRANSACTION_SAMPLE_RATE", "1")
			q, client, tracer := newConsumer(t)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type marker struct{}
			ctx = context.WithValue(ctx, marker{}, "retained")
			var parent *apm.Span
			if scenario != "no parent trace" {
				tx := tracer.StartTransaction("caller", "test")
				ctx = apm.ContextWithTransaction(ctx, tx)
				parent, ctx = apm.StartSpan(ctx, "caller", "test")
				parent.End()
				tx.End()
			}
			if scenario == "cancelled caller" {
				cancel()
			}

			ack := new(acknowledger)
			if scenario == "ack error" {
				ack.ackErr = errors.New("ack failed")
			}
			if scenario == "nack error" {
				ack.nackErr = errors.New("nack failed")
			}

			handlerFails := scenario == "callback error" || scenario == "nack error" || scenario == "inactive APM" || scenario == "ended delivery span"
			var calls atomic.Int32
			callbackErrors := make(chan string, 16)
			recordFailure := func(ok bool, message string) {
				if !ok {
					callbackErrors <- message
				}
			}
			err := q.Subscribe(ctx, "jobs", func(ctx context.Context, msg *queue.Message) error {
				calls.Add(1)
				recordFailure(string(msg.Body) == "job", "payload changed")
				recordFailure(ctx.Value(marker{}) == "retained", "caller values lost")
				if scenario == "cancelled caller" {
					recordFailure(errors.Is(ctx.Err(), context.Canceled), "caller cancellation lost")
				} else {
					recordFailure(ctx.Err() == nil, "callback context already expired")
				}
				if scenario != "inactive APM" {
					span := apm.SpanFromContext(ctx)
					recordFailure(span != nil, "missing per-delivery span")
					if span != nil {
						recordFailure(span != parent, "ended caller span inherited")
						recordFailure(span.SpanData != nil, "delivery span ended before callback")
					}
				}
				if scenario == "ended delivery span" {
					apm.SpanFromContext(ctx).End()
				}
				if handlerFails {
					return errors.New("handler failed")
				}
				return nil
			}, nil)
			require.NoError(t, err)
			require.Equal(t, "jobs", client.declared)
			require.Equal(t, "jobs", client.consumed)
			// Deliver only after Subscribe has returned and ended its own span.
			for range 2 {
				var delivery amqp.Delivery
				delivery.Acknowledger = ack
				delivery.DeliveryTag = 1
				delivery.Body = []byte("job")
				client.deliveries <- delivery
			}
			close(client.deliveries)
			require.Eventually(t, func() bool {
				return ack.ackCalls.Load()+ack.nackCalls.Load() == 2
			}, 3*time.Second, time.Millisecond)
			require.Equal(t, int32(2), calls.Load())
			var failures []string
			for len(callbackErrors) > 0 {
				failures = append(failures, <-callbackErrors)
			}
			require.Empty(t, failures)
			if scenario != "inactive APM" {
				require.Eventually(t, func() bool { return receivedSpanCount(tracer) == 2 }, 3*time.Second, time.Millisecond)
			}
			require.False(t, ack.multiple.Load())
			if handlerFails {
				require.Equal(t, int32(2), ack.nackCalls.Load())
				require.True(t, ack.requeue.Load())
			} else {
				require.Equal(t, int32(2), ack.ackCalls.Load())
			}
			wantFailed := int64(0)
			if handlerFails || scenario == "ack error" {
				wantFailed = 2
			}
			if scenario == "nack error" {
				wantFailed = 4
			}
			require.Eventually(t, func() bool { return q.GetCounterReceivedFailed().Value() == wantFailed }, time.Second, time.Millisecond)
		})
	}
}

func TestNewSubscribeParamsTimeout(t *testing.T) {
	prm := NewSubscribeParams()
	require.Equal(t, 5*time.Second, prm.ContextTimeout)
}

func TestSubscribeTimeout(t *testing.T) {
	for _, scenario := range []struct {
		name               string
		outer, inner, want time.Duration
	}{
		{"defaults", 0, 0, 5 * time.Second},
		{"outer explicit", time.Second, 2 * time.Second, time.Second},
		{"inner explicit", 0, 2 * time.Second, 2 * time.Second},
		{"negative outer", -time.Second, 2 * time.Second, 2 * time.Second},
		{"negative both", -time.Second, -time.Second, 5 * time.Second},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			outer := new(queue.SubscribeParams)
			outer.ContextTimeout = scenario.outer
			inner := new(SubscribeParams)
			inner.ContextTimeout = scenario.inner
			timeout := subscribeTimeout(outer, inner)
			require.Equal(t, scenario.want, timeout)
			ctx, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			require.NoError(t, ctx.Err())
		})
	}
	require.Equal(t, 5*time.Second, subscribeTimeout(nil, nil))
}

func TestSubscribeTimeoutWiring(t *testing.T) {
	t.Setenv("ELASTIC_APM_ACTIVE", "true")
	t.Setenv("ELASTIC_APM_TRANSACTION_SAMPLE_RATE", "1")
	for _, scenario := range []string{"nil", "helper", "empty", "outer", "inner", "outer override", "negative"} {
		t.Run(scenario, func(t *testing.T) {
			q, client, tracer := newConsumer(t)
			var prm *queue.SubscribeParams
			want := 5 * time.Second
			if scenario == "helper" {
				prm = NewSubscribeParams()
			}
			if scenario != "nil" && scenario != "helper" {
				prm = new(queue.SubscribeParams)
				specific := new(SubscribeParams)
				prm.Any = specific
				switch scenario {
				case "outer":
					prm.ContextTimeout = 2 * time.Second
					want = 2 * time.Second
				case "inner":
					specific.ContextTimeout = 4 * time.Second
					want = 4 * time.Second
				case "outer override":
					prm.ContextTimeout = 2 * time.Second
					specific.ContextTimeout = 4 * time.Second
					want = 2 * time.Second
				case "negative":
					prm.ContextTimeout = -time.Second
					specific.ContextTimeout = -time.Second
				}
			}
			type observation struct {
				err       error
				remaining time.Duration
			}
			observed := make(chan observation, 1)
			require.NoError(t, q.Subscribe(t.Context(), "jobs", func(ctx context.Context, _ *queue.Message) error {
				deadline, _ := ctx.Deadline()
				observed <- observation{err: ctx.Err(), remaining: time.Until(deadline)}
				return nil
			}, prm))
			var delivery amqp.Delivery
			delivery.Acknowledger = new(acknowledger)
			client.deliveries <- delivery
			close(client.deliveries)
			select {
			case actual := <-observed:
				require.NoError(t, actual.err)
				require.Greater(t, actual.remaining, want/2)
				require.LessOrEqual(t, actual.remaining, want)
			case <-time.After(3 * time.Second):
				t.Fatal("callback was not reached")
			}
			require.Eventually(t, func() bool { return receivedSpanCount(tracer) == 1 }, 3*time.Second, time.Millisecond)
		})
	}
}

func TestPublishParamsDefaults(t *testing.T) {
	specific := NewDefaultPublishParams()
	common := NewPublishParams()
	require.Equal(t, specific, common.Any)
	require.Equal(t, "application/json", specific.ContentType)
	require.Equal(t, uint8(2), specific.DeliveryMode)
	require.Empty(t, specific.Exchange)
	require.False(t, specific.Confirm)
	require.Nil(t, specific.ConfirmCh)
	require.False(t, specific.Immediate)
	require.False(t, specific.Mandatory)
}

func TestSubscribeParamsDefaults(t *testing.T) {
	specific := NewDefaultSubscribeParams()
	require.Equal(t, 5*time.Second, specific.ContextTimeout)
	require.True(t, specific.Durable)
	require.False(t, specific.AutoAck)
	require.False(t, specific.AutoDelete)
	require.False(t, specific.Exclusive)
	require.False(t, specific.NoLocal)
	require.False(t, specific.NoWait)
	require.Empty(t, specific.Consumer)
	require.Nil(t, specific.Args)
	require.Equal(t, specific, NewSubscribeParams().Any)
}
