// "amqp://guest:guest@localhost:5672/"
// Config is the RabbitMQ configuration.
// "task_queue", // name
// true,         // durable
// false,        // delete when unused
// false,        // exclusive
// false,        // no-wait
// nil,          // arguments
// 1,     // prefetch count
// 0,     // prefetch size
// false, // global

package rabbitmq

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thalesfsp/queue/internal/shared"
	"github.com/thalesfsp/queue/queue"
)

func TestNew(t *testing.T) {
	if !shared.IsEnvironment(shared.Integration) {
		t.Skip("Skipping test. Not in e2e " + shared.Integration + "environment.")
	}

	t.Setenv("HTTPCLIENT_METRICS_PREFIX", "queue_"+Name+"_test")

	host := os.Getenv("RABBITMQ_HOST")

	if host == "" {
		t.Fatal("RABBITMQ_HOST is not set")
	}

	queueName := "test_task_queue"

	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		args    args
		want    any
		wantErr bool
	}{
		{
			name: "Shoud work - E2E",
			args: args{
				ctx: context.Background(),
			},
			want:    nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//////
			// Tear up.
			//////

			ctx, cancel := context.WithTimeout(tt.args.ctx, shared.DefaultTimeout)
			defer cancel()

			q, err := New(ctx, host, queueName, nil)
			assert.NoError(t, err)

			//////
			// Should be able to subscribe.
			//////

			// Buffer of 3 to avoid blocking the publisher.
			messageReceived := make(chan struct{}, 3)

			assert.NoError(t, q.Subscribe(ctx, queueName, func(_ context.Context, msg *queue.Message) error {
				assert.NotNil(t, msg)

				// This also works because the content of msg.Body is a JSON as
				// a string.
				assert.Contains(t, string(msg.Body), "test")

				// Converts back to struct, to ensure, marshalling and
				// unmarshalling is working properly.
				var data shared.TestDataS
				assert.NoError(t, shared.Unmarshal(msg.Body, &data))

				assert.Contains(t, data.Name, "test")

				// Signal message was received
				messageReceived <- struct{}{}

				return nil
			}, NewSubscribeParams()))

			// Give enough time for the subscriber to be ready.
			time.Sleep(1 * time.Second)

			//////
			// Should be able to publish.
			//////

			assert.NoError(t, err)

			assert.NoError(t, q.Publish(ctx, queueName, queue.NewMustMessageFromStruct(&shared.TestDataS{
				Name: "test1",
			}), NewPublishParams()))

			assert.NoError(t, q.Publish(ctx, queueName, queue.NewMustMessageFromStruct(&shared.TestDataS{
				Name: "test2",
			}), NewPublishParams()))

			assert.NoError(t, q.Publish(ctx, queueName, queue.NewMustMessageFromStruct(&shared.TestDataS{
				Name: "test3",
			}), NewPublishParams()))

			//////
			// Ensures enough time for the message to be processed.
			//////

			// Wait for all 3 messages to be processed
			for i := 0; i < 3; i++ {
				select {
				case <-messageReceived:
					// Message was processed successfully
				case <-time.After(5 * time.Second):
					t.Fatalf("Timeout waiting for message %d to be processed", i+1)
				}
			}

			//////
			// Metrics should be correct.
			//////

			// q.GetCounterPublished().Value()
			assert.Equal(t, int64(3), q.GetCounterPublished().Value())
			assert.Equal(t, int64(1), q.GetCounterSubscribed().Value())
			assert.Equal(t, int64(3), q.GetCounterReceived().Value())
		})
	}
}
