package queue

import (
	"context"
	"testing"
)

var m1 = &Mock{
	MockPublish: func(ctx context.Context, queueName string, msg *Message, prm *PublishParams, options ...OptionsFunc) error {
		return nil
	},

	MockGetName: func() string {
		return "m1"
	},
}

var m2 = &Mock{
	MockPublish: func(ctx context.Context, queueName string, msg *Message, prm *PublishParams, options ...OptionsFunc) error {
		return nil
	},

	MockGetName: func() string {
		return "m2"
	},
}

func TestPublishToMany(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "Should work",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			m := make(Map)
			m["m1"] = m1
			m["m2"] = m2

			msg := &Message{Body: []byte("test")}

			err := PublishToMany(ctx, m, "test-queue", msg, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("PublishToMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestPublishMany(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "Should work",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()

			items := []*Message{
				{Body: []byte("content1")},
				{Body: []byte("content2")},
			}

			err := PublishMany(ctx, m1, "test-queue", items, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("PublishMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
