package queue

import (
	"context"
	"testing"
)

func publishMock(name string) *Mock {
	m := new(Mock)
	m.MockPublish = func(context.Context, string, *Message, *PublishParams, ...OptionsFunc) error { return nil }
	m.MockGetName = func() string { return name }
	return m
}

var (
	m1 = publishMock("m1")
	m2 = publishMock("m2")
)

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
			ctx := t.Context()

			m := make(Map)
			m["m1"] = m1
			m["m2"] = m2

			msg := NewMessage([]byte("test"))

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
			ctx := t.Context()

			items := []*Message{
				NewMessage([]byte("content1")),
				NewMessage([]byte("content2")),
			}

			err := PublishMany(ctx, m1, "test-queue", items, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("PublishMany() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
